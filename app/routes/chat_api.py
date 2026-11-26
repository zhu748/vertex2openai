from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from request_ingress import current_ingress, hold_ingress
from media_tasks import run_media_task
from request_validation import validate_model_response_format, validate_openai_audio, validate_openai_search, validate_openai_logit_bias
from schema_validation import SchemaValidationError, preflight_schema_contracts, validate_openai_tool_strictness
from starlette.background import BackgroundTask, BackgroundTasks
from starlette.requests import ClientDisconnect
from request_connection import ClientConnection
from signature_origin import ChatOriginDelivery, bind_signature_origins
from models import OpenAIRequest
from tool_policy import validate_model_tool_choice, validate_parallel_tool_calls
from auth import get_api_key, get_api_key_flexible

import asyncio
from functools import partial
import hashlib
import json
import math
import time
import traceback

# 引入运行状态管理器与多通道分发策略
from runtime_state import app_state
from credential_manager import credential_manager  # noqa: F401  # 测试 seam：对象属性 patch 的访问面
from retry_policy import advisory_headers, bounded_sse_advisory, response_retry_after
from upstream_errors import error_status, extract_upstream_error, create_openai_error_response
from logger import stats
from streaming import SSE_NO_BUFFER_HEADERS
from admission import (ManagedStreamingResponse, count_slow_setup_failure, image_limiter as image_limiter,
                       limit_generation, run_with_image_limit, SETUP_FAST_WINDOW_SECONDS)
from response_validation import IncompleteResponseError
from request_tracking import configure as track_configure
from json_encoding import iter_json_bytes, json_response_offloop

# 第二轮：弱网可靠传输 —— 服务端响应缓冲 + 序列号事件 + 断线重放 / 分片轮询。
# 上游读取由后台泵驱动，与前端连接解绑；已收到的事件保存在有界缓冲。
# 前端凭生成 ID / Last-Event-ID 续传，或轮询 /v1/resumable/{id} 拉分片。
# SSE 空闲时注释保活；上游中断或超出期限时发送明确错误。
from resumable import (
    resumable_store,
    is_enabled as resumable_enabled,
    image_bypass_enabled,
    DetachedRequest,
    pump_stream,
    ReplayStreamResponse,
    resolve_buffer,
    resume_cursor,
    resume_intent,
    within_request_deadline,
    buffered_setup_result,
    fast_setup_completed,
    FreshReplayCapacityError,
    ResumeRequestError,
)

router = APIRouter()



# ============================================================
# 跨通道调度引擎本体已下沉至 channel_dispatch（2026-11 P2-7）。
# 此处 re-export 维持三类历史契约：
#   1. 路由端点对 _call_upstream/_dispatch_upstream 的模块属性 patch
#      （tests 中 26 处 / 5 处）——本模块命名空间内的调用点仍解析到
#      这里的绑定；
#   2. 测试对 chat_api._sse_frame_kind 等引擎函数的直接调用；
#   3. express_upstream / cookie_upstream 的对象属性 patch
#      （patch 的是实例方法，跨模块共享同一实例）。
# ============================================================
from channel_dispatch import (  # noqa: F401
    cookie_upstream,
    express_upstream,
    _apply_json_fallback,
    _apply_stream_fallback,
    _channel_label,
    _dispatch_upstream,
    _express_manager,
    _fallback_eligible_code,
    _invoke_channel,
    _other_channel,
    _sse_frame_kind,
    _stream_fallback_candidate,
    _stream_with_fallback_frames,
    _structured_locks_express,
    _try_channel_fallback,
)


@limit_generation
async def _call_upstream(request_obj: OpenAIRequest, fastapi_request: Request):
    """选择上游；与 Gemini 原生接口共用生图内存并发闸。

    留在路由层（2026-11 P2-7）：内部经本模块命名空间解析 _dispatch_upstream，
    使 mock.patch("routes.chat_api._dispatch_upstream") 的隔离语义保持不变。
    claude_api 经公共别名 call_upstream 调用，消除路由间私有 import。
    """
    return await run_with_image_limit(
        request_obj.model, lambda: _dispatch_upstream(request_obj, fastapi_request)
    )


# 公共门面：claude_api 等兄弟路由的正式入口（不再 import 下划线私有名）。
call_upstream = _call_upstream




# 取真实已接收字节数而不是客户端 Content-Length；长文本和内嵌图片的完整摘要
# 在有界准备池中计算，断连时仍为在飞 worker 保留入口预算。
_FP_INLINE_BYTES = 64 * 1024


def _request_fingerprint(request_obj: OpenAIRequest, *, small=False) -> str:
    """Canonical JSON digest; avoid full-body serialization for large requests."""
    dumped = request_obj.model_dump(mode="json")
    if small:
        # Same canonical bytes, but the C encoder avoids per-field Python overhead.
        canonical = json.dumps(dumped, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    digest = hashlib.sha256()
    for chunk in iter_json_bytes(dumped, compact=True, sort_keys=True):
        digest.update(chunk)
    return digest.hexdigest()


async def _request_fingerprint_offloop(request_obj: OpenAIRequest) -> str:
    ingress = current_ingress.get()
    if ingress is not None and ingress.done and ingress.held[1] <= _FP_INLINE_BYTES:
        return _request_fingerprint(request_obj, small=True)
    return await run_media_task(_request_fingerprint, request_obj, heavy=False)


def _resume_headers(stream_id: str, replay: bool = False) -> dict:
    headers = {"X-Resume-Id": stream_id, **SSE_NO_BUFFER_HEADERS}
    if replay:
        headers["X-Resumable-Replay"] = "1"
    return headers


# Error terminals are guaranteed even under buffer pressure; bounded input must
# not turn this exception to the ordinary frame budgets into unbounded storage.
_SETUP_ERROR_LIMIT = 8192


def _openai_setup_payload(response, code):
    raw = getattr(response, "body", b"")
    if isinstance(raw, bytes) and len(raw) <= _SETUP_ERROR_LIMIT:
        try:
            payload = json.loads(raw)
            if isinstance(payload["error"]["message"], str) and len(JSONResponse(payload).body) <= _SETUP_ERROR_LIMIT:
                return payload
        except (KeyError, ValueError, TypeError, RecursionError):
            pass
    return create_openai_error_response(code, f"上游未返回流式响应（HTTP {response.status_code}）。", "upstream_error")


def _setup_receipt_headers(buf, *, replay):
    headers = _resume_headers(buf.id, replay=replay)
    if buf.json_retry_after is not None:
        headers["Retry-After"] = buf.json_retry_after
    return headers


def _setup_http_receipt(request, buf, *, replay, original=None):
    # First HTTP or non-cursor replay. Only a successful, connected send can
    # consume the receipt. Preserve a provider response's ASGI cleanup, if any.
    tasks = BackgroundTasks()
    tasks.add_task(buf.acknowledge_http_send, ClientConnection.for_request(request))
    if after := getattr(original, "background", None):
        tasks.tasks.append(after)
    return JSONResponse(buf.payload, status_code=buf.status_code,
                        headers=_setup_receipt_headers(buf, replay=replay), background=tasks)


def _begin_chat_origin_delivery(request):
    if request._signature_output_prefix:
        proof = request._chat_origin_delivery = ChatOriginDelivery()
        request._stage_signature_origin = proof.stage
        return proof
    return None


async def _resumable_stream(fastapi_request: Request, request_obj: OpenAIRequest, api_key: str):
    """流式：缓冲命中则重放/续传；否则建缓冲、后台泵抽帧，当前连接只做缓冲的读者。

    建缓冲走原子占位：并发的同一请求只有第一个去打上游，
    其余直接跟读同一份缓冲（不重复烧配额，也不会把先建的缓冲/泵任务覆盖掉）。
    """
    try:
        buf, is_new, start, id_src = await resolve_buffer(
            resumable_store, fastapi_request, api_key or "", await _request_fingerprint_offloop(request_obj),
            model=request_obj.model, kind="stream")
    except ResumeRequestError as exc:
        return JSONResponse(status_code=exc.code, content=create_openai_error_response(
            exc.code, str(exc), "invalid_resume_request"))
    stream_id = buf.id
    if not is_new:
        # 命中在用的缓冲：重放已缓冲的部分并继续跟随上游直到本轮结束，不再请求上游。
        age = round(time.monotonic() - buf.started_monotonic, 1)
        print(f"↩️ [续传命中] {stream_id}（{id_src}）：直接重放/续传，不再请求上游。"
              f"状态={buf.status}，已运行 {age} 秒，已缓冲 {buf.next_seq} 个事件。"
              "取消前端不会停止后台生成；需要重新生成可为新请求更换 Idempotency-Key，"
              "或使用 X-Resumable: false 关闭本次续传。")
        if buf.openai_setup_failed and not resume_cursor(fastapi_request)[2]:
            return _setup_http_receipt(fastapi_request, buf, replay=True)
        return ReplayStreamResponse(fastapi_request, buf, start, headers=_setup_receipt_headers(buf, replay=True))

    # 连准备阶段也由后台任务持有：前端在鉴权/预处理/首次连接时断开，
    # 不会取消上游或迫使重连者重复生成；淘汰/停机仍可经 store 取消它。
    detached = DetachedRequest(fastapi_request)
    proof = _begin_chat_origin_delivery(request_obj)

    async def start_stream():
        try:
            resp = await _call_upstream(request_obj, detached)
        except asyncio.CancelledError:
            if proof is not None:
                proof.discard()
            raise
        except Exception as exc:
            code, message = extract_upstream_error(exc)
            resp = JSONResponse(create_openai_error_response(code, message[:1024], "upstream_error"),
                                status_code=code, headers=advisory_headers(exc))
        if not isinstance(resp, StreamingResponse):
            if proof is not None:
                proof.discard()
            status = resp.status_code if 400 <= resp.status_code <= 599 else 502
            hint = response_retry_after(resp)
            await buf.fail_openai_setup(_openai_setup_payload(resp, status), status,
                                        hint if hint is not None and len(hint) <= 128 else None)
            return resp
        if buf.done:
            if proof is not None:
                proof.discard()
            close = getattr(resp, "aclose", None) or getattr(resp.body_iterator, "aclose", None)
            if close:
                await close()
            return await json_response_offloop(status_code=buf.status_code, content=buf.payload or
                                                create_openai_error_response(503, "响应缓冲已释放。", "buffer_evicted"))
        candidate = proof if 200 <= resp.status_code < 300 else None
        if candidate is None and proof is not None:
            proof.discard()
        pump_task = hold_ingress(asyncio.create_task(within_request_deadline(
            buf, lambda: pump_stream(buf, resp.body_iterator, chat_proof=candidate))))
        # 泵结束的收尾必须走 resp.aclose（若可用）而不是只关 body_iterator：
        # resp 是 ManagedStreamingResponse，只有 aclose 才会执行 cleanup 链
        # （case B 下会取消 setup 任务并关闭内层上游流；只关 body_iterator 会让
        # 慢 prepare 后被取消的泵泄漏孤儿 setup 任务与在飞上游连接，2026-10 修复）。
        # 正常耗尽路径下 aclose 幂等无害。
        resumable_store.register_pump(stream_id, pump_task, buffer=buf,
                                      close=getattr(resp, "aclose", None) or getattr(resp.body_iterator, "aclose", None))
        print(f"📦 [续传缓冲] {stream_id}（{id_src}）：后台泵已启动，上游读取与前端连接解绑。")
        return resp

    setup_task = hold_ingress(asyncio.create_task(within_request_deadline(buf, start_stream)))
    resumable_store.register_pump(stream_id, setup_task, buffer=buf)
    # Fast setup errors keep their HTTP status. Slow preparation gets an SSE
    # reader immediately, so image downloads/proxy setup cannot leave it idle.
    if await fast_setup_completed(buf, setup_task, SETUP_FAST_WINDOW_SECONDS):
        resp = buffered_setup_result(buf, setup_task)
        if not isinstance(resp, StreamingResponse):
            if buf.done and buf.payload is not None:
                # A concurrent deadline/retirement may have won. Deliver and
                # acknowledge the immutable accepted terminal, never a late one.
                return _setup_http_receipt(fastapi_request, buf, replay=False, original=resp)
            return resp
    else:
        # 慢路径：HTTP 200 + SSE 已经发给本次客户端，后台 setup 稍后失败时
        # 异常/错误响应只会进缓冲（错误帧），middleware 永远看不到——大盘
        # 上既非 success 也非 error（total ≠ success + error 永久漂移）。
        # 快路径（窗口内完成）由 result() re-raise / 真实状态码走 middleware
        # 计数；慢路径补记（2026-10 修复，单一实现见 admission）。
        setup_task.add_done_callback(partial(count_slow_setup_failure, on_error=stats.add_error))

    return ReplayStreamResponse(fastapi_request, buf, 0, headers=_resume_headers(stream_id))


def _delivery_ack(buf, connection, *, full_chat_reply=False):
    return BackgroundTask(buf.acknowledge_http_send, connection, full_chat_reply=full_chat_reply)


async def _read_while_connected(connection, operation, headers):
    """Cancel only this reader's wait/render; leave the detached generation alone."""
    if connection is None:
        return await operation()
    try:
        return await connection.while_connected(operation)
    except ClientDisconnect:
        return JSONResponse(status_code=499, headers=headers, content=create_openai_error_response(
            499, "客户端已断开连接，可重发请求续接。", "client_closed_request"))


async def _json_from_buffer(request, buf, *, replay: bool):
    connection = ClientConnection.for_request(request)
    headers = _resume_headers(buf.id, replay=replay)

    async def read_response():
        if buf.payload is None:
            await buf.wait_for(2 ** 62, max(0.001, buf.deadline_monotonic - time.monotonic()))
            if not buf.done:
                # This reader's absolute wait can fire in the same loop turn as
                # the worker alarm. Publish the same immutable receipt in either order.
                await buf.fail_deadline()
        if buf.payload is None:
            return JSONResponse(status_code=504, headers=headers, content=create_openai_error_response(
                504, "上游响应超时，缓冲未完成。", "timeout"))
        # Read the advisory after waiting for the terminal, not before the pump ran.
        result_headers = ({**headers, "Retry-After": buf.json_retry_after}
                          if buf.json_retry_after is not None else headers)
        return await json_response_offloop(content=buf.payload, status_code=buf.status_code,
                                           headers=result_headers,
                                           background=_delivery_ack(buf, connection, full_chat_reply=True))

    return await _read_while_connected(connection, read_response, headers)


async def _resumable_json(fastapi_request: Request, request_obj: OpenAIRequest, api_key: str):
    """非流式：完整回复落缓冲；原样重发命中未交付缓冲则直接重放。

    同样走原子占位：并发的同一请求只打一次上游，
    后来的请求等同一个后台泵的结果。
    """
    buf, is_new, _start, id_src = await resolve_buffer(
        resumable_store, fastapi_request, api_key or "", await _request_fingerprint_offloop(request_obj),
        model=request_obj.model, kind="json")
    stream_id = buf.id
    if not is_new:
        # 命中在用的缓冲：已出结果的直接重放；上游还在算的，等同一个后台泵的结果。
        print(f"↩️ [重放命中] {stream_id}（{id_src}）：直接返回已缓冲的完整回复，不再请求上游。")
        return await _json_from_buffer(fastapi_request, buf, replay=True)

    detached = DetachedRequest(fastapi_request)
    proof = _begin_chat_origin_delivery(request_obj)

    async def _produce_json():
        # 后台跑上游调用：这条前端连接断了，Google 的回复也要收完进缓冲。
        try:
            resp = await _call_upstream(request_obj, detached)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            code, msg = extract_upstream_error(e)
            print(f"❌ [响应缓冲] {stream_id} 上游调用失败：{msg[:200]}")
            await buf.finish("error", error=msg,
                             payload=create_openai_error_response(code, msg[:1024], "upstream_error"),
                             status_code=code,
                             retry_after=advisory_headers(e).get("retry-after") if error_status(e) is not None else None)
            return
        body = getattr(resp, "body", b"") or b""
        status = getattr(resp, "status_code", 200)
        # 空 body 的 2xx 不再包装成成功空对象（旧实现 payload={} 直接落缓冲，
        # 客户端拿到 200 空 JSON）；上游正常回复必有 JSON 体，空体即不完整。
        if not body:
            raise IncompleteResponseError("上游返回空响应体。") from None
        try:
            payload = json.loads(body.decode("utf-8"))
        except (ValueError, UnicodeError):
            raise IncompleteResponseError("上游返回无效或不完整的 JSON 响应。") from None
        if not isinstance(payload, dict):
            raise IncompleteResponseError("上游返回的 JSON 不是响应对象。")
        origins = ()
        if 200 <= status < 300 and not payload.get("error") and proof is not None and proof.matches_json(resp):
            origins = proof.take()
        await buf.finish("done" if 200 <= status < 300 else "error", payload=payload, status_code=status,
                         retry_after=response_retry_after(resp) if 400 <= status <= 599 else None,
                         chat_origins=origins)
        print(f"📦 [响应缓冲] {stream_id}（{id_src}）：非流式完整回复已落缓冲（HTTP {buf.status_code}）。")

    async def _pump_json():
        try:
            await _produce_json()
        finally:
            if proof is not None:
                proof.discard()

    pump_task = hold_ingress(asyncio.create_task(within_request_deadline(buf, _pump_json)))
    resumable_store.register_pump(stream_id, pump_task, buffer=buf)

    return await _json_from_buffer(fastapi_request, buf, replay=False)


@router.post("/v1/chat/completions")
async def chat_completions(fastapi_request: Request, request: OpenAIRequest, api_key: str = Depends(get_api_key)):
    """
    /v1/chat/completions 动态分流路由器
    按仅 Key、仅 Cookie 或混合模式选择上游；混合时两类启用账号共同参与调度。

    弱网可靠传输（resumable_enabled 开启时，默认开）：
    响应先落服务端缓冲（带序列号），上游读取与前端连接解绑；
    前端断线可凭 Last-Event-ID / ?resume_from= 续传，或轮询 /v1/resumable/{id} 拉分片。

    统一异常兜底：把上游抛出的 404/403/400 等如实转成 OpenAI 错误格式，
    避免笼统的 500 Internal Server Error（非流式路径此前直接 raise）。
    统计由 main.py 的中间件按响应状态码统一计入，这里不重复计数。
    """
    try:
        validate_model_response_format(request)
        validate_openai_audio(request)
        validate_openai_search(request)
        validate_openai_logit_bias(request)
        validate_model_tool_choice(request.model, request.tool_choice)
        validate_parallel_tool_calls(request)
        validate_openai_tool_strictness(request.tools)
        preflight_schema_contracts(request)
    except SchemaValidationError as exc:
        return JSONResponse(status_code=400, content=create_openai_error_response(
            400, str(exc), "invalid_request_error"))
    try:
        bind_signature_origins(request, api_key)
        track_configure(model=request.model, protocol="openai", stream=bool(request.stream),
                        channel="mixed" if app_state.get_channel_mode() == "mixed" else
                        "cookie" if app_state.is_web_proxy_enabled() else "express")
        # 普通聊天客户端通常把“取消后重发”理解为重新生成；允许单次请求
        # 直通，避免相同请求体反复接回一个仍在等待的后台任务。
        requested, explicit = resume_intent(fastapi_request, default_on=True)
        unbuffered = False
        if requested and resumable_enabled():
            if not image_bypass_enabled(request.model):
                try:
                    if request.stream:
                        return await _resumable_stream(fastapi_request, request, api_key)
                    return await _resumable_json(fastapi_request, request, api_key)
                except FreshReplayCapacityError:
                    # No receipt or producer started. Only an implicit new
                    # call can lose replay rather than fail: do not bypass a
                    # caller's explicit opt-in, cursor, or idempotency contract.
                    if explicit or any(name in fastapi_request.headers for name in (
                            "x-resumable", "idempotency-key", "x-idempotency-key")):
                        raise
                    unbuffered = True
            elif not explicit:
                print(f"🖼️ [生图直通] {request.model}：绕过续传缓冲（resumable_image_streams=false），降低内存驻留。")
        if explicit:
            raise ResumeRequestError(409, "当前请求不支持续传；如需重新生成，请移除续传 ID 与游标。")
        # Direct public send, including the implicit capacity fallback.
        proof = _begin_chat_origin_delivery(request)
        response = await _call_upstream(request, fastapi_request)
        if unbuffered:
            response.headers["X-Resumable"] = "false"
        connection = ClientConnection.for_request(fastapi_request)
        if isinstance(response, ManagedStreamingResponse):
            # Bind only the final direct sender. Detached replay setup/pumps use
            # the same dispatcher, but never enter this branch.
            response.client_connection = connection
        if proof is not None:
            streaming = isinstance(response, StreamingResponse)
            if (connection is not None and 200 <= response.status_code < 300
                    and bool(request.stream) == streaming and (streaming or proof.matches_json(response))):
                tasks = BackgroundTasks()
                tasks.add_task(proof.publish, connection, stream=streaming)
                if (after := response.background) is not None:
                    tasks.tasks.append(after)
                response.background = tasks
            else:
                proof.discard()
        return response
    except ResumeRequestError as exc:
        # 续传参数错误是客户端 400，不是上游错误：单独分支保持
        # invalid_resume_request 分类（与 _resumable_stream 同口径），
        # 避免被兜底 except 重写成误导性的 upstream_error。
        print(f"⚠️ [路由兜底] 模型 {request.model} 续传参数无效 | HTTP {exc.code} | {str(exc)[:200]}")
        return JSONResponse(status_code=exc.code, content=create_openai_error_response(
            exc.code, str(exc), "invalid_resume_request"))
    except Exception as e:
        # 兜底 except 会把编程错误（AttributeError/TypeError 等）也归类成
        # 上游错误；留下 traceback 才能定位真正的缺陷。
        code, msg = extract_upstream_error(e)
        print(f"❌ [路由兜底] 模型 {request.model} 调用失败 | HTTP {code} | {msg[:200]}")
        traceback.print_exc()
        return JSONResponse(status_code=code, content=create_openai_error_response(code, msg, "upstream_error"),
                            headers=advisory_headers(e))


@router.get("/v1/resumable/{stream_id}")
async def resumable_poll(stream_id: str, fastapi_request: Request,
                         api_key: str = Depends(get_api_key_flexible),
                         from_: str = Query("0", alias="from"),
                         limit: str = Query("200"),
                         wait: str = Query("0")):
    """
    分片轮询拉取：GET /v1/resumable/{id}?from=<seq>&limit=200&wait=10

    短连接天生抗抖动；返回带序号的事件分片、next 游标与 done 标志，
    客户端按 seq 去重、按 done 判定本轮完整结束。wait>0 时为长轮询（最多等 25 秒）。
    响应头 X-Resume-Id 即为这里的 {id}。

    鉴权与原生端点同口径（Bearer / x-goog-api-key / ?key=），并校验缓冲
    归属：rid 只能被创建它的那把 Key 轮询（rid 本身混入 Key+指纹，跨 Key
    读取意味着拿到了他人的生成 ID，2026-10 收紧）。
    """
    # REQ-P3-5：参数一律 str 接收 + 手工解析，类型错误统一走本端点的
    # OpenAI 错误形状（400 invalid_resume_cursor），不再漏成 FastAPI
    # 422 形状（与续传主路径 resume_cursor 的口径一致）。
    try:
        start_from = int(from_)
    except ValueError:
        return JSONResponse(status_code=400, content=create_openai_error_response(
            400, "轮询游标 from 必须是整数。", "invalid_resume_cursor"))
    try:
        poll_limit = int(limit)
    except ValueError:
        return JSONResponse(status_code=400, content=create_openai_error_response(
            400, "轮询参数 limit 必须是整数。", "invalid_resume_cursor"))
    try:
        poll_wait = float(wait)
    except ValueError:
        return JSONResponse(status_code=400, content=create_openai_error_response(
            400, "轮询参数 wait 必须是数字。", "invalid_resume_cursor"))
    if not math.isfinite(poll_wait):
        # inf/nan 会毒化 note_client_activity 的窗口计算（旧 float 解析
        # 默默收下 nan，min(nan, 25) = nan 直接写进断连计时器）。
        return JSONResponse(status_code=400, content=create_openai_error_response(
            400, "轮询参数 wait 必须是有限数字。", "invalid_resume_cursor"))
    if start_from < 0:
        # 与续传主路径（resume_cursor 对负数返回 400）保持同一口径：
        # 静默锐 0 会把客户端的游标 bug 掩盖成「从头全量重放」。
        return JSONResponse(status_code=400, content=create_openai_error_response(
            400, "轮询游标 from 必须是非负整数。", "invalid_resume_cursor"))
    buf = await resumable_store.get_exact(stream_id)
    if buf is None:
        return JSONResponse(
            status_code=404,
            content=create_openai_error_response(
                404, f"未找到响应缓冲 {stream_id}（可能已过期被清理）。", "not_found"))
    owner_digest = hashlib.sha256((api_key or "").encode("utf-8")).hexdigest() if api_key else ""
    if getattr(buf, "owner_key_digest", "") and owner_digest and buf.owner_key_digest != owner_digest:
        # 404 而非 403：不向持有他人 rid 的调用者确认该缓冲存在。
        return JSONResponse(
            status_code=404,
            content=create_openai_error_response(
                404, f"未找到响应缓冲 {stream_id}（可能已过期被清理）。", "not_found"))
    try:
        await resumable_store.require_resume_window(buf)
    except ResumeRequestError as exc:
        return JSONResponse(status_code=exc.code, content=create_openai_error_response(
            exc.code, str(exc), "invalid_resume_request"))
    # Only a cursor at/before the current tail is a reconnect. Reject a future
    # cursor before registering any activity; waiting could even make it valid.
    # No await between this check and attach_poll, and next_seq only grows.
    if start_from > buf.next_seq:
        return JSONResponse(status_code=409, content=create_openai_error_response(
            409, "轮询游标超过缓冲末尾。", "invalid_resume_cursor"))
    connection = ClientConnection.for_request(fastapi_request)
    if watch := getattr(fastapi_request, "scope", {}).get("state", {}).get("watch_resumable_poll"):
        watch()
    poll_wait = max(0.0, min(poll_wait, 25.0))
    buf.attach_poll(connection, poll_wait)
    headers = _resume_headers(buf.id)
    return await _read_while_connected(connection, lambda: _poll_result(
        buf, start_from, max(1, min(poll_limit, 500)), poll_wait, connection, headers), headers)


async def _poll_result(buf, start, poll_limit, poll_wait, connection, headers):
    if poll_wait > 0 and not buf.done:
        await buf.wait_for(start, poll_wait)
    events = buf.snapshot(start, poll_limit)
    end = start + len(events)
    done = buf.done and end >= buf.next_seq
    content = {
        "id": buf.id,
        "model": buf.model,
        "kind": buf.kind,
        "status": buf.status,
        "error": buf.error,
        "events": [{"seq": start + i, "data": frame} for i, frame in enumerate(events)],
        "next": end,
        "done": done,
    }
    if buf.kind == "json" and buf.done:
        # JSON 没有事件分片；轮询必须实际交付正文，不能只报告 done。
        content.update(payload=buf.payload, status_code=buf.status_code)
        # The GET itself succeeded; the advisory belongs to its retained error.
        if buf.status == "error" and (hint := bounded_sse_advisory(buf.json_retry_after)) is not None:
            content["retry_after"] = hint
    # REQ-P2-2：轮询一次可拉 500 帧（巨帧可达数 MB），大响应离环序列化。
    return await json_response_offloop(
        content=content,
        headers=headers,
        background=_delivery_ack(buf, connection, full_chat_reply=start == 0)
        if buf.includes_delivery_terminal(start, end) else None)

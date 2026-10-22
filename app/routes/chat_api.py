from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.background import BackgroundTask
from models import OpenAIRequest
from auth import get_api_key, get_api_key_flexible

import asyncio
import hashlib
import json
import time
import traceback

# 引入运行状态管理器与多通道分发策略
from runtime_state import app_state
from credential_manager import credential_manager
from upstreams.express_sdk import ExpressSDKUpstream
from upstreams.cookie_proxy import CookieProxyUpstream
from upstream_errors import extract_upstream_error, create_openai_error_response
from logger import stats
from streaming import keepalive_sse, close_setup_task
from admission import limit_generation, ManagedStreamingResponse
from response_validation import IncompleteResponseError
from request_tracking import configure as track_configure

# 第二轮：弱网可靠传输 —— 服务端响应缓冲 + 序列号事件 + 断线重放 / 分片轮询。
# 上游读取由后台泵驱动，与前端连接解绑；已收到的事件保存在有界缓冲。
# 前端凭生成 ID / Last-Event-ID 续传，或轮询 /v1/resumable/{id} 拉分片。
# SSE 空闲时注释保活；上游中断或超出期限时发送明确错误。
from resumable import (
    resumable_store,
    is_enabled as resumable_enabled,
    DetachedRequest,
    pump_stream,
    sse_from_buffer,
    resolve_buffer,
    resume_cursor,
    within_request_deadline,
    request_timeout_seconds,
    ResumeRequestError,
    DONE_FRAME,
)

router = APIRouter()

# 实例化多通道策略
express_upstream = ExpressSDKUpstream()
cookie_upstream = CookieProxyUpstream()


@limit_generation
async def _call_upstream(request_obj: OpenAIRequest, fastapi_request: Request):
    """Select an enabled upstream credential using the saved channel mode."""
    deadline = time.monotonic() + request_timeout_seconds()
    async def prepare():
        async with asyncio.timeout(max(0.001, deadline - time.monotonic())):
            if app_state.get_channel_mode() == "mixed":
                structured = bool(request_obj.response_format and request_obj.response_format.get("type") not in (None, "text"))
                record = credential_manager.get_mixed_record(
                    fastapi_request.app.state.express_key_manager, express_only=structured)
                if record is None:
                    return JSONResponse(status_code=401, content=create_openai_error_response(
                        401, "没有可用的上游账号。请启用 Express Key 或填写完整的 Cookie 与 Project ID；结构化输出需要 Express Key。",
                        "authentication_error"))
                upstream = cookie_upstream if record["channel"] == "cookie" else express_upstream
                return await upstream.chat_completions(request_obj, fastapi_request, credential_record=record)
            if app_state.is_web_proxy_enabled():
                return await cookie_upstream.chat_completions(request_obj, fastapi_request)
            return await express_upstream.chat_completions(request_obj, fastapi_request)

    if request_obj.stream:
        # Start SSE promptly even if proxy startup or image preprocessing is
        # slow. Fast validation errors can still keep their HTTP status.
        setup = asyncio.create_task(prepare())

        async def close_setup():
            await close_setup_task(setup)

        try:
            done, _ = await asyncio.wait({setup}, timeout=0.05)
        except BaseException:
            await close_setup()
            raise
        if done:
            response = setup.result()
        else:
            async def prepared_frames():
                result = await setup
                if isinstance(result, StreamingResponse):
                    async for frame in result.body_iterator:
                        yield frame
                else:
                    stats.add_error()
                    try:
                        payload = json.loads(result.body)
                    except (ValueError, TypeError, AttributeError):
                        payload = None
                    if not isinstance(payload, dict) or not payload.get("error"):
                        payload = create_openai_error_response(
                            result.status_code if result.status_code >= 400 else 502,
                            "上游未返回流式响应。", "upstream_error")
                    yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
                    yield DONE_FRAME
            response = ManagedStreamingResponse(prepared_frames(), media_type="text/event-stream", cleanup=close_setup)
    else:
        response = await prepare()
    if isinstance(response, StreamingResponse):
        original = response.body_iterator

        async def relay():
            stream = keepalive_sse(original, fastapi_request)
            try:
                async with asyncio.timeout(max(0.001, deadline - time.monotonic())):
                    async for frame in stream:
                        yield frame
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                stats.add_error()
                code, message = extract_upstream_error(exc)
                # 裸 TimeoutError 的 str() 是空串，客户端会收到不可排障的空
                # message 错误帧；对齐 resumable 路径的固定文案。
                if not message and isinstance(exc, TimeoutError):
                    message = "本轮请求超过总执行期限。"
                print(f"❌ [流式转发] {request_obj.model} | HTTP {code} | {message[:200]}")
                error = create_openai_error_response(code, message[:1024], "upstream_error")
                yield f"data: {json.dumps(error, ensure_ascii=False)}\n\n"
                yield DONE_FRAME
            finally:
                await stream.aclose()
        headers = dict(response.headers)
        headers.pop("content-length", None)
        headers.update({"Cache-Control": "no-store, no-transform", "X-Accel-Buffering": "no"})
        return ManagedStreamingResponse(
            relay(), status_code=response.status_code, headers=headers, background=response.background,
            cleanup=getattr(response, "aclose", original.aclose))
    return response


def _request_fingerprint(request_obj: OpenAIRequest) -> str:
    """稳定请求指纹：同一请求原样重发即命中同一缓冲区（不再烧上游配额）。"""
    try:
        return json.dumps(request_obj.model_dump(mode="json"), sort_keys=True,
                          ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return json.dumps(request_obj.model_dump(), sort_keys=True, default=str)


def _resume_headers(stream_id: str, replay: bool = False) -> dict:
    headers = {"X-Resume-Id": stream_id, "Cache-Control": "no-store, no-transform", "X-Accel-Buffering": "no"}
    if replay:
        headers["X-Resumable-Replay"] = "1"
    return headers


async def _abort_placeholder(buf, stream_id: str, message: str, code: int = 500) -> None:
    """占位缓冲没能接上上游：给已经在跟读它的并发请求补一个错误帧 + 终止帧，再撤掉占位。

    撤掉占位是为了不让一个「空壳」缓冲挡住后续重试（跟读者手里还拿着缓冲对象，读得完）。
    """
    await buf.fail(message, code)
    await resumable_store.drop(stream_id, expected=buf)


async def _resumable_stream(fastapi_request: Request, request_obj: OpenAIRequest, api_key: str):
    """流式：缓冲命中则重放/续传；否则建缓冲、后台泵抽帧，当前连接只做缓冲的读者。

    建缓冲走原子占位：并发的同一请求只有第一个去打上游，
    其余直接跟读同一份缓冲（不重复烧配额，也不会把先建的缓冲/泵任务覆盖掉）。
    """
    try:
        buf, is_new, start, id_src = await resolve_buffer(
            resumable_store, fastapi_request, api_key or "", _request_fingerprint(request_obj),
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
        return StreamingResponse(
            sse_from_buffer(buf, start),
            media_type="text/event-stream",
            headers=_resume_headers(stream_id, replay=True),
        )

    # 连准备阶段也由后台任务持有：前端在鉴权/预处理/首次连接时断开，
    # 不会取消上游或迫使重连者重复生成；淘汰/停机仍可经 store 取消它。
    detached = DetachedRequest(fastapi_request)

    async def start_stream():
        try:
            resp = await _call_upstream(request_obj, detached)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            code, message = extract_upstream_error(exc)
            await _abort_placeholder(buf, stream_id, message, code)
            raise
        if not isinstance(resp, StreamingResponse):
            # 初始化错误仍向发起者返回原 HTTP 响应，并唤醒已有的跟读者。
            status = getattr(resp, "status_code", 500)
            await _abort_placeholder(buf, stream_id, f"上游返回非流式响应（HTTP {status}）。", status)
            return resp
        if buf.done:
            close = getattr(resp, "aclose", None) or getattr(resp.body_iterator, "aclose", None)
            if close:
                await close()
            return JSONResponse(status_code=buf.status_code, content=buf.payload or
                                create_openai_error_response(503, "响应缓冲已释放。", "buffer_evicted"))
        pump_task = asyncio.create_task(within_request_deadline(buf, lambda: pump_stream(buf, resp.body_iterator)))
        # 泵结束的收尾必须走 resp.aclose（若可用）而不是只关 body_iterator：
        # resp 是 ManagedStreamingResponse，只有 aclose 才会执行 cleanup 链
        # （case B 下会取消 setup 任务并关闭内层上游流；只关 body_iterator 会让
        # 慢 prepare 后被取消的泵泄漏孤儿 setup 任务与在飞上游连接，2026-10 修复）。
        # 正常耗尽路径下 aclose 幂等无害。
        pump_task.add_done_callback(lambda _task: resumable_store.schedule_cleanup(
            getattr(resp, "aclose", resp.body_iterator.aclose)))
        resumable_store.register_pump(stream_id, pump_task, buffer=buf)
        print(f"📦 [续传缓冲] {stream_id}（{id_src}）：后台泵已启动，上游读取与前端连接解绑。")
        return resp

    setup_task = asyncio.create_task(within_request_deadline(buf, start_stream))
    resumable_store.register_pump(stream_id, setup_task, buffer=buf)
    # Fast setup errors keep their HTTP status. Slow preparation gets an SSE
    # reader immediately, so image downloads/proxy setup cannot leave it idle.
    done, _ = await asyncio.wait({setup_task}, timeout=0.05)
    if done:
        resp = setup_task.result()
        if not isinstance(resp, StreamingResponse):
            return resp
    else:
        # 慢路径：HTTP 200 + SSE 已经发给本次客户端，后台 setup 稍后失败时
        # 异常/错误响应只会进缓冲（错误帧），middleware 永远看不到——大盘
        # 上既非 success 也非 error（total ≠ success + error 永久漂移）。
        # 快路径（0.05s 内完成）由 result() re-raise / 真实状态码走 middleware
        # 计数，这里只补慢路径且每 setup_task 一次，无双计（2026-10 修复）。
        slow_path = True

        def _count_slow_failure(task: asyncio.Task) -> None:
            if not slow_path or task.cancelled():
                return  # 取消（淘汰/停机）不算失败
            exc = None
            try:
                exc = task.exception()
            except asyncio.CancelledError:
                return
            if exc is not None or not isinstance(task.result(), StreamingResponse):
                stats.add_error()

        setup_task.add_done_callback(_count_slow_failure)

    return StreamingResponse(
        sse_from_buffer(buf, 0),
        media_type="text/event-stream",
        headers=_resume_headers(stream_id),
    )


async def _resumable_json(fastapi_request: Request, request_obj: OpenAIRequest, api_key: str):
    """非流式：完整回复落缓冲；原样重发命中未交付缓冲则直接重放。

    同样走原子占位：并发的同一请求只打一次上游，
    后来的请求等同一个后台泵的结果。
    """
    buf, is_new, _start, id_src = await resolve_buffer(
        resumable_store, fastapi_request, api_key or "", _request_fingerprint(request_obj),
        model=request_obj.model, kind="json")
    stream_id = buf.id
    if not is_new:
        # 命中在用的缓冲：已出结果的直接重放；上游还在算的，等同一个后台泵的结果。
        print(f"↩️ [重放命中] {stream_id}（{id_src}）：直接返回已缓冲的完整回复，不再请求上游。")
        if buf.payload is None:
            await buf.wait_for(2 ** 62, max(0.001, buf.deadline_monotonic - time.monotonic()))
        if buf.payload is None:
            return JSONResponse(
                status_code=504,
                content=create_openai_error_response(504, "上游响应超时，缓冲未完成。", "timeout"),
                headers=_resume_headers(stream_id, replay=True))
        return JSONResponse(status_code=buf.status_code, content=buf.payload,
                            headers=_resume_headers(stream_id, replay=True),
                            background=BackgroundTask(buf.mark_delivered))

    detached = DetachedRequest(fastapi_request)

    async def _pump_json():
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
                             status_code=code)
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
        await buf.finish("done" if 200 <= status < 300 else "error",
                         payload=payload, status_code=status)

    pump_task = asyncio.create_task(within_request_deadline(buf, _pump_json))
    resumable_store.register_pump(stream_id, pump_task, buffer=buf)

    # 当前连接等待缓冲完成（等同于原来的直通等待；断线不影响后台泵）。
    # wait_for(seq=极大值) 等价于“等到 done 或超时”。
    await buf.wait_for(2 ** 62, max(0.001, buf.deadline_monotonic - time.monotonic()))
    if buf.payload is None:
        return JSONResponse(
            status_code=504,
            content=create_openai_error_response(504, "上游响应超时，缓冲未完成。", "timeout"),
            headers=_resume_headers(stream_id))
    # 响应体发送完成后才标记交付；发送失败时保留同一份回复供重试。
    print(f"📦 [响应缓冲] {stream_id}（{id_src}）：非流式完整回复已落缓冲（HTTP {buf.status_code}）。")
    return JSONResponse(status_code=buf.status_code, content=buf.payload,
                        headers=_resume_headers(stream_id),
                        background=BackgroundTask(buf.mark_delivered))


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
        track_configure(model=request.model, protocol="openai", stream=bool(request.stream),
                        channel="mixed" if app_state.get_channel_mode() == "mixed" else
                        "cookie" if app_state.is_web_proxy_enabled() else "express")
        # 普通聊天客户端通常把“取消后重发”理解为重新生成；允许单次请求
        # 直通，避免相同请求体反复接回一个仍在等待的后台任务。
        resume_disabled = fastapi_request.headers.get("x-resumable", "").strip().lower() in ("0", "false", "no")
        if resume_disabled and resume_cursor(fastapi_request)[2]:
            raise ResumeRequestError(400, "X-Resumable: false 不能与续传 ID 或游标同时使用。")
        if resumable_enabled() and not resume_disabled:
            if request.stream:
                return await _resumable_stream(fastapi_request, request, api_key)
            return await _resumable_json(fastapi_request, request, api_key)
        return await _call_upstream(request, fastapi_request)
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
        return JSONResponse(status_code=code, content=create_openai_error_response(code, msg, "upstream_error"))


@router.get("/v1/resumable/{stream_id}")
async def resumable_poll(stream_id: str, fastapi_request: Request,
                         api_key: str = Depends(get_api_key_flexible),
                         from_: int = Query(0, alias="from"),
                         limit: int = Query(200),
                         wait: float = Query(0.0)):
    """
    分片轮询拉取：GET /v1/resumable/{id}?from=<seq>&limit=200&wait=10

    短连接天生抗抖动；返回带序号的事件分片、next 游标与 done 标志，
    客户端按 seq 去重、按 done 判定本轮完整结束。wait>0 时为长轮询（最多等 25 秒）。
    响应头 X-Resume-Id 即为这里的 {id}。

    鉴权与原生端点同口径（Bearer / x-goog-api-key / ?key=），并校验缓冲
    归属：rid 只能被创建它的那把 Key 轮询（rid 本身混入 Key+指纹，跨 Key
    读取意味着拿到了他人的生成 ID，2026-10 收紧）。
    """
    if from_ < 0:
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
    limit = max(1, min(limit, 500))
    if wait > 0 and not buf.done:
        await buf.wait_for(max(0, from_), min(wait, 25.0))
    start = from_
    if start > buf.next_seq:
        return JSONResponse(status_code=409, content=create_openai_error_response(
            409, "轮询游标超过缓冲末尾。", "invalid_resume_cursor"))
    events = buf.snapshot(start, limit)
    end = start + len(events)
    done = buf.done and end >= buf.next_seq
    delivered = done and start <= buf.next_seq
    content = {
        "id": stream_id,
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
    return JSONResponse(
        content=content,
        headers=_resume_headers(stream_id),
        background=BackgroundTask(buf.mark_delivered) if delivered else None)

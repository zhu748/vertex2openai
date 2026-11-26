"""Anthropic Claude Messages API 兼容路由（/v1/messages 双端点）。

为什么需要它：Claude 生态（Claude Code、Anthropic SDK、Claude Code Router、
Cherry Studio 等说 Claude 协议的客户端）说的是另一套协议：

    POST /v1/messages                （主端点，stream:true 走 SSE）
    POST /v1/messages/count_tokens   （上下文 token 估算）
    GET  /api/hello                  （Claude Code 网关启动探测）

本模块把 Anthropic 请求翻译成内部 OpenAIRequest，**完全复用**
/v1/chat/completions 的上游管线（Express Key / Cookie 双通道调度、重试、
抗截断、预填充兼容、思考档位、安全设置等），再把响应翻译回 Anthropic
格式；协议转换细节见 claude_protocol.py。

实现要点：
- 认证兼容 Anthropic 标准写法 x-api-key，同时接受 Authorization: Bearer、
  x-goog-api-key 与 ?key=（值必须等于本服务 API_KEY，常量时间比较）；
  401/400 等错误一律返回 Anthropic 错误形状 {"type":"error","error":{...}}；
- anthropic-version / anthropic-beta 头不校验（放行即可）；
- 方法约束对齐 vertex-master：/v1/messages 与 count_tokens 仅接受 POST、
  /api/hello 仅接受 GET/HEAD，其余方法一律 405 + Anthropic 错误形状
  （FastAPI 路由层的默认 405 是 {"detail":...} 形状，Claude 客户端无法解析）；
- 流式：进入即发 message_start（客户端先拿到头），上游心跳注释帧转换为
  event: ping，上游错误帧转换为 event: error；另有 10s 空闲保活——长思考
  场景 Gemini 可能 10-30s 不发内容，静默超 10s 自动补 event: ping，防止
  Claude Code SDK 误判超时断连（对齐 vertex-master 的 10s ping ticker）；
  中途错误不改变 HTTP 状态码（SSE 头已提交，对齐 Anthropic 官方流式行为）；
- 已知 Retry-After：快速失败返回合法 HTTP 头；已提交的流在 error 前发限长的只读 SSE 注释，
  不改 Anthropic JSON 结构、不触发自动重连；
- 非流式与流式的快速失败（0.05s 窗口内）：保留真实 HTTP 状态码 + Anthropic
  错误体（invalid_request_error / not_found_error / rate_limit_error 等）；
- 续传缓冲（resumable）为 OpenAI SSE 专属语义，Claude 路径直连上游，
  X-Resumable 头不生效。
"""

import asyncio
import json
import threading
import time
from typing import Optional

from fastapi import APIRouter, Depends, Header, Request
from fastapi.responses import JSONResponse, StreamingResponse
from google.genai import types
from starlette.background import BackgroundTask
from starlette.requests import ClientDisconnect

from admission import GenerationCapacityError, ManagedStreamingResponse
from signature_origin import DeliveredOrigin, bind_signature_origins
from claude_signature_projection import OmittedClaudeProjection, bind_omitted_claude_projection
from auth import constant_time_equal, extract_candidate_key
from claude_protocol import (
    ClaudeRequestError,
    ClaudeStreamState,
    PING_EVENT,
    anthropic_error_type,
    anthropic_models_payload,
    anthropic_omit_thinking,
    anthropic_to_openai_request,
    estimate_request_tokens,
    error_event,
    new_message_id,
    new_request_id,
    openai_completion_to_anthropic,
    resolve_claude_model,
)
from catalog_pagination import CatalogPageError, anthropic_model_page
from config import API_KEY
from express_endpoint import resolve_express_model_path
from generation_config import function_tools, system_instruction
from logger import stats
from json_encoding import LargeJSONResponse
from media_tasks import prompt_needs_media_pool, run_media_task
from message_processing import create_gemini_prompt
from model_loader import get_express_models
from proxy_failover import FailoverSDKClient, ProxyChain, ProxyResolveError
from proxy_resolver import ProxyUsage, current_proxy_usage
from replay_event import ReplayEvent
from client_pool import get_pooled_express_client
from request_tracking import configure as track_configure
from response_validation import IncompleteResponseError
from request_connection import BodyTooLargeError, ClientConnection
from stream_cleanup import await_owned_close
from request_ingress import IngressError
from routes.chat_api import call_upstream
from runtime_state import app_state
from streaming import ByteBoundedQueue, SSE_NO_BUFFER_HEADERS, sse_frames
from tool_policy import validate_parallel_tool_calls
from request_validation import validate_model_response_format
from schema_validation import SchemaValidationError, preflight_schema_contracts
from retry_policy import advisory_headers, bounded_sse_advisory, response_retry_after
from upstream_errors import extract_upstream_error
from upstreams.express_sdk import normalize_model_name

router = APIRouter()

# 空闲 ping 周期（秒）：模块级常量便于单测注入缩短周期。
# Claude Code SDK 要求流式连接静默不超过 ~15s，否则误判超时断连；
# 长思考场景 Gemini 可能 10-30s 不发任何内容，静默超时即补 event: ping
# （对齐 vertex-master 的 10s ping ticker，取「空闲才补」语义）。
IDLE_PING_SECONDS = 10.0


class ClaudeAPIError(Exception):
    """携带 Anthropic 错误 JSONResponse 的异常，由 main.py 注册的全局
    exception_handler 转成响应（依赖函数里不能直接 return Response）。"""

    def __init__(self, response: JSONResponse):
        self.response = response
        super().__init__("ClaudeAPIError")


def claude_error(status: int, err_type: str, message: str) -> JSONResponse:
    """构造 Anthropic 错误体（官方形状 {"type":"error","error":{...},"request_id":...}）。"""
    return JSONResponse(status_code=status, content={
        "type": "error",
        "error": {"type": err_type, "message": message},
        "request_id": new_request_id(),
    })


# ============================================================
# 认证：x-api-key（Anthropic 标准）> Bearer > x-goog-api-key > ?key=
# ============================================================

async def get_claude_auth(
    request: Request,
    authorization: Optional[str] = Header(None, include_in_schema=False),
    x_api_key: Optional[str] = Header(None, alias="x-api-key", include_in_schema=False),
    x_goog_api_key: Optional[str] = Header(None, alias="x-goog-api-key", include_in_schema=False),
) -> str:
    if not API_KEY:
        raise ClaudeAPIError(claude_error(503, "api_error", "服务端未配置 API_KEY，无法鉴权。"))
    candidate = extract_candidate_key(
        request, authorization=authorization or "",
        x_api_key=x_api_key or "", x_goog_api_key=x_goog_api_key or "")
    if not candidate:
        raise ClaudeAPIError(claude_error(
            401, "authentication_error",
            "缺少 API Key：请传入 x-api-key 或 Authorization: Bearer 请求头。"))
    if not constant_time_equal(str(candidate), API_KEY):
        raise ClaudeAPIError(claude_error(401, "authentication_error", "API Key 无效。"))
    return str(candidate)


# ============================================================
# 工具函数
# ============================================================

async def _parse_json_body(fastapi_request: Request) -> dict:
    """手动解析 JSON 体：让 400 错误保持 Anthropic 形状（而非 FastAPI 422）。"""
    try:
        body = await fastapi_request.json()
    except (BodyTooLargeError, IngressError):
        raise
    except Exception:
        raise ClaudeRequestError("invalid JSON request body") from None
    if not isinstance(body, dict):
        raise ClaudeRequestError("request body must be a JSON object")
    return body


def _track_channel() -> str:
    if app_state.get_channel_mode() == "mixed":
        return "mixed"
    return "cookie" if app_state.is_web_proxy_enabled() else "express"


async def _available_models() -> list:
    try:
        return list(await get_express_models() or [])
    except Exception:
        return []


def _openai_error_message(payload: Optional[dict], fallback: str) -> str:
    error = payload.get("error") if isinstance(payload, dict) else None
    if isinstance(error, dict) and isinstance(error.get("message"), str) and error["message"]:
        return error["message"]
    return fallback


def _json_response_payload(response) -> Optional[dict]:
    try:
        return json.loads(response.body)
    except Exception:
        return None


async def _claude_frames(queue: ByteBoundedQueue, producer: asyncio.Task, ready: asyncio.Event):
    """Drain real frames before observing EOF/failure; None denotes an idle ping."""
    while True:
        if not queue.empty():
            yield await queue.get()
        elif producer.done():
            if producer.cancelled():
                # This is the upstream task, not the connected client's task.
                raise IncompleteResponseError("上游 OpenAI 流读取被取消，回复可能不完整。")
            producer.result()  # propagate ordinary upstream errors after buffered frames
            return
        else:
            # Single consumer: nothing can enqueue/finish between these checks
            # and clear() without an await. Completion never needs a queue slot.
            ready.clear()
            try:
                async with asyncio.timeout(IDLE_PING_SECONDS):
                    await ready.wait()
            except TimeoutError:
                if queue.empty() and not producer.done():
                    yield None


def _translate_openai_sse_frame(state: ClaudeStreamState, item: Optional[bytes]):
    """Decode one input locally; retain only output frames or state genuinely needed later."""
    if item is None:
        return None, [PING_EVENT]
    # Extra leading empty lines do not dispatch an event.
    text = item.decode("utf-8", errors="replace").lstrip("\r\n")
    event = ReplayEvent(text)
    if not event.has_data:
        return None, [PING_EVENT] if text.startswith(":") else []
    data = event.data().strip(" \t\r\n")
    if not data:
        return None, []
    if data == "[DONE]":
        return "done", []
    try:
        payload = json.loads(data)
    except (ValueError, TypeError):
        raise IncompleteResponseError("上游 OpenAI 流包含无效 JSON 事件。") from None
    if not isinstance(payload, dict):
        raise IncompleteResponseError("上游 OpenAI 流事件必须是 JSON 对象。")
    if error := payload.get("error"):
        code = error.get("code") if isinstance(error, dict) else None
        try:
            code = int(code)
        except (ValueError, TypeError, OverflowError):
            code = 502
        message = _openai_error_message(payload, "上游流式响应出错。")
        hint = bounded_sse_advisory(error.get("retry_after")) if isinstance(error, dict) else None
        # A committed stream can only carry the optional wait as a bounded comment.
        frames = [f": retry-after {hint}\n\n"] if hint is not None else []
        frames.append(error_event(anthropic_error_type(code), message))
        return "error", frames
    if not payload.get("choices") and isinstance(payload.get("usage"), dict):
        state.feed_usage(payload["usage"])
        return None, []
    return None, state.feed_openai_chunk(payload)


async def _publish_delivered_signature(proof, connection):
    if connection is not None and not connection.delivery_possible:
        proof.discard()
    else:
        await proof.publish()


def _claude_stream_response(response: StreamingResponse, display_model: str, *,
                           omit_thinking: bool,
                           started_at: Optional[float] = None,
                           state: Optional[ClaudeStreamState] = None,
                           connection: Optional[ClientConnection] = None) -> ManagedStreamingResponse:
    """把上游 OpenAI SSE 流翻译成 Anthropic 事件流（claude_messages 的流式段）。

    职责与保活策略（详见模块 docstring 与 IDLE_PING_SECONDS 注释）：
    - message_start 先行（不等上游首字节）；
    - 10s 空闲补 ping、上游心跳注释帧转 ping；
    - 有界帧队列背压防慢客户端堆内存；生产者/消费者生命周期与上游
      关闭均在 finally 中收尾，客户端断开不泄漏任务与连接。
    """
    if state is None:
        state = ClaudeStreamState(display_model, omit_thinking=omit_thinking)
    upstream_iterator = response.body_iterator
    proof = state.projection if state.projection is not None else state.delivered_origin
    serialized = wire_done = False

    async def _bytes_source():
        async for chunk in upstream_iterator:
            if isinstance(chunk, str):
                yield chunk.encode("utf-8")
            else:
                yield chunk

    # Text retains the existing 128-frame read-ahead, while image slices have
    # a 2 MiB byte budget (128 x 256 KiB would otherwise retain 32 MiB).
    # Cancelling the producer also interrupts a blocked byte-budget wait.
    frame_queue = ByteBoundedQueue(maxsize=128, max_bytes=2 * 1024 * 1024)

    ready = asyncio.Event()

    async def _produce_frames():
        async for frame in sse_frames(_bytes_source()):
            await frame_queue.put(frame)
            ready.set()

    async def event_stream():
        nonlocal serialized, wire_done
        failed = False
        producer = asyncio.create_task(_produce_frames())
        producer.add_done_callback(lambda _: ready.set())
        try:
            # message_start 先行：不等上游首字节，客户端连接立刻建立
            yield state.message_start_event()
            async for item in _claude_frames(frame_queue, producer, ready):
                terminal, frames = _translate_openai_sse_frame(state, item)
                del item
                # The list contains only this translated chunk; retire each
                # rendered event once offered to the consumer.
                frames.reverse()
                while frames:
                    yield frames.pop()
                if terminal:
                    failed = terminal == "error"
                    wire_done = terminal == "done"
                    break
            if not failed:
                if not state.finish:
                    raise IncompleteResponseError("上游 OpenAI 流缺少结束原因，回复可能不完整。")
                for event in state.finish_events():
                    yield event
                serialized = True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            code, message = extract_upstream_error(exc)
            stats.add_error()
            print(f"❌ [Claude] 模型 {display_model} 流式转发失败 | HTTP {code} | {message[:200]}")
            yield error_event(anthropic_error_type(code), message[:1024])
        finally:
            if proof is not None and not (serialized and wire_done):
                proof.discard()
            # This is a translation milestone, not proof of successful ASGI
            # delivery. Keep the existing iterator-level log and token usage.
            if serialized:
                elapsed = time.monotonic() - started_at if started_at is not None else 0.0
                usage = state.usage or {}
                tokens = ""
                if usage.get("input_tokens") or usage.get("output_tokens"):
                    tokens = f" | 输入 {usage.get('input_tokens', 0)} / 输出 {usage.get('output_tokens', 0)} tokens"
                print(f"✅ [Claude] 模型 {display_model} | 流式翻译完成 | 耗时 {elapsed:.1f}s{tokens}")
            # Drain the producer in a separate owned task. Its expected
            # cancellation must not consume an independent server cancellation.
            await await_owned_close(stop_producer(producer))

    async def after_send():
        # The iterator can end before the final ASGI body is sent. A silent
        # disconnect during that write also makes an otherwise complete reply
        # ineligible, even when the server's send() did not raise.
        if serialized and wire_done and proof is not None:
            await _publish_delivered_signature(proof, connection)

    async def stop_producer(producer):
        producer.cancel()
        try:
            await producer
        except (asyncio.CancelledError, Exception):
            pass
        await cleanup()

    closing = None

    async def cleanup():
        nonlocal closing
        close = getattr(response, "aclose", None) or getattr(upstream_iterator, "aclose", None)
        if close is not None:
            if closing is None:
                closing = asyncio.ensure_future(close())
            try:
                await await_owned_close(closing)
            except Exception:
                pass

    async def close_public_response():
        if proof is not None:
            proof.discard()
        await cleanup()

    translated = ManagedStreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers=SSE_NO_BUFFER_HEADERS,
        cleanup=close_public_response,
        background=BackgroundTask(after_send) if proof is not None else None,
    )
    translated.client_connection = connection
    return translated


# ============================================================
# 主端点：POST /v1/messages
# ============================================================

@router.post("/v1/messages")
async def claude_messages(fastapi_request: Request, api_key: str = Depends(get_claude_auth)):
    """Anthropic Messages 端点：Anthropic 请求 → 内部管线 → Anthropic 响应。

    并发闸（limit_generation）内置于 call_upstream，不在此重复装饰，
    避免同一请求占用两份并发配额。
    上游错误（404 模型不可用 / 403 权限 / 429 拥堵等）如实透传状态码，
    仅把错误体改写为 Anthropic 形状；流式已开始后错误以 SSE error 事件表达。
    """
    try:
        body = await _parse_json_body(fastapi_request)
    except ClaudeRequestError as exc:
        return claude_error(400, exc.err_type, str(exc))

    raw_model = body.get("model")
    if not isinstance(raw_model, str) or not raw_model.strip():
        return claude_error(400, "invalid_request_error", "model is required")
    if "max_tokens" not in body:
        return claude_error(400, "invalid_request_error", "max_tokens is required")

    display_model, internal_model = resolve_claude_model(raw_model, await _available_models())
    try:
        request_obj = anthropic_to_openai_request(body, internal_model)
        validate_model_response_format(request_obj, label="output_config.format")
        validate_parallel_tool_calls(request_obj, serial_setting="tool_choice.disable_parallel_tool_use=true")
        preflight_schema_contracts(request_obj)
    except ClaudeRequestError as exc:
        return claude_error(exc.status, exc.err_type, str(exc))
    except SchemaValidationError as exc:
        return claude_error(400, "invalid_request_error", str(exc))

    # thinking.display=omitted：响应侧不回传思考块（对齐 vertex-master
    # includeThoughts 口径；模型照常思考，reasoning_tokens 照计 usage）。
    bind_signature_origins(request_obj, api_key)
    omit_thinking = anthropic_omit_thinking(body)
    projection = None
    delivered_origin = None
    if omit_thinking:
        projection = OmittedClaudeProjection(request_obj._signature_output_prefix, body.get("metadata"))
        request_obj._signature_output_destination = projection.set_destination
        # The OpenAI wire projection contains reasoning the public Claude
        # response has explicitly omitted. Never register it as delivered.
        request_obj._signature_output_prefix = None
        bind_omitted_claude_projection(request_obj, body.get("metadata"))
    elif request_obj._signature_output_prefix:
        # Unlike the internal Chat delivery, this translator can still refuse
        # or abandon a reply before a real public Messages result exists.
        delivered_origin = DeliveredOrigin()
        request_obj._stage_signature_origin = delivered_origin.stage
    request_started = time.monotonic()
    stream_state = None
    if request_obj.stream:
        stream_state = ClaudeStreamState(display_model, omit_thinking=omit_thinking, projection=projection,
                                        delivered_origin=delivered_origin)
        request_obj._select_buffered_order = stream_state.select_buffered_order
    connection = ClientConnection.for_request(fastapi_request)

    track_configure(model=display_model, protocol="claude",
                    stream=bool(request_obj.stream), channel=_track_channel())
    print(f"🔵 [Claude] 模型 {display_model} → {internal_model} | "
          f"{'流式' if request_obj.stream else '非流式'} | max_tokens={request_obj.max_tokens}")

    try:
        response = await call_upstream(request_obj, fastapi_request)
    except GenerationCapacityError:
        return claude_error(529, "overloaded_error", "服务并发已达上限，请稍后重试。")
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        code, message = extract_upstream_error(exc)
        print(f"❌ [Claude] 模型 {display_model} 调用失败 | HTTP {code} | {message[:200]}")
        failure = claude_error(code, anthropic_error_type(code), message[:1024])
        failure.headers.update(advisory_headers(exc))
        return failure

    if not isinstance(response, StreamingResponse):
        # 非流式 / 流式快速失败：真实 HTTP 状态码 + Anthropic 错误体
        status = getattr(response, "status_code", 200) or 200
        payload = _json_response_payload(response)
        if status >= 400 or (isinstance(payload, dict) and payload.get("error")):
            message = _openai_error_message(payload, f"上游请求失败（HTTP {status}）。")
            err_status = status if status >= 400 else 502
            failure = claude_error(err_status, anthropic_error_type(err_status), message)
            hint = response_retry_after(response)
            if hint is not None:
                failure.headers["retry-after"] = hint
            return failure
        # The parsed content owns the image now. Release the intermediate
        # OpenAI JSON body before encoding its Anthropic representation.
        body_size = len(getattr(response, "body", b""))
        response = None
        try:
            anthropic_payload = openai_completion_to_anthropic(
                payload, display_model, new_message_id(), omit_thinking=omit_thinking)
        except IncompleteResponseError as exc:
            return claude_error(502, "api_error", str(exc))
        # 与 OpenAI 通道的完成日志同构：显示模型 | 耗时 | tokens
        usage = (anthropic_payload.get("usage") or {}) if isinstance(anthropic_payload, dict) else {}
        tokens = ""
        if usage.get("input_tokens") or usage.get("output_tokens"):
            tokens = f" | 输入 {usage.get('input_tokens', 0)} / 输出 {usage.get('output_tokens', 0)} tokens"
        print(f"✅ [Claude] 模型 {display_model} | 非流式响应完成 | "
              f"耗时 {time.monotonic() - request_started:.1f}s{tokens}")
        response_class = LargeJSONResponse if body_size > 1024 * 1024 else JSONResponse
        background = None
        if projection is not None:
            projection.prepare_json(payload, anthropic_payload)
        elif delivered_origin is not None:
            delivered_origin.confirm_batch(payload["choices"][0]["finish_reason"],
                                          sum(block["type"] == "tool_use" for block in anthropic_payload["content"]))
        proof = projection if projection is not None else delivered_origin
        if proof is not None and proof.ready:
            background = BackgroundTask(_publish_delivered_signature, proof, connection)
        return response_class(content=anthropic_payload, background=background)

    # ---- 流式：OpenAI SSE 帧 → Anthropic SSE 事件 ----
    # The adapter, rather than its upstream iterator, owns the actual ASGI send.
    return _claude_stream_response(response, display_model, omit_thinking=omit_thinking, started_at=request_started,
                                   state=stream_state, connection=connection)


# ============================================================
# 方法约束：非 POST 访问 /v1/messages 系列端点 → 405 Anthropic 形状
# ============================================================

@router.api_route("/v1/messages", methods=["GET", "PUT", "DELETE", "PATCH", "OPTIONS"])
async def claude_messages_method_guard(fastapi_request: Request):
    """对齐 vertex-master 的方法检查：非 POST 一律 405 invalid_request_error。

    不挂鉴权依赖：405 语义与凭据无关（官方 API 同样未授权也回 405），
    且避免在方法错误的请求上多吐一条鉴权日志。
    """
    return claude_error(405, "invalid_request_error", "method not allowed: use POST")


@router.api_route("/v1/messages/count_tokens", methods=["GET", "PUT", "DELETE", "PATCH", "OPTIONS"])
async def claude_count_tokens_method_guard(fastapi_request: Request):
    return claude_error(405, "invalid_request_error", "method not allowed: use POST")


# ============================================================
# count_tokens：POST /v1/messages/count_tokens
# ============================================================

COUNT_TOKENS_TIMEOUT_SECONDS = 15.0
_count_tokens_slots = threading.BoundedSemaphore(4)


async def _express_exact_input_tokens(request_obj, fastapi_request: Request,
                                      raw_internal_model: str) -> Optional[int]:
    # Auxiliary requests have their own small, fail-fast budget. Saturation and
    # timeouts fall back to estimation; cancellation always propagates.
    if not _count_tokens_slots.acquire(blocking=False):
        return None
    usage = ProxyUsage()
    token = current_proxy_usage.set(usage)
    try:
        async with asyncio.timeout(COUNT_TOKENS_TIMEOUT_SECONDS):
            return await _count_exact_input_tokens(request_obj, fastapi_request, raw_internal_model)
    except TimeoutError:
        return None
    finally:
        current_proxy_usage.reset(token)
        usage.close()
        _count_tokens_slots.release()


async def _count_exact_input_tokens(request_obj, fastapi_request: Request,
                                    raw_internal_model: str) -> Optional[int]:
    """Express 通道模型侧精确计数（vertex-master CountTokensExact 的对应物）。

    计数口径：协议转换后的客户端请求本身（消息 contents + system_instruction
    + tools），不含控制台注入的 system / 预填充——count_tokens 语义是
    “客户端请求占多少 token”，注入部分是代理自身的常量开销，不由客户端
    上下文管理买单（与 vertex-master 把 prompt policy 注入计入计数的口径
    略有差异，但与 Anthropic 官方 count_tokens = 请求原文口径一致）。
    contents 由 create_gemini_prompt 生成：与真实生成请求同一转换器，
    Gemini 3.x 的思考签名哨兵也会被计入（生成时确实存在）。

    任一环节失败（无 Key / 代理解析失败 / 上游错误 / contents 为空）返回
    None，由调用方回退启发式估算：与 vertex-master「错误透传」不同，
    本服务双通道架构下计数通道≠生成通道，计数失败不代表请求无效，
    count_tokens 是辅助端点，可用性优先（Claude Code 上下文管理依赖它）。
    """
    app_state_obj = getattr(fastapi_request, "app", None)
    manager = getattr(getattr(app_state_obj, "state", None), "express_key_manager", None)
    get_record = getattr(manager, "get_express_record", None)
    if not callable(get_record):
        return None
    try:
        record = get_record()
    except Exception:
        return None
    if not isinstance(record, dict) or not record.get("key"):
        return None

    base_model, _, model_error = normalize_model_name(raw_internal_model)
    if model_error or not base_model:
        return None
    try:
        # 放线程池：create_gemini_prompt 内含远程图片下载（同步 httpx +
        # DNS + 最多 3 跳重定向 + 代理链重试，单次超时 10s）与 PIL 全图
        # 压缩——Express 主路径（express_pipeline）与 Cookie 路径（cookie_request）
        # 都已线程化，唯独此处曾在事件循环上直跑：多模态 count_tokens 可把
        # 整个服务冻结 10~40s，所有并发流的心跳集体停摆（2026-10 修复）。
        contents = await run_media_task(
            create_gemini_prompt, request_obj.messages, base_model, heavy=prompt_needs_media_pool(request_obj.messages)
        )
    except Exception:
        return None
    if not contents:
        return None

    try:
        settings = app_state.get_effective_settings(base_model)
        model_path = resolve_express_model_path(
            base_model, settings, key_project_id=str(record.get("project_id") or ""))
        client_wrapper = await FailoverSDKClient(
            ProxyChain(record).bind_to_generation(),
            lambda raw, resolved: get_pooled_express_client(record["key"], raw),
        ).prepare()
        sdk_client = getattr(client_wrapper, "client", None)
        count_tokens_call = getattr(getattr(sdk_client, "aio", None), "models", None)
        count_tokens_call = getattr(count_tokens_call, "count_tokens", None)
        if not callable(count_tokens_call):
            return None
        config = types.CountTokensConfig(
            system_instruction=system_instruction(request_obj) or None,
            tools=function_tools(request_obj) or None,
        )
        response = await count_tokens_call(model=model_path, contents=contents, config=config)
        total = getattr(response, "total_tokens", None)
        if isinstance(total, bool) or not isinstance(total, int) or total < 0:
            return None
        return total
    except ProxyResolveError:
        return None
    except Exception as exc:
        print(f"⚠️ [Claude] count_tokens 精确计数失败：{type(exc).__name__}: {exc}")
        return None


@router.post("/v1/messages/count_tokens")
async def claude_count_tokens(fastapi_request: Request, api_key: str = Depends(get_claude_auth)):
    """统计请求输入 token 数（Claude Code 上下文管理依赖此端点）。

    两级口径：
    1. Express 通道有可用 Key → 调 Vertex countTokens 拿模型侧精确计数
       （消息 contents + system_instruction + tools 全部计入，同真实生成
       请求的分词器；比 vertex-master 只计 contents 多算了 tools，对齐
       Anthropic 官方 count_tokens 含工具声明的口径）；
    2. 纯 Cookie 通道 / 精确计数任一环节失败 → 启发式估算：
       system / 消息文本 / tool_use 入参 / tool_result / 工具声明全部计入，
       CJK ≈ 1 token/字、其余 ≈ 4 字符/token；量级正确，非逐 token 精确。
    """
    try:
        body = await _parse_json_body(fastapi_request)
    except ClaudeRequestError as exc:
        return claude_error(400, exc.err_type, str(exc))

    raw_model = body.get("model")
    if not isinstance(raw_model, str) or not raw_model.strip():
        return claude_error(400, "invalid_request_error", "model is required")

    async def count():
        display_model, internal_model = resolve_claude_model(raw_model, await _available_models())
        request_obj = anthropic_to_openai_request(body, internal_model, require_max_tokens=False)
        exact = await _express_exact_input_tokens(request_obj, fastapi_request, internal_model)
        return display_model, internal_model, exact

    # The connection middleware owns receive. Join any cancelled SDK call
    # (and its semaphore) before returning; a lost caller needs no estimate.
    connection = ClientConnection.for_request(fastapi_request)
    try:
        display_model, internal_model, exact = await (connection.while_connected(count) if connection else count())
    except ClientDisconnect:
        return claude_error(499, "api_error", "客户端已断开连接，计数已取消。")
    except ClaudeRequestError as exc:
        return claude_error(exc.status, exc.err_type, str(exc))
    if exact is not None:
        print(f"🔢 [Claude] count_tokens 模型 {display_model} → {internal_model}："
              f"精确 {exact} tokens（Express 分词器）")
        result = exact
    else:
        result = estimate_request_tokens(body)
        print(f"🔢 [Claude] count_tokens 模型 {display_model} → {internal_model}：估算 ~{result} tokens")

    async def record_delivered_count():
        # Middleware owns totals and HTTP errors. Count a successful helper
        # response once, never as generated/billed usage or a discarded send.
        if connection is None or connection.delivery_possible:
            stats.add_success()

    return JSONResponse(content={"input_tokens": result}, background=BackgroundTask(record_delivered_count))


# ============================================================
# Claude Code 网关探测：GET /api/hello
# ============================================================

@router.api_route("/api/hello", methods=["GET", "HEAD"])
async def claude_hello(fastapi_request: Request):
    """Claude Code 启动时对自定义网关的可达性探测（不鉴权、不泄露信息）。"""
    response = JSONResponse(content={"status": "ok"})
    if fastapi_request.method == "HEAD":
        response.body = b""
    return response


@router.api_route("/api/hello", methods=["POST", "PUT", "DELETE", "PATCH", "OPTIONS"])
async def claude_hello_method_guard(fastapi_request: Request):
    """Claude Code 网关探测仅接受 GET/HEAD。"""
    return claude_error(405, "invalid_request_error", "method not allowed: use GET")


# ============================================================
# Anthropic 格式模型目录（供 models_api 的 /v1/models 复用）
# ============================================================

def is_anthropic_client(fastapi_request: Request) -> bool:
    """/v1/models 的响应格式判定：anthropic-version 头或 x-api-key 头存在
    即视为 Anthropic 客户端（OpenAI 客户端发 Authorization: Bearer）。
    getattr 兜底：单测里常以 SimpleNamespace 直调路由，请求对象可能没有 headers。"""
    headers = getattr(fastapi_request, "headers", None) or {}
    get_header = getattr(headers, "get", None)
    if callable(get_header):
        if get_header("anthropic-version"):
            return True
        return bool(get_header("x-api-key"))
    return False


async def anthropic_models_data() -> dict:
    """Anthropic 模型目录数据体（列表 /v1/models 与单模型检索共用）。

    变体目录与 OpenAI /v1/models 同源展开（model_catalog.catalog_variant_ids
    单一事实源）：-search 变体与按模型设置开启的抗截断后缀一并列出——
    请求侧 resolve_claude_model 对非 claude-* 名字原样透传，Claude 客户端
    可直接使用这些变体，目录应与实际能力一致。
    """
    from model_catalog import catalog_variant_ids
    from model_capabilities import split_model_alias

    available = await _available_models()
    settings_base = app_state.get_settings()
    variants: list[str] = []
    for model_id in available:
        plain_id, _ = split_model_alias(model_id)
        effective = app_state.get_effective_settings(plain_id, base=settings_base)
        variants.extend(catalog_variant_ids(model_id, effective))
    return anthropic_models_payload(available, variants)


async def anthropic_model_list(query_params) -> JSONResponse:
    data = await anthropic_models_data()
    try:
        page = anthropic_model_page(data["data"], query_params)
    except CatalogPageError as exc:
        return claude_error(400, "invalid_request_error", str(exc))
    return JSONResponse(content=page)

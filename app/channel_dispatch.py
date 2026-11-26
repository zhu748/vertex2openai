"""跨通道上游调度与兜底引擎（2026-11 P2-7 自 routes/chat_api.py 下沉）。

HTTP 端点（参数解析、认证、resumable 编排）仍在 routes/chat_api.py；
本模块持有通道选择、快慢 setup 窗口与「未输出内容前可跨通道接力」的
兜底状态机。路由层经 re-export 保持历史测试 seam（patch
routes.chat_api._call_upstream / ._dispatch_upstream 的语义不变）。
"""
import asyncio
import json
import re
import time
from collections import deque
from contextlib import suppress
from typing import Optional

from fastapi import Request
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.requests import ClientDisconnect

from admission import SETUP_FAST_WINDOW_SECONDS, ManagedStreamingResponse
from channel_capabilities import cookie_feature_error
from credential_manager import credential_manager
from logger import stats
from models import OpenAIRequest
from retry_policy import note_retry_deadline, response_requests_delay, response_retry_after, retry_after_seconds
from request_connection import ClientConnection
from stream_cleanup import await_owned_close
from resumable import DONE_FRAME, DetachedRequest, request_timeout_seconds
from runtime_state import app_state
from streaming import (SSE_NO_BUFFER_HEADERS, StreamDeadlineExceeded, close_setup_task, close_stream_response,
                       keepalive_sse, reads_before_deadline, response_before_deadline)
from upstream_errors import create_openai_error_response, extract_upstream_error
from upstreams.cookie_proxy import CookieProxyUpstream
from upstreams.express_sdk import ExpressSDKUpstream

# 实例化多通道策略（原属 routes/chat_api.py；引擎与路由共用同一实例，
# 测试经任一模块名 patch 实例方法都指向这里）
express_upstream = ExpressSDKUpstream()
cookie_upstream = CookieProxyUpstream()

_DONE_FRAME_BYTES = DONE_FRAME.encode("ascii")
_TERMINAL_FINALIZE_TIMEOUT_SECONDS = 0.25
_FALLBACK_PREFIX_MAX_BYTES = 2 * 1024 * 1024
_FALLBACK_PREFIX_MAX_FRAMES = 128

# ============================================================
# 跨通道兜底重试（对齐上游 bad-woman/vertex2openai 的 Key ↔ Cookie 互备）
#
# 主通道（Express 或 Cookie）在**尚未向客户端输出任何内容**前失败，
# 且错误属于可兜底类（429 拥堵 / 401 / 403 凭证问题 / 5xx 服务端错误）
# 时，自动切换到另一通道接力完成回复；已输出内容绝不切换（半截答案
# 不能重发，与通道内重试的安全线同口径）。控制台可经
# cross_channel_fallback 开关关闭。
# ============================================================


def _fallback_eligible_code(code) -> bool:
    """可兜底状态码：429 拥堵、401/403 凭证与权限问题、5xx 服务端错误。

    400/404/422 等请求级错误换通道同样失败，不兜底，避免无谓二次调用。
    """
    if not isinstance(code, int):
        return False
    return code in (401, 403, 429) or 500 <= code <= 599


def _fallback_eligible_response(response, default_code: int) -> bool:
    return (_fallback_eligible_code(getattr(response, "status_code", default_code))
            and not response_requests_delay(response))


def _structured_locks_express(request_obj: OpenAIRequest) -> bool:
    """Requests with unsupported Cookie features must stay on Express."""
    return bool(cookie_feature_error(
        request_obj, structured_output=app_state.get_setting("cookie_structured_output", False)))


def _express_manager(fastapi_request: Request):
    """从请求对象安全取出 Express Key 管理器（兼容测试桩与 DetachedRequest）。"""
    # Starlette always provides request.state, but the manager is registered
    # on app.state. An empty request state must not hide the real Key pool.
    app = getattr(fastapi_request, "app", None)
    manager = getattr(getattr(app, "state", None), "express_key_manager", None)
    if manager is not None:
        return manager
    return getattr(getattr(fastapi_request, "state", None), "express_key_manager", None)


def _other_channel(fastapi_request: Request, primary: str, request_obj: OpenAIRequest) -> Optional[str]:
    """跨通道兜底开启且另一通道有可用凭证时，返回备用通道名；否则 None。"""
    if not app_state.get_setting("cross_channel_fallback", False):
        return None
    if primary == "express":
        # 结构化输出锁定 Express：没有可用的 Cookie 备用
        if _structured_locks_express(request_obj):
            return None
        return "cookie" if credential_manager.has_cookie_record() else None
    if primary == "cookie":
        manager = _express_manager(fastapi_request)
        if manager is None or manager.get_total_keys() == 0:
            return None
        return "express"
    return None


def _channel_label(channel: str) -> str:
    return "Express API Key" if channel == "express" else "Studio Cookie 直连"


async def _invoke_channel(request_obj: OpenAIRequest, fastapi_request: Request, channel: str):
    """按通道直接调用上游（兜底只一层，不再递归）。"""
    if channel == "cookie":
        return await cookie_upstream.chat_completions(request_obj, fastapi_request)
    return await express_upstream.chat_completions(request_obj, fastapi_request)


# 内容帧快筛（2026-11 O-7）：仅在值确为**非空**时命中——空串
# （"content": ""，Express 流式真实存在）不匹配（前瞻要求引号后有字符），
# 与解析后 has_payload 口径一致；JSON 转义会给内嵌同类子串加反斜杠，不误报。
_CONTENT_FRAME_RE = re.compile(
    r'"(?:content|reasoning_content)":\s*"(?=[^"])|"tool_calls":\s*\[\s*\{')


def _sse_frame_kind(frame: str) -> tuple[str, Optional[int]]:
    """解析一帧 SSE：返回 (类型, 错误码)。

    类型 ∈ keepalive（注释帧/保活帧）/ done（[DONE]）/ error（含 advisory_error）/
    content（真实内容：正文、思考或工具调用增量）/ finish（仅含结束标记、
    无任何内容增量的空收尾帧）/ other（role 声明等）。

    分类顺序按热路径成本排布：在兜底的预提交阶段，内容帧用廉价子串
    预筛直通、零 JSON 解析；提交后不再分类。
    疑似错误/收尾/角色/用量帧再走完整反序列化。子串模式只在「值确为
    非空」时命中（空串/空列表不匹配），与解析后 has_payload 口径一致；
    JSON 转义会把内嵌的同类子串打上反斜杠，不会误报。bytes 帧归一化。
    """
    if isinstance(frame, (bytes, bytearray)):
        frame = frame.decode("utf-8", "replace")
    stripped = frame.strip()
    if not stripped.startswith("data:"):
        comments_only = all(not line or line.startswith(":") for line in stripped.splitlines())
        return ("keepalive" if comments_only else "other", None)
    payload_text = stripped[len("data:"):].strip()
    if payload_text == "[DONE]":
        return ("done", None)
    if _CONTENT_FRAME_RE.search(payload_text):
        return ("content", None)
    if '"error"' not in payload_text and '"choices"' not in payload_text:
        # 无错误负载也无 choices 结构（如纯 usage 帧）：不可能是
        # error/content/finish，直接归 other，省一次完整反序列化。
        return ("other", None)
    try:
        payload = json.loads(payload_text)
    except ValueError:
        return ("other", None)
    if not isinstance(payload, dict):
        return ("other", None)
    err = payload.get("error")
    if isinstance(err, dict):
        code = err.get("code")
        # Already-committed SDK streams carry the upstream Retry-After in the
        # error event. Do not replace that advisory with another generation.
        if retry_after_seconds(err.get("retry_after")):
            return ("advisory_error", code if isinstance(code, int) else None)
        return ("error", code if isinstance(code, int) else None)
    choices = payload.get("choices")
    if isinstance(choices, list) and choices and isinstance(choices[0], dict):
        choice = choices[0]
        delta = choice.get("delta") or {}
        has_payload = bool(
            delta.get("content") or delta.get("reasoning_content") or delta.get("tool_calls")
        )
        if has_payload:
            return ("content", None)
        if choice.get("finish_reason"):
            # 空内容 + 仅结束标记的帧不是真实输出：旧实现把它计入 content，
            # 会使跨通道兜底误判「已出内容」而放弃切换（错误帧晚于空收尾帧
            # 到达时被锁死）。独立分类，保持可兜底。
            return ("finish", None)
    return ("other", None)


async def _try_channel_fallback(
    request_obj: OpenAIRequest,
    fastapi_request: Request,
    fallback_channel: str,
    primary_label: str,
    reason: str,
    deadline: float,
):
    """启动备用通道调用；失败返回 None（由调用方回退主通道错误）。"""
    async def attempt():
        print(
            f"🔁 [跨通道兜底] 主通道[{primary_label}]{reason}，"
            f"自动切换至[{_channel_label(fallback_channel)}]接力完成回复。"
        )
        return await _invoke_channel(request_obj, fastapi_request, fallback_channel)

    try:
        return await response_before_deadline(attempt, deadline, clock=time.monotonic)
    except (asyncio.CancelledError, StreamDeadlineExceeded):
        raise
    except Exception as exc:
        code, msg = extract_upstream_error(exc)
        # 不在此 add_error（2026-11 C-3 修复）：主通道失败已计过一次错误，
        # 兜底通道自身的流式/非流式路径也会按各自口径计数，这里再计会使
        # 同一请求在大盘上出现 2 个错误（success + error 之和超过 total）。
        print(f"❌ [跨通道兜底] 备用通道[{_channel_label(fallback_channel)}]启动失败："
              f"HTTP {code} | {str(msg)[:200]}")
        return None


def _stream_closer(response):
    """A fallback wrapper and its body share one joinable primary close."""
    task = None

    async def close():
        nonlocal task
        if task is None:
            task = asyncio.create_task(close_stream_response(response))
        await await_owned_close(task)

    return close


async def _stream_with_fallback_frames(
    primary_response: StreamingResponse,
    request_obj: OpenAIRequest,
    fastapi_request: Request,
    fallback_channel: str,
    primary_channel: str,
    deadline: float,
    *, close_primary=None,
):
    """Switch only before committing any generation-specific data to the caller."""
    primary_label = _channel_label(primary_channel)
    fallback_label = _channel_label(fallback_channel)
    committed = False
    prefix = deque()
    prefix_bytes = 0
    pending_error_frame: Optional[str] = None
    pending_error_code: Optional[int] = None
    close_primary = close_primary or _stream_closer(primary_response)
    try:
        async for frame in primary_response.body_iterator:
            if committed:
                yield frame
                continue
            kind, code = _sse_frame_kind(frame)
            if kind == "keepalive":
                yield frame
                continue
            if kind == "error" and _fallback_eligible_code(code if code is not None else 502):
                pending_error_frame = frame
                pending_error_code = code if code is not None else 502
                break  # close the losing attempt; never read or forward its trailers
            if kind in ("other", "finish"):
                # Even without text, role, usage and opaque signatures carry this
                # response's identity. Do not mix them into a winning standby response.
                remaining = _FALLBACK_PREFIX_MAX_BYTES - prefix_bytes
                if len(prefix) < _FALLBACK_PREFIX_MAX_FRAMES and len(frame) <= remaining:
                    size = (len(frame.encode("utf-8"))
                            if isinstance(frame, str) and not frame.isascii() else len(frame))
                    if size <= remaining:
                        prefix.append(frame)
                        prefix_bytes += size
                        continue
            # Content, a terminal/advisory, or a full staging buffer commits
            # the primary. A full buffer is not permission to drop signed data.
            committed = True
            while prefix:
                yield prefix.popleft()
            yield frame
        else:
            # Preserve a metadata-only response when the producer ends normally.
            while prefix:
                yield prefix.popleft()
    finally:
        prefix.clear()
        await close_primary()

    if pending_error_frame is None:
        return

    alt = await _try_channel_fallback(
        request_obj, fastapi_request, fallback_channel, primary_label,
        f"流式失败（HTTP {pending_error_code}）且尚未输出内容", deadline)
    if alt is None:
        # 备用通道不可用：回放主通道错误帧，如实告知失败原因
        yield pending_error_frame
        yield DONE_FRAME
        return
    if isinstance(alt, StreamingResponse):
        try:
            async for frame in alt.body_iterator:
                yield frame
        finally:
            # Managed transports can own more than their iterator (also when
            # cancelled before the first token). Hold this generation until both close.
            await await_owned_close(close_stream_response(alt))
        return
    # 备用通道返回了非流式响应（配置/鉴权错误等）：保留主通道错误，不误导排查方向
    alt_code = getattr(alt, "status_code", 502)
    print(f"ℹ️ [跨通道兜底] 备用通道[{fallback_label}]未返回流式响应（HTTP {alt_code}），回放主通道错误。")
    yield pending_error_frame
    yield DONE_FRAME


def _stream_fallback_candidate(result, fastapi_request, primary_channel, request_obj) -> bool:
    """快路径结果是否需要**尝试**跨通道兜底（决定同步返回还是移入生成器）。

    流式响应的兜底在流内懒触发（不阻塞）；无备用通道或不可兜底的错误
    保持同步路径（快验证错误保留 HTTP 状态码语义）。只有「可兜底的快
    错误」需要进入带心跳的生成器路径（2026-11 C-5）。
    """
    if isinstance(result, StreamingResponse):
        return False
    return (_fallback_eligible_response(result, 502)
            and _other_channel(fastapi_request, primary_channel, request_obj) is not None)


async def _apply_stream_fallback(
    result,
    request_obj: OpenAIRequest,
    fastapi_request: Request,
    primary_channel: str,
    deadline: float,
):
    """流式请求的主通道响应 → 附加跨通道兜底语义；不可兜底时原样返回。"""
    fallback_channel = _other_channel(fastapi_request, primary_channel, request_obj)
    if fallback_channel is None:
        return result
    if isinstance(result, StreamingResponse):
        headers = dict(result.headers)
        headers.pop("content-length", None)
        close_primary = _stream_closer(result)
        return ManagedStreamingResponse(
            _stream_with_fallback_frames(
                result, request_obj, fastapi_request, fallback_channel, primary_channel,
                deadline, close_primary=close_primary),
            media_type="text/event-stream", headers=headers, background=result.background,
            cleanup=close_primary,  # covers a consumer that never even starts our generator
        )
    # 主通道 prepare 阶段快速失败（鉴权/配置错误）：先试备用通道再报错
    if not _fallback_eligible_response(result, 502):
        return result
    code = getattr(result, "status_code", 502)
    alt = await _try_channel_fallback(
        request_obj, fastapi_request, fallback_channel, _channel_label(primary_channel),
        f"请求初始化失败（HTTP {code}）", deadline)
    if alt is None:
        return result
    if isinstance(alt, StreamingResponse):
        return alt
    if getattr(alt, "status_code", 502) < 400:
        return alt
    print(f"ℹ️ [跨通道兜底] 备用通道也未能接手（HTTP {getattr(alt, 'status_code', 502)}），返回主通道错误。")
    return result


async def _apply_json_fallback(
    result,
    request_obj: OpenAIRequest,
    fastapi_request: Request,
    primary_channel: str,
    deadline: float,
):
    """非流式请求：主通道返回可兜底错误时，用另一通道再试一次。"""
    if not _fallback_eligible_response(result, 200):
        return result
    code = getattr(result, "status_code", 200)
    fallback_channel = _other_channel(fastapi_request, primary_channel, request_obj)
    if fallback_channel is None:
        return result
    alt = await _try_channel_fallback(
        request_obj, fastapi_request, fallback_channel, _channel_label(primary_channel),
        f"非流式失败（HTTP {code}）", deadline)
    if alt is None:
        return result
    if getattr(alt, "status_code", 502) < 400:
        return alt
    print(f"ℹ️ [跨通道兜底] 备用通道非流式也失败（HTTP {getattr(alt, 'status_code', 502)}），返回主通道错误。")
    return result



async def _dispatch_upstream(request_obj: OpenAIRequest, fastapi_request: Request):
    """Select an enabled upstream credential using the saved channel mode."""
    deadline = note_retry_deadline(fastapi_request, time.monotonic() + request_timeout_seconds())
    # 本次请求选中的主通道（express / cookie）：跨通道兜底按它决定备用方向。
    selected_channel: list[str] = []

    def _note_channel(channel: str) -> None:
        selected_channel.clear()
        selected_channel.append(channel)

    async def invoke_primary():
        kwargs = {}
        if app_state.get_channel_mode() == "mixed":
            # 结构化输出默认只选 Express（Cookie 私有协议未验证）；控制台
            # 打开 cookie_structured_output 后 Cookie 记录同样参与选择。
            structured = _structured_locks_express(request_obj)
            record = credential_manager.get_mixed_record(
                fastapi_request.app.state.express_key_manager, express_only=structured)
            if record is None:
                return JSONResponse(status_code=401, content=create_openai_error_response(
                    401, "没有可用的上游账号。请启用 Express Key 或填写完整的 Cookie 与 Project ID；结构化输出（未开启 Cookie 支持）、多候选与 logprobs 请求需要 Express Key。",
                    "authentication_error"))
            channel = record["channel"]
            kwargs["credential_record"] = record
        else:
            channel = "cookie" if app_state.is_web_proxy_enabled() else "express"
        _note_channel(channel)
        if time.monotonic() >= deadline:  # even credential selection consumes the same budget
            raise StreamDeadlineExceeded()
        upstream = cookie_upstream if channel == "cookie" else express_upstream
        return await upstream.chat_completions(request_obj, fastapi_request, **kwargs)

    async def prepare():
        return await response_before_deadline(invoke_primary, deadline, clock=time.monotonic)

    def _primary_channel() -> str:
        # prepare() 异常或尚未执行时无主通道信息，返回空串禁用兜底
        return selected_channel[0] if selected_channel else ""

    if request_obj.stream:
        # Start SSE promptly even if proxy startup or image preprocessing is
        # slow. Fast validation errors can still keep their HTTP status.
        setup = asyncio.create_task(prepare())

        try:
            done, _ = await asyncio.wait({setup}, timeout=SETUP_FAST_WINDOW_SECONDS)
        except BaseException:
            await close_setup_task(setup)
            raise
        primary = setup.result() if done else None
        if done and not _stream_fallback_candidate(
                primary, fastapi_request, _primary_channel(), request_obj):
            # Fast streams and non-retryable errors keep their real HTTP status.
            response = await _apply_stream_fallback(
                primary, request_obj, fastapi_request, _primary_channel(), deadline)
        else:
            # Slow setup and quick retryable errors both get heartbeats while
            # preparing their alternative. Until a body is adopted, setup owns
            # any response arriving even after cancellation.
            adopted = False

            async def close_setup():
                if not adopted:
                    await close_setup_task(setup, join=True)

            async def prepared_frames():
                nonlocal adopted
                result = await _apply_stream_fallback(
                    await setup, request_obj, fastapi_request, _primary_channel(), deadline)
                if isinstance(result, StreamingResponse):
                    adopted = True
                    try:
                        async for frame in result.body_iterator:
                            yield frame
                    finally:
                        # The result may be a *different* upstream response than
                        # setup.result(). async for alone does not close its owner.
                        await await_owned_close(close_stream_response(result))
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
                    hint = response_retry_after(result)
                    if hint is not None and isinstance(payload["error"], dict):
                        payload["error"]["retry_after"] = hint
                    yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
                    yield DONE_FRAME
            response = ManagedStreamingResponse(prepared_frames(), media_type="text/event-stream", cleanup=close_setup)
    else:
        async def generate_json():
            return await _apply_json_fallback(
                await prepare(), request_obj, fastapi_request, _primary_channel(), deadline)

        # DetachedRequest forwards .scope and thus exposes the original
        # connection too. Its buffer, not this request, owns cancellation.
        connection = getattr(fastapi_request, "scope", {}).get("state", {}).get("client_connection")
        if isinstance(connection, ClientConnection) and not isinstance(fastapi_request, DetachedRequest):
            try:
                response = await connection.while_connected(generate_json, discard=close_stream_response)
            except ClientDisconnect:
                return JSONResponse(status_code=499, content=create_openai_error_response(
                    499, "客户端已断开连接，请求已取消。", "client_closed_request"))
        else:
            response = await generate_json()
    if isinstance(response, StreamingResponse):
        original = response.body_iterator

        async def relay():
            stream = keepalive_sse(original, fastapi_request)
            error = terminal = None
            try:
                async for frame in reads_before_deadline(stream, deadline):
                    if frame == DONE_FRAME or frame == _DONE_FRAME_BYTES:
                        # Complete the producer's normal bookkeeping before handing
                        # off [DONE]: downstream adapters may stop reading there.
                        # The result is final, so do not apply the generation
                        # deadline to this single cleanup pull or forward a trailer.
                        with suppress(Exception):
                            async with asyncio.timeout(_TERMINAL_FINALIZE_TIMEOUT_SECONDS):
                                await anext(stream, None)
                        terminal = frame
                        break
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
                terminal = DONE_FRAME
            finally:
                await stream.aclose()
            if error is not None:
                yield f"data: {json.dumps(error, ensure_ascii=False)}\n\n"
            if terminal is not None:
                yield terminal
        headers = dict(response.headers)
        headers.pop("content-length", None)
        headers.update(SSE_NO_BUFFER_HEADERS)
        return ManagedStreamingResponse(
            relay(), status_code=response.status_code, headers=headers, background=response.background,
            cleanup=getattr(response, "aclose", original.aclose))
    return response

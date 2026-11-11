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
from contextlib import suppress
from typing import Optional

from fastapi import Request
from fastapi.responses import JSONResponse, StreamingResponse

from admission import SETUP_FAST_WINDOW_SECONDS, ManagedStreamingResponse
from channel_capabilities import cookie_feature_error
from credential_manager import credential_manager
from logger import stats
from models import OpenAIRequest
from resumable import DONE_FRAME, request_timeout_seconds
from runtime_state import app_state
from streaming import SSE_NO_BUFFER_HEADERS, close_setup_task, keepalive_sse
from upstream_errors import create_openai_error_response, extract_upstream_error
from upstreams.cookie_proxy import CookieProxyUpstream
from upstreams.express_sdk import ExpressSDKUpstream

# 实例化多通道策略（原属 routes/chat_api.py；引擎与路由共用同一实例，
# 测试经任一模块名 patch 实例方法都指向这里）
express_upstream = ExpressSDKUpstream()
cookie_upstream = CookieProxyUpstream()

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
        return "cookie" if credential_manager.get_cookie_record() is not None else None
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

    类型 ∈ keepalive（注释帧/保活帧）/ done（[DONE]）/ error（错误负载）/
    content（真实内容：正文、思考或工具调用增量）/ finish（仅含结束标记、
    无任何内容增量的空收尾帧）/ other（role 声明等）。

    分类顺序按热路径成本排布（2026-11 O-7）：兜底开启时长流的每帧都要
    过本函数——内容帧（绝大多数）用廉价子串预筛直通、零 JSON 解析；
    疑似错误/收尾/角色/用量帧再走完整反序列化。子串模式只在「值确为
    非空」时命中（空串/空列表不匹配），与解析后 has_payload 口径一致；
    JSON 转义会把内嵌的同类子串打上反斜杠，不会误报。bytes 帧归一化。
    """
    if isinstance(frame, (bytes, bytearray)):
        frame = frame.decode("utf-8", "replace")
    stripped = frame.strip()
    if not stripped.startswith("data:"):
        return ("keepalive", None)
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
    print(
        f"🔁 [跨通道兜底] 主通道[{primary_label}]{reason}，"
        f"自动切换至[{_channel_label(fallback_channel)}]接力完成回复。"
    )
    try:
        async with asyncio.timeout(max(0.001, deadline - time.monotonic())):
            return await _invoke_channel(request_obj, fastapi_request, fallback_channel)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        code, msg = extract_upstream_error(exc)
        # 不在此 add_error（2026-11 C-3 修复）：主通道失败已计过一次错误，
        # 兜底通道自身的流式/非流式路径也会按各自口径计数，这里再计会使
        # 同一请求在大盘上出现 2 个错误（success + error 之和超过 total）。
        print(f"❌ [跨通道兜底] 备用通道[{_channel_label(fallback_channel)}]启动失败："
              f"HTTP {code} | {str(msg)[:200]}")
        return None


async def _stream_with_fallback_frames(
    primary_response: StreamingResponse,
    request_obj: OpenAIRequest,
    fastapi_request: Request,
    fallback_channel: str,
    primary_channel: str,
    deadline: float,
):
    """主通道流式帧转发：在首个真实内容帧之前以可兜底错误终止时，切换备用通道。

    主通道的错误帧与后续收尾帧（[DONE] 等）会被吞掉，客户端只看到备用
    通道的接力输出；一旦出现过真实内容帧（或主通道正常结束），不再兜底。
    """
    primary_label = _channel_label(primary_channel)
    fallback_label = _channel_label(fallback_channel)
    saw_content = False
    fallback_started = False
    pending_error_frame: Optional[str] = None
    pending_error_code: Optional[int] = None
    iterator = primary_response.body_iterator
    try:
        async for frame in iterator:
            kind, code = _sse_frame_kind(frame)
            if kind == "content" and not fallback_started:
                # REQ-P2-7（2026-11 收敛）：主通道 error 帧之后迟到的
                # content 帧属于已被判死的尝试（错误帧已吞、内容不会
                # 下发），不再计入 saw_content——否则流既不切备用通道、
                # 又因错误帧被吞而无 [DONE] 终止语义，客户端只能靠
                # 自身超时判死。修正后 saw_content 与 fallback_started
                # 互斥（内容先于错误则错误分支不触发）。
                saw_content = True
            elif (kind == "error" and not saw_content and not fallback_started
                    and _fallback_eligible_code(code if code is not None else 502)):
                # 吞掉错误帧，主通道收尾后切换备用通道接力
                fallback_started = True
                pending_error_frame = frame
                pending_error_code = code if code is not None else 502
                continue
            if fallback_started:
                # 主通道错误后的收尾帧（[DONE] / 尾随心跳 / 迟到内容）不再下发
                continue
            yield frame
    finally:
        close = getattr(iterator, "aclose", None)
        if close:
            with suppress(Exception):
                await close()

    # 互斥不变量：fallback_started=True 蕴含 saw_content=False（上方守卫）。
    # 未触发兜底时主通道全部帧已原样转发（含自身的 [DONE]），直接结束。
    if not fallback_started:
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
        alt_iter = alt.body_iterator
        try:
            async for frame in alt_iter:
                yield frame
        finally:
            close = getattr(alt_iter, "aclose", None)
            if close:
                with suppress(Exception):
                    await close()
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
    fallback_channel = _other_channel(fastapi_request, primary_channel, request_obj)
    return fallback_channel is not None and _fallback_eligible_code(
        getattr(result, "status_code", 502))


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
        return StreamingResponse(
            _stream_with_fallback_frames(
                result, request_obj, fastapi_request, fallback_channel, primary_channel, deadline),
            media_type="text/event-stream",
            headers=headers,
            background=result.background,
        )
    # 主通道 prepare 阶段快速失败（鉴权/配置错误）：先试备用通道再报错
    code = getattr(result, "status_code", 502)
    if not _fallback_eligible_code(code):
        return result
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
    code = getattr(result, "status_code", 200)
    if not _fallback_eligible_code(code):
        return result
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
    deadline = time.monotonic() + request_timeout_seconds()
    # 本次请求选中的主通道（express / cookie）：跨通道兜底按它决定备用方向。
    selected_channel: list[str] = []

    def _note_channel(channel: str) -> None:
        selected_channel.clear()
        selected_channel.append(channel)

    async def prepare():
        async with asyncio.timeout(max(0.001, deadline - time.monotonic())):
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
                upstream = cookie_upstream if record["channel"] == "cookie" else express_upstream
                _note_channel(record["channel"])
                return await upstream.chat_completions(request_obj, fastapi_request, credential_record=record)
            if app_state.is_web_proxy_enabled():
                _note_channel("cookie")
                return await cookie_upstream.chat_completions(request_obj, fastapi_request)
            _note_channel("express")
            return await express_upstream.chat_completions(request_obj, fastapi_request)

    def _primary_channel() -> str:
        # prepare() 异常或尚未执行时无主通道信息，返回空串禁用兜底
        return selected_channel[0] if selected_channel else ""

    if request_obj.stream:
        # Start SSE promptly even if proxy startup or image preprocessing is
        # slow. Fast validation errors can still keep their HTTP status.
        setup = asyncio.create_task(prepare())

        async def close_setup():
            await close_setup_task(setup)

        try:
            done, _ = await asyncio.wait({setup}, timeout=SETUP_FAST_WINDOW_SECONDS)
        except BaseException:
            await close_setup()
            raise
        if done:
            primary = setup.result()
            if not _stream_fallback_candidate(
                    primary, fastapi_request, _primary_channel(), request_obj):
                # 流式响应（兜底在流内懒触发）或不可兜底的快错误（保持
                # HTTP 状态码）：同步路径，无额外等待。
                response = await _apply_stream_fallback(
                    primary, request_obj, fastapi_request, _primary_channel(), deadline)
            else:
                # 可兜底的快错误：兜底统一移入带心跳的生成器路径（2026-11
                # C-5）。旧实现在此同步 await 备用通道（含最长 30 秒媒体
                # 下载），客户端 SSE 尚未建立、零字节无心跳——同一失败落在
                # 50ms 窗口前后体验完全不同；现在与慢路径一致：等待期间
                # keepalive 心跳照发，错误以 SSE 错误帧交付（流式客户端本
                # 就要处理该形态，慢路径一直是这么交付的）。
                async def fast_prepared_frames():
                    result = await _apply_stream_fallback(
                        primary, request_obj, fastapi_request, _primary_channel(), deadline)
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
                response = ManagedStreamingResponse(fast_prepared_frames(), media_type="text/event-stream", cleanup=close_setup)
        else:
            async def prepared_frames():
                result = await setup
                result = await _apply_stream_fallback(
                    result, request_obj, fastapi_request, _primary_channel(), deadline)
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
        response = await _apply_json_fallback(
            await prepare(), request_obj, fastapi_request, _primary_channel(), deadline)
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
        headers.update(SSE_NO_BUFFER_HEADERS)
        return ManagedStreamingResponse(
            relay(), status_code=response.status_code, headers=headers, background=response.background,
            cleanup=getattr(response, "aclose", original.aclose))
    return response

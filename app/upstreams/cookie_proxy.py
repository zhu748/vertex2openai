"""Cookie 通道调用编排：认证、重试、协议降级与响应交付。

请求构造由 cookie_request 负责，响应解析由 cookie_response 负责。
"""
import asyncio
import json
import re
import time
import traceback
import uuid
from typing import Any, AsyncGenerator, Dict

import config as app_config
import httpx
import model_capabilities as mc
from anti_truncation import buffered_sse, prepare_transport, split_model_alias
from cookie_auth import BATCH_GRAPHQL_URL, build_headers
from fastapi import Request
from fastapi.responses import JSONResponse, StreamingResponse
from impersonate import open_upstream_client
from logger import stats
from message_processing import _create_safety_ratings_html
from models import OpenAIRequest
from prefill import PrefillDeduper, strip_prefill_overlap
from proxy_failover import ProxyChain
from credential_manager import credential_manager
from response_validation import (
    AntiTruncationError,
    FINISH_CONTENT_FILTER,
    IncompleteResponseError,
    PendingMetadataGate,
    has_finish_reason,
    map_finish_reason_to_openai,
    require_finish_reason,
)
from retry_policy import (
    get_retry_settings,
    is_retryable_exception,
    is_retryable_message,
    is_retryable_status,
    retry_delay,
    wait_for_retry,
)
from runtime_state import app_state
from schema_validation import SchemaValidationError
from request_validation import validate_openai_request
from streaming import HEARTBEAT, HEARTBEAT_FRAME, with_heartbeats
from thought_signatures import (
    ordinary_part_metadata,
    thought_signature_extra,
)
from upstream_errors import create_openai_error_response, extract_upstream_error
from upstreams.base import BaseUpstream
from upstreams.cookie_request import (
    COOKIE_STRUCTURED_OUTPUT_DISABLED_MESSAGE,
    build_request_body_async,
    classify_tool_traffic,
    is_builtin_search_name,
    normalized_tool_name,
)
from upstreams.cookie_response import (
    RawSampler,
    TextSource,
    apply_wire_signature,
    extract_events,
    iter_json_objects,
    make_openai_chunk,
    openai_tool_call,
)
from upstreams.request_preparation import prepare_messages
from usage import make_usage_chunk, map_usage, wants_usage
from request_tracking import configure as track_configure, upstream_attempt, upstream_chunk
from first_reply import FirstReplyGuard, FirstReplyTimeout, timed_call, timeout_seconds

COOKIE_EXPIRED_KEYWORDS = [
    "permission",
    "denied",
    "aiplatform.endpoints.predict",
    "not authorized",
    "unauthenticated",
    "login required",
    "session expired",
    "invalid credentials",
]


PROJECT_ERROR_KEYWORDS = [
    "requires billing",
    "billing to be enabled",
    "billing account",
    "has not been used in project",
    "is not found and cannot be used",
    "project not found",
    # 注：不含 "invalid argument"——那是通用 400 文案（字段非法 / 原生函数
    # Schema 被拒都常带它），归进来会把可降级的协议拒绝误报成“项目/计费
    # 问题”，阻断降级重试并给出误导性提示。项目级误判由下方 projects/
    # 资源路径 + denied 的组合判断兜底。
]


PROJECT_ERROR_HINT = (
    "\n\n💡 这看起来是**项目层面**的问题，不是 Cookie 失效，重取 Cookie 无用。请依次检查：\n"
    "1) 控制台里的 Project ID 是否填对（要用你能在 Agent Platform Studio 里正常出文的那个项目）；\n"
    "2) 该项目是否已**开启计费**（Google 对这条接口要求计费账号；未开启会报 requires billing）；\n"
    "3) 当前登录的 Google 账号对该项目是否有权限（换项目或换账号试试）。"
)


def _is_project_error(error_msg: str) -> bool:
    """是否为项目/计费/权限类错误（与 Cookie 失效区分开）。"""
    lower = (error_msg or "").lower()
    if any(kw in lower for kw in PROJECT_ERROR_KEYWORDS):
        return True
    # "Permission 'aiplatform.endpoints.predict' denied on resource ...projects/xxx"
    # 这种同时命中 cookie 关键词，但既可能是 Cookie 没登录、也可能是项目没权限，
    # 只要错误里点名了具体项目资源，就按项目问题给出更有用的指引。
    return ("projects/" in lower or "project #" in lower) and "denied" in lower


COOKIE_REFRESH_HINT = (
    "\n\n💡 Cookie 通常较为持久（只要不退出登录/改密码/被 Google 主动失效，可维持数周甚至更久）；"
    "仅当确实出现权限错误时才需更新。"
    "重新获取：电脑浏览器打开 console.cloud.google.com，F12 → Network，"
    "复制任意请求的 Cookie 头（或用 Cookie-Editor 导出），到大盘粘贴保存。"
)


_THINKING_RUNAWAY_HINT = (
    "这通常是原生思考在高强度下跑飞或被截断（前端如 SillyTavern 常发 reasoning_effort=xhigh，"
    "会覆盖控制台档位）。解决：控制台“模型参数→思考强度→原生思考控制”选“关闭原生思考”"
    "（可用“保存为该模型专属”只对本模型生效），即忽略前端强度、压到 minimal 并剥离原生思考。"
)


_NO_BODY_HINT = (
    "可能被安全策略拦截或接口行为变化；原始响应样本已写入运行日志，可到大盘“运行日志”页查看。"
)


def _no_body_detail(finish_raw, blocked_msg) -> str:
    """无正文诊断前缀：结束原因 + promptFeedback 拦截说明（三条路径共用）。"""
    detail = f"finishReason={finish_raw or '无'}"
    if blocked_msg:
        detail += f"；promptFeedback 拦截：{blocked_msg}"
    return detail


def _no_body_desc_hint(has_thought: bool, strip_thoughts: bool) -> tuple:
    """无正文时的 (描述, 处置提示)：只有思考 → 跑飞提示，完全为空 → 通用提示。"""
    desc = "只返回了思考、未返回正文" if has_thought else "未返回任何内容"
    return desc, (_THINKING_RUNAWAY_HINT if (has_thought and not strip_thoughts) else _NO_BODY_HINT)


def _no_body_outcome(finish_raw, blocked_msg, has_thought: bool, strip_thoughts: bool) -> tuple:
    """无正文收尾的统一四元组（流式/非流式共用）：

    (日志描述, 诊断detail, 客户端提示文案, finish_reason)。
    content_filter 判定与提示文案在两条交付路径上必须一字不差。
    """
    detail = _no_body_detail(finish_raw, blocked_msg)
    desc, hint = _no_body_desc_hint(has_thought, strip_thoughts)
    filtered = bool(blocked_msg) or ((finish_raw or "").upper() in FINISH_CONTENT_FILTER)
    finish = "content_filter" if filtered else "stop"
    notice = f"[Studio 提示] 上游{desc}（{detail}）。{hint}"
    return desc, detail, notice, finish


class _RequestBodyCache:
    """batchGraphql 请求体的「原生 / 降级」两形态缓存（流式与非流式共用）。

    构建内部含远程图片下载 + PIL 压缩，旧实现每次重试都重建 = 反复下载
    同一批图片；重试发同一个 body 本就是重试语义。降级形态惰性构建：
    原生函数 Schema 首次被拒才建，同时把降级模式锁定到后续重试。
    """

    def __init__(self, project_id, base_model_name, request_obj, *,
                 prefill_active: bool, force_search: bool):
        self._args = (project_id, base_model_name, request_obj)
        self._prefill_active = prefill_active
        self._force_search = force_search
        self._cache: Dict[str, dict] = {}
        self.native_latched = False

    async def get(self, native: bool) -> dict:
        key = "native" if native else "degraded"
        if key not in self._cache:
            self._cache[key] = await build_request_body_async(
                *self._args, prefill_active=self._prefill_active,
                force_search=self._force_search, native_tools=native)
        return self._cache[key]

    async def degraded(self) -> dict:
        """取降级 body，并把本次及后续重试锁定为降级模式。"""
        self.native_latched = True
        return await self.get(False)


def _safety_html_if_enabled(ratings: Any) -> str:
    """开关打开时把 batchGraphql 的 safetyRatings 渲染成附加块，否则空串。"""
    if not ratings:
        return ""
    if not app_state.get_setting("safety_score", app_config.SAFETY_SCORE):
        return ""
    return _create_safety_ratings_html(ratings)


def _is_retryable_error(error_msg: str) -> bool:
    """判断错误是否可重试（429 限流类）；与 Express 通道共用 retry_policy 的单一口径。"""
    return is_retryable_message(error_msg)


def _is_cookie_expired_error(error_msg: str) -> bool:
    """判断是否为 Cookie 过期/权限失效错误"""
    lower = error_msg.lower()
    return any(kw in lower for kw in COOKIE_EXPIRED_KEYWORDS)


def _native_tool_error(error_msg: str) -> bool:
    """Narrow detector for schema/model rejection eligible for one safe degrade."""
    lower = (error_msg or "").lower()
    tool_terms = ("functiondeclaration", "function declaration", "functioncallingconfig",
                  "function calling", "toolconfig", "tool config", "variables.tools")
    reject_terms = ("unsupported", "not supported", "unknown field", "unrecognized",
                    "invalid argument", "invalid value", "cannot use", "not enabled")
    return any(term in lower for term in tool_terms) and any(term in lower for term in reject_terms)


def _native_tool_error_response(status_code: int, payload: str) -> bool:
    """Avoid mistaking ordinary model text about tools for a protocol error."""
    if status_code != 200:
        return _native_tool_error(payload)
    has_error_envelope = bool(re.search(r'"errors?"\s*:', payload or ""))
    return has_error_envelope and _native_tool_error(payload)


def _api_error_details(data) -> tuple[int, str]:
    raw = data.get("message") if isinstance(data, dict) else data
    message = raw if isinstance(raw, str) and raw.strip() else str(data)
    code = data.get("code") if isinstance(data, dict) else None
    if not isinstance(code, int) or not 400 <= code <= 599:
        code = 429 if _is_retryable_error(message) else 502
    return code, message


async def _sleep_with_heartbeat(total_sec: float, hb_interval: float = 3.0, request=None):
    """边等待边吐心跳的异步生成器：每 hb_interval 秒 yield 一次心跳，直到累计 total_sec。"""
    waited = 0.0
    step = max(0.5, min(hb_interval, total_sec)) if total_sec > 0 else 0
    while waited < total_sec:
        if not await wait_for_retry(request, min(step, total_sec - waited)):
            return
        waited += step
        yield HEARTBEAT_FRAME


def _get_cookie_string() -> str:
    return app_state.get_google_cookie() or ""


def _get_project_id() -> str:
    return app_state.get_project_id() or ""


class _StreamRequestError(str):
    """保持错误字符串兼容性，并把 HTTP 退避信息带到流式重试层。"""
    def __new__(cls, message, retry_after=None, status_code=None):
        value = super().__new__(cls, message)
        value.retry_after = retry_after
        value.status_code = status_code
        return value


async def _execute_stream_request_generator_once(
    client: Any,
    headers: dict,
    body: dict,
    sampler: "RawSampler | None" = None,
) -> AsyncGenerator[tuple[str, Any], None]:
    """
    异步生成器：真正实时地拉取数据，抛出**原始事件**（不做 SSE 格式化）：
    ("text"|"thought"|"image", str)、("finish", 原始finishReason)、("usage", dict)、
    ("blocked", str)、("api_error_text", str)、("cookie_error"|"retryable_error"|"fatal_error", str)。
    由消费方负责格式化成 OpenAI chunk（便于预填充去重与无正文诊断）。
    """
    has_content = False
    got_finish = False
    try:
        upstream_attempt()
        async with client.stream("POST", BATCH_GRAPHQL_URL, headers=headers, json=body) as response:

            # 1. 拦截 HTTP 状态码错误
            if response.status_code != 200:
                error_text = await response.aread()
                error_msg = error_text.decode('utf-8', errors='replace')[:1000]

                # 原生函数 Schema 被拒优先于 Cookie/项目分类（与事件流分支
                # 的判定顺序一致；旧实现反序，且 "invalid argument" 被归进
                # 项目关键词，导致可降级的协议拒绝永远走不到降级重试）。
                if _native_tool_error(error_msg):
                    yield "native_tool_unsupported", _StreamRequestError(error_msg, status_code=response.status_code)
                    return

                if response.status_code in (401, 403) or _is_cookie_expired_error(error_msg) \
                        or _is_project_error(error_msg):
                    hint = PROJECT_ERROR_HINT if _is_project_error(error_msg) else COOKIE_REFRESH_HINT
                    yield "cookie_error", _StreamRequestError(error_msg + hint, status_code=response.status_code)
                    return
                is_retryable = is_retryable_status(response.status_code)
                error = _StreamRequestError(error_msg, getattr(response, "headers", {}).get("retry-after"),
                                            response.status_code)
                yield "retryable_error" if is_retryable else "fatal_error", error
                return

            # 2. 实时遍历并抛出流式 JSON 事件块
            async for obj in iter_json_objects(response):
                upstream_chunk()
                if sampler is not None:
                    sampler.add(obj)
                for event_type, data in extract_events(obj):
                    if event_type in ("text", "thought", "image", "function_call", "thought_signature"):
                        has_content = True
                        yield event_type, data

                    elif event_type in ("finish", "usage", "blocked", "safety"):
                        if event_type == "finish" and has_finish_reason(data):
                            got_finish = True
                        yield event_type, data

                    elif event_type == "error":
                        code, err_msg = _api_error_details(data)
                        error = _StreamRequestError(err_msg, status_code=code)
                        explicit_code = data.get("code") if isinstance(data, dict) else None

                        # 在还没发送任何有效数据前遇到错误，尝试走顶层重试
                        if _native_tool_error(err_msg) and not has_content:
                            yield "native_tool_unsupported", error
                            return
                        if (code in (401, 403) or _is_cookie_expired_error(err_msg) or _is_project_error(err_msg)) \
                                and not has_content:
                            hint = PROJECT_ERROR_HINT if _is_project_error(err_msg) else COOKIE_REFRESH_HINT
                            yield "cookie_error", _StreamRequestError(err_msg + hint, status_code=code)
                            return
                        # Numeric HTTP status wins over message keywords. The
                        # consumer knows whether thoughts were actually exposed
                        # and decides whether replay is still safe.
                        retryable = (is_retryable_status(code) if isinstance(explicit_code, int)
                                     and 400 <= explicit_code <= 599 else _is_retryable_error(err_msg))
                        yield "retryable_error" if retryable else "fatal_error", error
                        return

            if not got_finish:
                yield "fatal_error", _StreamRequestError(
                    "上游连接已结束，但回复缺少结束标记，内容可能不完整。" if has_content else
                    "上游连接已结束，但未收到模型回复或结束标记。", status_code=502)

    except Exception as e:
        err_msg = str(e)
        is_retryable = is_retryable_exception(e)
        code, _ = extract_upstream_error(e)
        yield "retryable_error" if is_retryable else "fatal_error", _StreamRequestError(err_msg, status_code=code)


async def _execute_stream_request_generator_raw(
    client: Any,
    headers: dict,
    body: dict,
    sampler: "RawSampler | None" = None,
    fallback_body: Any = None,
) -> AsyncGenerator[tuple[str, Any], None]:
    """Execute native tools, lazily degrading after one clear protocol rejection."""
    events = _execute_stream_request_generator_once(client, headers, body, sampler)
    try:
        async for status, data in events:
            if status != "native_tool_unsupported":
                yield status, data
                continue
            if fallback_body is None:
                yield "fatal_error", data
                return
            break
        else:
            return
    finally:
        await events.aclose()

    # 先归还被拒请求的连接，再构造/启动替代请求。
    # （降级锁定由 fallback_body 闭包内的 native_fallback_state["latched"]=True
    # 完成；旧签名的 fallback_state 参数无任何调用方传入，已移除。）
    resolved_fallback = fallback_body() if callable(fallback_body) else fallback_body
    if hasattr(resolved_fallback, "__await__"):
        resolved_fallback = await resolved_fallback
    print("⚠️ [Studio] 当前模型/协议拒绝原生函数 Schema；本次及后续重试固定降级为文本工具观测。")
    events = _execute_stream_request_generator_once(client, headers, resolved_fallback, sampler)
    try:
        async for fallback_status, fallback_data in events:
            yield fallback_status, fallback_data
    finally:
        await events.aclose()


async def _execute_stream_request_generator(client, headers, body, sampler=None, fallback_body=None, *, model=""):
    chain = getattr(client, "chain", None)
    # 首回复窗口每请求只读一次设置（while 循环里换出口/重试会反复新建 Guard）。
    fr_seconds = timeout_seconds(model)
    # Guard 在循环外创建一次：initial_deadline 记录原始预算终点。旧实现每轮
    # 换出口都新建 Guard（initial_deadline 重置为满窗口），流式路径的首回复
    # 总预算不变量失效——N 个候选出口 × 满窗口（默认 3×15s=45s）才轮到外层
    # 重试，与非流式 timed_call+proxy_attempt 的钳制语义不一致（2026-10 修复）。
    guard = FirstReplyGuard(model, seconds=fr_seconds)
    while True:
        # 每轮把 deadline 钳到「满窗口 与 原始预算剩余」的较小值（首轮=满窗口），
        # 与 proxy_attempt 对同一 Guard 的重写公式逐字一致；guard.ready 在换出口
        # 前提下恒为 False（switch 分支要求 not guard.ready），复用无状态残留。
        # seconds=0（禁用超时）时 wait 直接放行，deadline 不参与计时。
        if guard.seconds:
            window = max(0.001, min(guard.seconds, guard.initial_deadline - time.monotonic()))
            guard.deadline = time.monotonic() + window
            if guard.trace:
                with guard.trace._lock:
                    guard.trace.data["first_reply_deadline"] = time.time() + window
        source = _execute_stream_request_generator_raw(client, headers, body, sampler, fallback_body)
        pending = PendingMetadataGate()
        switch_proxy = False
        try:
            async for item in guard.iterate(source, lambda event: event[0] in ("text", "thought", "image", "function_call") and bool(event[1])):
                status, data = item
                if status in ("retryable_error", "fatal_error", "cookie_error", "blocked"):
                    if not guard.ready and chain is not None and status in ("retryable_error", "fatal_error"):
                        switch_proxy = chain.advance(status=getattr(data, "status_code", None))
                        if switch_proxy:
                            break
                    # 错误前已缓冲的元数据（finish/usage/safety）先冲刷再报错，
                    # 与正常收尾路径一致，不静默丢弃上游给出的诊断线索。
                    for prefix in pending.drain():
                        yield prefix
                    yield item
                    return
                if not guard.ready:
                    pending.add(item, len(str(data).encode("utf-8")))
                    continue
                for prefix in pending.drain():
                    yield prefix
                yield item
            if not switch_proxy:
                for prefix in pending.drain():
                    yield prefix
                return
        except FirstReplyTimeout as exc:
            switch_proxy = not guard.ready and chain is not None and chain.advance(exc)
            if not switch_proxy:
                yield "retryable_error", _StreamRequestError(str(exc), status_code=504)
                return
        finally:
            await source.aclose()


async def _collect_full_response(project_id, base_model_name, request_obj, headers, client_kwargs,
                                 retry_max, backoff_sec, jitter_sec, fastapi_request, *,
                                 prefill_active=False, force_search=False) -> dict:
    """非流式地完整取回一次响应（供生图假流式复用）。返回 dict。"""
    # 请求体只构建一次：构建内部含远程图片下载 + PIL 压缩，旧实现每次
    # 重试都重建一遍 = 反复下载同一批图片。重试发同一个 body 本就是重试语义。
    if await fastapi_request.is_disconnected():
        return {"kind": "error", "message": "客户端已断开连接。"}
    body = await build_request_body_async(project_id, base_model_name, request_obj,
                                          prefill_active=prefill_active, force_search=force_search)
    fr_seconds = timeout_seconds(base_model_name)
    for attempt in range(retry_max + 1):
        if await fastapi_request.is_disconnected():
            return {"kind": "error", "message": "客户端已断开连接。"}
        if attempt:
            stats.add_retry()
        try:
            req_headers = build_headers(headers.get("cookie", "")) or headers
            async with open_upstream_client(client_kwargs) as client:
                upstream_attempt()
                # 整取响应（生图假流式路径）：首回复窗口按整调用预算放宽，
                # 否则生图 30-120s 会在默认 15s 窗口下永远超时。
                response = await timed_call(base_model_name, client.post(BATCH_GRAPHQL_URL, headers=req_headers, json=body),
                                            whole_call=True, seconds=fr_seconds)
                upstream_chunk()

            if is_retryable_status(response.status_code) and attempt < retry_max:
                delay = retry_delay(getattr(response, "headers", {}).get("retry-after"), backoff_sec, attempt, jitter_sec)
                print(f"⚠️ [Studio] HTTP {response.status_code}（生图，尝试 {attempt+1}），{delay:.1f}s 后重试...")
                if not await wait_for_retry(fastapi_request, delay):
                    return {"kind": "error", "message": "客户端已断开连接。"}
                continue
            if response.status_code != 200:
                return {"kind": "error", "code": response.status_code,
                        "message": f"HTTP {response.status_code}: {response.text[:300]}"}

            full_text, reasoning_text, api_error, usage_meta = "", "", None, None
            api_error_status = 502
            finish_raw, blocked_msg = None, None
            safety_html = ""
            sampler = RawSampler()

            async for obj in iter_json_objects(TextSource(response.text)):
                sampler.add(obj)
                # 一旦拿到错误事件就停止解析剩余响应：生图响应可达数 MB base64，
                # 错误分支的返回值不依赖后续对象，继续拼接是纯浪费。
                if api_error:
                    break
                for et, data in extract_events(obj):
                    if et == "text":
                        full_text += data
                    elif et == "thought":
                        reasoning_text += data
                    elif et == "image":
                        full_text += data
                    elif et == "finish":
                        finish_raw = data
                    elif et == "safety":
                        safety_html = _safety_html_if_enabled(data)
                    elif et == "blocked":
                        blocked_msg = data
                    elif et == "usage":
                        usage_meta = data
                    elif et == "error":
                        api_error_status, api_error = _api_error_details(data)
                        break

            if api_error and is_retryable_status(api_error_status) and attempt < retry_max:
                delay = retry_delay(None, backoff_sec, attempt, jitter_sec)
                print(f"⚠️ [Studio] 生图 429/限流（尝试 {attempt+1}），{delay:.1f}s 后重试...")
                if not await wait_for_retry(fastapi_request, delay):
                    return {"kind": "error", "message": "客户端已断开连接。"}
                continue
            if api_error:
                return {"kind": "error", "code": api_error_status, "message": api_error}

            if not full_text.strip():
                # 无正文：给出可见的诊断信息，而不是静默空回复
                detail = _no_body_detail(finish_raw, blocked_msg)
                print(f"🔎 [Studio 诊断] 生图无正文（{detail}）。原始响应样本：\n{sampler.dump()}")
                return {"kind": "error", "message": f"上游未返回图片/正文（{detail}）。已在日志记录原始响应样本。"}

            require_finish_reason(finish_raw)
            return {"kind": "ok", "full_text": full_text + safety_html, "reasoning_text": reasoning_text,
                    "finish_reason": map_finish_reason_to_openai(finish_raw), "usage_meta": usage_meta}
        except Exception as e:
            em = str(e)
            if is_retryable_exception(e) and attempt < retry_max:
                delay = retry_delay(None, backoff_sec, attempt, jitter_sec)
                print(f"⚠️ [Studio] 生图异常（尝试 {attempt+1}）：{em[:80]}，{delay:.1f}s 后重试...")
                if not await wait_for_retry(fastapi_request, delay):
                    return {"kind": "error", "message": "客户端已断开连接。"}
                continue
            code, _ = extract_upstream_error(e)
            return {"kind": "error", "code": code, "message": f"batchGraphql proxy error: {em}"}
    return {"kind": "error", "message": "重试多次仍失败。"}


class CookieProxyUpstream(BaseUpstream):
    """
    batchGraphql 直连代理
    使用 Cookie + SAPISIDHASH 鉴权调用 batchGraphql 端点。
    """

    async def chat_completions(self, request_obj: OpenAIRequest, fastapi_request: Request, *, credential_record=None):
        track_configure(mode="stream" if request_obj.stream else "json", channel="cookie")
        try:
            validate_openai_request(request_obj)
            # 与 build_request_body 的门控同口径：开关关闭时快速失败（省去
            # Cookie 认证与请求体构造的网络准备）；开启时放行，由构造侧注入
            # responseMimeType/responseSchema。
            if (
                request_obj.response_format
                and request_obj.response_format.get("type") not in (None, "text")
                and not app_state.get_setting("cookie_structured_output", False)
            ):
                raise SchemaValidationError(COOKIE_STRUCTURED_OUTPUT_DISABLED_MESSAGE)
        except SchemaValidationError as exc:
            return JSONResponse(status_code=400, content={
                "error": {"message": str(exc), "type": "invalid_request_error", "code": 400}
            })

        # ===== 1. 验证认证 =====
        record = credential_record or credential_manager.get_cookie_record()
        cookie_str = record["cookie"] if record else _get_cookie_string()
        if not cookie_str:
            return JSONResponse(status_code=401, content={"error": {"message": (
                "未配置 Google Cookie。\n"
                "请在大盘控制台中粘贴 Cookie 和 Project ID，\n"
                "或设置环境变量 GOOGLE_COOKIE 和 GOOGLE_PROJECT_ID。"
            ), "type": "auth_error"}})

        project_id = record["project_id"] if record else _get_project_id()
        track_configure(project_id=project_id)
        if not project_id:
            return JSONResponse(status_code=400, content={"error": {"message": (
                "未配置 Google Cloud Project ID。\n"
                "请在大盘中填写，或设置环境变量 GOOGLE_PROJECT_ID。\n"
                "可从 Studio URL 中获取：...?project=YOUR_PROJECT_ID"
            ), "type": "config_error"}})

        # ===== 2. 构建请求头 =====
        headers = build_headers(cookie_str)
        if not headers:
            return JSONResponse(status_code=401, content={"error": {"message": (
                "Cookie 中未找到 SAPISID，无法计算认证头。\n"
                "请确保 Cookie 来自已登录的 console.cloud.google.com 页面。"
            ), "type": "auth_error"}})

        # ===== 2.5 工具流量处理：原生函数调用 =====
        _tool_info = classify_tool_traffic(request_obj.messages, request_obj.tools)
        force_builtin_search = bool(_tool_info["builtin_search"])
        _choice_name = normalized_tool_name(request_obj.tool_choice) if isinstance(request_obj.tool_choice, dict) else ""
        _forced_search = bool(_choice_name and is_builtin_search_name(_choice_name))
        native_custom_tools = bool(_tool_info["custom_names"] or _tool_info["history"])
        if force_builtin_search and _tool_info["custom_names"]:
            if _forced_search:
                print("🔎 [Studio] batchGraphql 不支持内建搜索与自定义函数混用；本轮按强制选择仅启用 googleSearch。")
            else:
                print("⚠️ [Studio] batchGraphql 不支持内建搜索与自定义函数混用；本轮优先启用自定义函数，忽略搜索声明。")
        elif force_builtin_search:
            print("🔎 [Studio] 请求声明了搜索类工具，已映射为 Studio 内建 googleSearch。")
        if _tool_info["custom_names"] and not _forced_search:
            print(f"🛠️ [Studio] 原生函数调用：{'、'.join(_tool_info['custom_names'][:5])}"
                  f"{' 等' if len(_tool_info['custom_names']) > 5 else ''}。")

        # ===== 3. 解析模型名 =====
        model_display = request_obj.model
        base_model_name, _ = split_model_alias(model_display)
        is_grounded_search = base_model_name.endswith("-search")
        if is_grounded_search:
            base_model_name = base_model_name[:-len("-search")]

        _profile = mc.get_profile(base_model_name)
        _inj_settings = app_state.get_effective_settings(base_model_name)
        request_obj, prefill_text, prefill_active = prepare_messages(
            request_obj, _profile, _inj_settings,
            has_tools=bool(_tool_info["declared"] or _tool_info["history"]))

        request_obj, text_transport = prepare_transport(
            request_obj, base_model_name, _inj_settings,
            search=force_builtin_search or is_grounded_search or _forced_search,
            prefill=prefill_active or bool(prefill_text))

        # ===== 4. HTTP 客户端配置 =====
        # One account snapshot binds Cookie, project and proxy chain for this
        # generation. The transport tries dedicated candidates before globals.
        client_kwargs = {
            "timeout": httpx.Timeout(connect=30.0, read=180.0, write=30.0, pool=10.0),
            "follow_redirects": False,
            "trust_env": False,
        }
        if app_config.SSL_CERT_FILE:
            client_kwargs["verify"] = app_config.SSL_CERT_FILE
        client_kwargs["_proxy_chain"] = ProxyChain(record).bind_to_generation()

        # 重试配置（控制台可调）；语义与 Express 通道统一：总尝试次数 = retry_max + 1
        # 间隔为固定值 + 抖动（backoff ± jitter/2，不随次数放大）
        retry_max, backoff_sec, jitter_sec = get_retry_settings()

        buffer_text = not _profile["is_image"] and (
            (text_transport and text_transport.mode == "nonstream")
            or app_state.get_setting("fake_streaming", app_config.FAKE_STREAMING_ENABLED))
        is_stream = request_obj.stream and not buffer_text
        response_id = f"chatcmpl-studio-{int(time.time())}-{uuid.uuid4().hex[:8]}"
        start_time = time.time()
        want_usage = wants_usage(request_obj)
        cookie_debug = bool(app_state.get_setting("debug_outbound", False)
                            or app_state.get_setting("cookie_debug", False))

        # batchGraphql(Studio) 会忽略 includeThoughts=false（真机验证），因此当解析出的思考配置
        # 要求不回传思考时，由本通道在响应侧主动剥离思考块，生图同样适用。
        # 复用 3. 段已取的 _inj_settings：同参数同模型，两次 get_effective_settings
        # 之间无任何设置写入点，旧实现白做一次全量深拷贝+合并（热路径，2026-10 优化）。
        _eff_settings = _inj_settings
        _tk = mc.resolve_thinking(base_model_name, request_obj, _eff_settings, prefill_active=prefill_active)
        strip_thoughts = bool(_tk.get("mode") and not _tk.get("include_thoughts", True))
        if strip_thoughts and cookie_debug:
            print("🔎 [Studio 调试] 已启用响应侧思考剥离（batchGraphql 不支持 includeThoughts=false）。")

        # 打印请求日志
        msg_count = len(request_obj.messages)
        print(f"→ [Studio] {base_model_name} | {msg_count} 条消息 | {'流式' if is_stream else '非流式'}")

        is_image = _profile["is_image"]
        track_configure(mode="fake_stream" if request_obj.stream and (buffer_text or is_image) else
                        "stream" if request_obj.stream else "json", thinking_level=_tk.get("level"))

        # ========== 生图 + 流式：强制假流式 ==========
        # 生图输出是超大 base64，若按流式分块传输会卡死前端解析器；
        # 因此先完整取回，再把整张图作为“单个 chunk”一次性发出（与官方 SDK 通道一致）。
        if is_stream and is_image:
            async def image_fake_stream():
                if await fastapi_request.is_disconnected():
                    return
                print(f"🖼️ [生图保护] 图片模型 {base_model_name} 已自动切换为假流式输出（Cookie 通道），避免分块 base64 卡死前端。")
                # 收集期间持续吐心跳：生图（含重试等待，固定间隔+抖动）可能持续
                # 数十秒到数分钟；旧实现首个字节在收集完成之后才发，SSE 连接
                # 在整个生成期间零字节，前端/中间层会按空闲超时掐断（与
                # README 声明的心跳覆盖范围不符，也与 stream_generator /
                # buffered_stream 的首心跳行为不一致）。
                yield HEARTBEAT_FRAME
                collect_task = asyncio.create_task(_collect_full_response(
                    project_id, base_model_name, request_obj, headers, client_kwargs,
                    retry_max, backoff_sec, jitter_sec, fastapi_request,
                    prefill_active=prefill_active, force_search=force_builtin_search,
                ))
                try:
                    while True:
                        done, _pending = await asyncio.wait({collect_task}, timeout=10.0)
                        if done:
                            break
                        if await fastapi_request.is_disconnected():
                            print("ℹ️ [Studio] 客户端已断开连接，取消生图收集。")
                            return
                        yield HEARTBEAT_FRAME
                    try:
                        res = collect_task.result()
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        res = {"kind": "error", "code": 502,
                               "message": f"batchGraphql proxy error: {type(e).__name__}: {e}"}
                    yield make_openai_chunk(response_id, model_display, role="assistant")
                    if res.get("kind") != "ok":
                        stats.add_error()
                        print(f"❌ [Studio] 生图失败 | {res.get('message', '')[:150]}")
                        error = create_openai_error_response(
                            res.get("code", 502), res.get("message", "生图失败"), "upstream_error")
                        yield "data: " + json.dumps(error, ensure_ascii=False) + "\n\n"
                        yield "data: [DONE]\n\n"
                        return
                    full_text = res.get("full_text") or ""
                    if prefill_text:
                        full_text = prefill_text + strip_prefill_overlap(prefill_text, full_text)
                    if res.get("reasoning_text") and not strip_thoughts:
                        yield make_openai_chunk(response_id, model_display, reasoning_content=res["reasoning_text"])
                    # 关键：整张图作为单个 chunk 发出，绝不分块
                    yield make_openai_chunk(response_id, model_display, content=full_text or " ")
                    stats.add_success()
                    usage = map_usage(res.get("usage_meta"))
                    if want_usage:
                        yield make_usage_chunk(response_id, model_display, usage)
                    yield make_openai_chunk(response_id, model_display, finish_reason=res.get("finish_reason", "stop"))
                    yield "data: [DONE]\n\n"
                    print(f"✅ [Studio] {base_model_name} | 生图假流式完成")
                finally:
                    # 生成器被提前关闭（断连/停机/缓冲淘汰都会 aclose 本生成器，
                    # GeneratorExit 打进 asyncio.wait 的挂起点）时必须收尸：
                    # 孤儿任务会带着完整重试循环（retry_max × 退避）继续打上游，
                    # 浪费配额且此时并发名额已归还，完全绕过 upstream_max_concurrency。
                    if not collect_task.done():
                        collect_task.cancel()
                    await asyncio.gather(collect_task, return_exceptions=True)
            return StreamingResponse(image_fake_stream(), media_type="text/event-stream")

        # ========== 流式处理（彻底解决 60s 超时的真·流式机制） ==========
        if is_stream:
            async def _stream_core():
                nonlocal start_time

                body_cache = _RequestBodyCache(
                    project_id, base_model_name, request_obj,
                    prefill_active=prefill_active, force_search=force_builtin_search)

                for attempt in range(retry_max + 1):
                    # 客户端已断开则立即停止，避免无谓的上游调用与重试
                    if await fastapi_request.is_disconnected():
                        print("ℹ️ [Studio] 客户端已断开连接，停止流式重试。")
                        return
                    if attempt:
                        stats.add_retry()

                    use_native_tools = not body_cache.native_latched
                    body = await body_cache.get(use_native_tools)
                    # 降级 body 惰性构建（原生 Schema 首次被拒才建）；
                    # degraded() 会把降级模式锁定到后续重试。
                    fallback_body = (body_cache.degraded
                                     if native_custom_tools and use_native_tools and not text_transport
                                     else None)
                    req_headers = build_headers(cookie_str) or headers
                    if cookie_debug:
                        print(f"🔎 [Studio 调试] 出站 generationConfig: "
                              f"{json.dumps(body['variables'].get('generationConfig', {}), ensure_ascii=False)}")

                    async with open_upstream_client(client_kwargs) as client:
                        has_yielded_to_client = False
                        got_text = False       # 是否收到过“正文”（text/image）
                        got_thought = False    # 是否收到过思考
                        got_tool_call = False
                        tool_call_index = 0
                        stream_ordinary_metadata = []
                        stream_part_order = []
                        should_retry = False
                        error_to_raise = None
                        usage_meta = None
                        finish_raw = None
                        blocked_msg = None
                        sampler = RawSampler()
                        deduper = PrefillDeduper(prefill_text)
                        pending_safety_html = ""

                        # 消费实时生成器（原始事件 → 此处格式化为 OpenAI chunk）
                        events = _execute_stream_request_generator(
                            client, req_headers, body, sampler=sampler,
                            fallback_body=fallback_body, model=base_model_name,
                        )
                        if text_transport:
                            events = text_transport.decoder().cookie_stream(events)
                        try:
                            async for status, data in events:
                                if status == "transport_error":
                                    stats.add_error()
                                    print(f"❌ [抗截断] {data}")
                                    yield "data: " + json.dumps({"error": {
                                        "code": 502, "message": data, "type": "upstream_error"}}, ensure_ascii=False) + "\n\n"
                                    yield "data: [DONE]\n\n"
                                    return
                                # 思考剥离：batchGraphql 忽略 includeThoughts，这里按解析出的配置主动丢弃思考块
                                if status == "thought":
                                    got_thought = True
                                    if strip_thoughts:
                                        continue  # 不建连、不输出，等真正的正文

                                if status == "function_call":
                                    if not has_yielded_to_client:
                                        print(f"⚡ [Studio] {base_model_name} | 连接建立，正在实时流式输出...")
                                        yield make_openai_chunk(response_id, model_display, role="assistant")
                                        has_yielded_to_client = True
                                    tool_payload = openai_tool_call(response_id, tool_call_index, data)
                                    tool_payload["index"] = tool_call_index
                                    stream_part_order.append({"type": "tool_call", "index": tool_call_index})
                                    yield make_openai_chunk(response_id, model_display, tool_calls=[tool_payload])
                                    tool_call_index += 1
                                    got_tool_call = True

                                elif status == "thought_signature":
                                    signature = apply_wire_signature(
                                        data, stream_ordinary_metadata, stream_part_order)
                                    extra = thought_signature_extra(signature, data.get("part_kind"))
                                    if extra:
                                        google = extra.setdefault("google", {})
                                        if stream_ordinary_metadata:
                                            google["ordinary_parts"] = list(stream_ordinary_metadata)
                                        if got_tool_call:
                                            google["part_order"] = list(stream_part_order)
                                        if not has_yielded_to_client:
                                            yield make_openai_chunk(response_id, model_display, role="assistant")
                                            has_yielded_to_client = True
                                        yield make_openai_chunk(response_id, model_display, extra_content=extra)

                                elif status in ("text", "thought", "image", "api_error_text"):
                                    if not has_yielded_to_client:
                                        print(f"⚡ [Studio] {base_model_name} | 连接建立，正在实时流式输出...")
                                        yield make_openai_chunk(response_id, model_display, role="assistant")
                                        has_yielded_to_client = True
                                        # 预填充智能兼容：先把预填充文本作为回复开头发出
                                        if prefill_text:
                                            yield make_openai_chunk(response_id, model_display, content=prefill_text)

                                    if status == "thought":
                                        stream_part_order.append({"type": "ordinary", "index": len(stream_ordinary_metadata)})
                                        stream_ordinary_metadata.append(ordinary_part_metadata("thought", data, None))
                                        yield make_openai_chunk(response_id, model_display, reasoning_content=data)
                                    elif status == "api_error_text":
                                        stats.add_error()
                                        yield make_openai_chunk(response_id, model_display, content=f"\n[Studio API 错误] {data}")
                                        yield "data: " + json.dumps({"error": {
                                            "code": 502, "message": data, "type": "upstream_error"}}, ensure_ascii=False) + "\n\n"
                                        yield "data: [DONE]\n\n"
                                        return
                                    elif status == "image":
                                        got_text = True
                                        stream_part_order.append({"type": "ordinary", "index": len(stream_ordinary_metadata)})
                                        stream_ordinary_metadata.append(ordinary_part_metadata("text", data, None))
                                        yield make_openai_chunk(response_id, model_display, content=data)
                                    else:  # text（经过预填充去重器）
                                        got_text = True
                                        out = deduper.feed(data)
                                        if out:
                                            stream_part_order.append({"type": "ordinary", "index": len(stream_ordinary_metadata)})
                                            stream_ordinary_metadata.append(ordinary_part_metadata("text", out, None))
                                            yield make_openai_chunk(response_id, model_display, content=out)

                                elif status == "finish":
                                    finish_raw = data
                                elif status == "safety":
                                    # 安全分与 finish 同块到达（最后一块）：先暂存，
                                    # 等去重器 flush 完尾段再输出。旧实现在循环内即时
                                    # yield，预填充 + 短回复场景下会插在去重器攒住的
                                    # 正文尾段**之前**，把安全块嵌进正文中间（2026-10 修复）。
                                    pending_safety_html = _safety_html_if_enabled(data)
                                elif status == "usage":
                                    usage_meta = data
                                elif status == "blocked":
                                    blocked_msg = data

                                elif status == "cookie_error":
                                    stats.add_error()
                                    code = getattr(data, "status_code", None) or 403
                                    error = create_openai_error_response(code, str(data)[:1024], "upstream_error")
                                    yield "data: " + json.dumps(error, ensure_ascii=False) + "\n\n"
                                    yield "data: [DONE]\n\n"
                                    return

                                elif status in ("retryable_error", "fatal_error"):
                                    if not has_yielded_to_client and status == "retryable_error" and attempt < retry_max:
                                        should_retry = True
                                        error_to_raise = data
                                        break
                                    stats.add_error()
                                    default_code = 429 if status == "retryable_error" else 502
                                    code = getattr(data, "status_code", None) or default_code
                                    error = create_openai_error_response(code, str(data)[:1024], "upstream_error")
                                    print(f"❌ [Studio] HTTP {code} | {str(data)[:150]}")
                                    yield "data: " + json.dumps(error, ensure_ascii=False) + "\n\n"
                                    yield "data: [DONE]\n\n"
                                    return
                        finally:
                            await events.aclose()

                        # 如果标记了需要重试，在当前 attempt 结束时等待并开启下一次循环
                        if should_retry:
                            # 重试前再次确认客户端仍在
                            if await fastapi_request.is_disconnected():
                                print("ℹ️ [Studio] 客户端已断开连接，取消后续重试。")
                                return
                            wait_sec = retry_delay(getattr(error_to_raise, "retry_after", None), backoff_sec, attempt, jitter_sec)
                            print(f"⚠️ [Studio] 遇到可重试拥堵/限流: {error_to_raise[:80]}... {wait_sec}s 后进行第 {attempt+2} 次退避重试")
                            # 退避等待期间持续吐心跳，保活前端连接（issue4：3.1-pro 频繁 429 长等待易被前端断开）
                            async for _hb in _sleep_with_heartbeat(wait_sec, request=fastapi_request):
                                if await fastapi_request.is_disconnected():
                                    print("ℹ️ [Studio] 客户端已断开连接，取消后续重试。")
                                    return
                                yield _hb
                            start_time = time.time()
                            continue

                        # ===== 正常收尾（修复旧版“空响应静默关流”，并显式暴露“只有思考没有正文”）=====
                        if not has_yielded_to_client:
                            yield make_openai_chunk(response_id, model_display, role="assistant")
                            has_yielded_to_client = True
                            if prefill_text:
                                yield make_openai_chunk(response_id, model_display, content=prefill_text)

                        # 预填充去重器里可能还攒着开头的一小段（短回复场景）
                        tail = deduper.flush()
                        if tail:
                            stream_part_order.append({"type": "ordinary", "index": len(stream_ordinary_metadata)})
                            stream_ordinary_metadata.append(ordinary_part_metadata("text", tail, None))
                            yield make_openai_chunk(response_id, model_display, content=tail)

                        # 安全分在正文（含去重尾段）之后、结束帧之前输出，
                        # 与非流式路径 content_value = full_text + safety_html 的顺序一致。
                        if pending_safety_html:
                            yield make_openai_chunk(response_id, model_display, content=pending_safety_html)

                        if got_text or got_tool_call:
                            stats.add_success()
                            if got_tool_call and stream_ordinary_metadata:
                                google = {
                                    "ordinary_parts": list(stream_ordinary_metadata),
                                    "part_order": list(stream_part_order),
                                }
                                last_signed = next((item for item in reversed(stream_ordinary_metadata)
                                                    if item.get("thought_signature")), None)
                                if last_signed:
                                    google["thought_signature"] = last_signed["thought_signature"]
                                    google["thought_signature_part"] = last_signed["kind"]
                                yield make_openai_chunk(
                                    response_id, model_display, extra_content={"google": google})
                            yield make_openai_chunk(
                                response_id, model_display,
                                finish_reason="tool_calls" if got_tool_call else map_finish_reason_to_openai(finish_raw))
                        else:
                            # 无正文：明确告知客户端 + 落诊断日志（旧版此处一个字节都不发就关流）
                            desc, detail, notice, no_body_finish = _no_body_outcome(
                                finish_raw, blocked_msg, got_thought, strip_thoughts)
                            stats.add_error()
                            print(f"❌ [Studio] {base_model_name} | 上游{desc}（{detail}）")
                            print(f"🔎 [Studio 诊断] 原始响应样本：\n{sampler.dump()}")
                            yield make_openai_chunk(
                                response_id, model_display,
                                content=f"\n{notice}")
                            yield make_openai_chunk(response_id, model_display,
                                                     finish_reason=no_body_finish)

                        usage = map_usage(usage_meta)
                        if want_usage:
                            yield make_usage_chunk(response_id, model_display, usage)

                        yield "data: [DONE]\n\n"

                        elapsed = time.time() - start_time
                        if got_text or got_tool_call:
                            print(f"✅ [Studio] {base_model_name} | 流式传输顺利完毕 | 耗时 {elapsed:.1f}s")
                        return

            async def stream_generator():
                # 立即吐一个心跳，尽快建立连接（避免首个上游调用较慢/429 时前端久等无字节）
                yield HEARTBEAT_FRAME
                try:
                    async for frame in _stream_core():
                        yield frame
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    # 准备阶段（请求体构建 / 客户端池获取）的异常旧实现直接抛出
                    # 生成器：SSE 已 200 建流后连接被拦腰掐断，客户端拿不到任何
                    # 错误帧只能干等到超时。这里统一以错误帧 + [DONE] 收尾，
                    # 与「半截答案不当成功、异常必须显式返回」的设计原则一致。
                    stats.add_error()
                    code, message = extract_upstream_error(e)
                    print(f"❌ [Studio] {base_model_name} | 流式响应意外中断 | HTTP {code} | {str(e)[:150]}")
                    error = create_openai_error_response(code, message[:1024], "upstream_error")
                    yield "data: " + json.dumps(error, ensure_ascii=False) + "\n\n"
                    yield "data: [DONE]\n\n"

            return StreamingResponse(stream_generator(), media_type="text/event-stream")

        # ========== 非流式处理 ==========
        else:
            async def nonstream_response():
                # 请求体两形态各自只构建一次（同流式路径：防重试反复下载图片）
                body_cache = _RequestBodyCache(
                    project_id, base_model_name, request_obj,
                    prefill_active=prefill_active, force_search=force_builtin_search)

                # 非流式整取：首回复窗口按整调用预算放宽（生图/长思考）。
                fr_seconds = timeout_seconds(base_model_name)
                for attempt in range(retry_max + 1):
                    # 客户端已断开则立即停止，避免无谓的上游调用与重试
                    if await fastapi_request.is_disconnected():
                        print("ℹ️ [Studio] 客户端已断开连接，停止非流式重试。")
                        return JSONResponse(status_code=499, content={
                            "error": {"message": "客户端已断开连接，请求已取消。", "type": "client_closed_request"}
                        })
                    if attempt:
                        stats.add_retry()
                    try:
                        use_native_tools = not body_cache.native_latched
                        body = await body_cache.get(use_native_tools)
                        # 降级 body 惰性构建（原生 Schema 首次被拒才建）；
                        # degraded() 会把降级模式锁定到后续重试。
                        fallback_body = (body_cache.degraded
                                         if native_custom_tools and use_native_tools and not text_transport
                                         else None)
                        req_headers = build_headers(cookie_str) or headers
                        if cookie_debug:
                            print(f"🔎 [Studio 调试] 出站 generationConfig: "
                                  f"{json.dumps(body['variables'].get('generationConfig', {}), ensure_ascii=False)}")

                        async with open_upstream_client(client_kwargs) as client:
                            upstream_attempt()
                            response = await timed_call(base_model_name, client.post(
                                BATCH_GRAPHQL_URL, headers=req_headers, json=body
                            ), whole_call=True, seconds=fr_seconds)
                            upstream_chunk()
                            if fallback_body is not None and _native_tool_error_response(
                                    response.status_code, response.text):
                                resolved_fallback = await fallback_body()  # degraded() 同时锁定后续重试
                                print("⚠️ [Studio] 当前模型/协议拒绝原生函数 Schema；本次及后续重试固定降级为文本工具观测。")
                                upstream_attempt()
                                response = await timed_call(base_model_name, client.post(
                                    BATCH_GRAPHQL_URL, headers=req_headers, json=resolved_fallback
                                ), whole_call=True, seconds=fr_seconds)
                                upstream_chunk()
                                fallback_body = None

                        if is_retryable_status(response.status_code):
                            if attempt < retry_max:
                                delay = retry_delay(getattr(response, "headers", {}).get("retry-after"), backoff_sec, attempt, jitter_sec)
                                print(f"⚠️ [Studio] HTTP {response.status_code} (尝试 {attempt+1}), {delay:.1f}s 后重试...")
                                if not await wait_for_retry(fastapi_request, delay):
                                    return JSONResponse(status_code=499, content={"error": {"message": "客户端已断开连接。"}})
                                continue

                        if response.status_code != 200:
                            elapsed = time.time() - start_time
                            print(f"❌ [Studio] {base_model_name} | HTTP {response.status_code} | {elapsed:.1f}s")
                            return JSONResponse(status_code=response.status_code, content={
                                "error": {"message": response.text[:500], "type": "upstream_error"}
                            })

                        full_text = ""
                        reasoning_text = ""
                        safety_html = ""
                        tool_calls = []
                        ordinary_metadata = []
                        part_order = []
                        message_extra = None
                        api_error = None
                        api_error_status = 502
                        usage_meta = None
                        finish_raw = None
                        blocked_msg = None
                        sampler = RawSampler()

                        fake_resp = TextSource(response.text)
                        text_decoder = text_transport.decoder() if text_transport else None
                        async for obj in iter_json_objects(fake_resp):
                            sampler.add(obj)
                            # error 即停：后续对象不再解析（生图响应可达数 MB，
                            # 白烧事件循环 CPU），且首个 error 不被后续 error 覆盖。
                            # 生图路径（_collect_full_response）同构修复已验证。
                            if api_error:
                                break
                            events = extract_events(obj)
                            if text_decoder:
                                events = text_decoder.cookie_events(events)
                            for event_type, data in events:
                                if event_type == "text":
                                    full_text += data
                                    part_order.append({"type": "ordinary", "index": len(ordinary_metadata)})
                                    ordinary_metadata.append(ordinary_part_metadata("text", data, None))
                                elif event_type == "thought":
                                    reasoning_text += data   # 先收集（供诊断判断），输出时按 strip 决定是否附带
                                    part_order.append({"type": "ordinary", "index": len(ordinary_metadata)})
                                    ordinary_metadata.append(ordinary_part_metadata("thought", data, None))
                                elif event_type == "image":
                                    full_text += data
                                    part_order.append({"type": "ordinary", "index": len(ordinary_metadata)})
                                    ordinary_metadata.append(ordinary_part_metadata("text", data, None))
                                elif event_type == "function_call":
                                    part_order.append({"type": "tool_call", "index": len(tool_calls)})
                                    tool_calls.append(openai_tool_call(response_id, len(tool_calls), data))
                                elif event_type == "thought_signature":
                                    sig = apply_wire_signature(data, ordinary_metadata, part_order)
                                    if sig:
                                        message_extra = thought_signature_extra(sig, data.get("part_kind"))
                                elif event_type == "finish":
                                    finish_raw = data
                                elif event_type == "safety":
                                    safety_html = _safety_html_if_enabled(data)
                                elif event_type == "blocked":
                                    blocked_msg = data
                                elif event_type == "usage":
                                    usage_meta = data
                                elif event_type == "error":
                                    api_error_status, api_error = _api_error_details(data)
                                    break

                        if api_error and is_retryable_status(api_error_status) and attempt < retry_max:
                            wait_sec = retry_delay(None, backoff_sec, attempt, jitter_sec)
                            print(f"⚠️ [Studio] 429/限流 (尝试 {attempt+1}): {api_error[:100]}, {wait_sec:.1f}s 后重试...")
                            if not await wait_for_retry(fastapi_request, wait_sec):
                                return JSONResponse(status_code=499, content={"error": {"message": "客户端已断开连接。"}})
                            continue
                        if api_error:
                            return JSONResponse(status_code=api_error_status, content={"error": {
                                "code": api_error_status, "message": api_error, "type": "upstream_error"}})

                        if text_decoder:
                            text_decoder.validate_end()

                        got_text = bool(full_text.strip())
                        got_output = got_text or bool(tool_calls)
                        elapsed = time.time() - start_time

                        if got_output:
                            require_finish_reason(finish_raw)
                            # 预填充智能兼容：去重后把预填充文本拼回输出开头
                            if prefill_text and got_text:
                                full_text = prefill_text + strip_prefill_overlap(prefill_text, full_text)
                            finish_reason = "tool_calls" if tool_calls else map_finish_reason_to_openai(finish_raw)
                            stats.add_success()
                            print(f"✅ [Studio] {base_model_name} | {len(full_text)} 字符 | {elapsed:.1f}s")
                        else:
                            # 无正文（只有思考 / 完全空）：返回明确诊断而不是一个空格
                            desc, detail, notice, finish_reason = _no_body_outcome(
                                finish_raw, blocked_msg, bool(reasoning_text), strip_thoughts)
                            stats.add_error()
                            print(f"❌ [Studio] {base_model_name} | 上游{desc}（{detail}）| {elapsed:.1f}s")
                            print(f"🔎 [Studio 诊断] 原始响应样本：\n{sampler.dump()}")
                            full_text = notice

                        usage = map_usage(usage_meta)

                        content_value = full_text + safety_html
                        message_obj = {"role": "assistant", "content": content_value if content_value else None}
                        if reasoning_text and not strip_thoughts:   # strip：batchGraphql 忽略 includeThoughts，输出侧剥离
                            message_obj["reasoning_content"] = reasoning_text
                        if tool_calls:
                            message_obj["tool_calls"] = tool_calls
                        if ordinary_metadata:
                            if message_extra is None:
                                message_extra = {"google": {}}
                            google = message_extra.setdefault("google", {})
                            google["ordinary_parts"] = ordinary_metadata
                            if tool_calls:
                                google["part_order"] = part_order
                        if message_extra:
                            message_obj["extra_content"] = message_extra

                        payload = {
                            "id": response_id,
                            "object": "chat.completion",
                            "created": int(time.time()),
                            "model": model_display,
                            "choices": [{
                                "index": 0,
                                "message": message_obj,
                                "finish_reason": finish_reason,
                            }],
                            "usage": usage,
                        }
                        return JSONResponse(content=payload)

                    except Exception as e:
                        err_msg = str(e)
                        is_retryable = is_retryable_exception(e)

                        if is_retryable and attempt < retry_max:
                            wait_sec = retry_delay(None, backoff_sec, attempt, jitter_sec)
                            print(f"⚠️ [Studio] 异常 (尝试 {attempt+1}): {err_msg[:100]}, {wait_sec:.1f}s 后重试...")
                            if not await wait_for_retry(fastapi_request, wait_sec):
                                return JSONResponse(status_code=499, content={"error": {"message": "客户端已断开连接。"}})
                            continue

                        elapsed = time.time() - start_time
                        print(f"❌ [Studio] {base_model_name} | 异常 | {elapsed:.1f}s: {err_msg[:150]}")
                        if not isinstance(e, (AntiTruncationError, IncompleteResponseError)):
                            traceback.print_exc()
                        code, _ = extract_upstream_error(e)
                        return JSONResponse(status_code=code, content={
                            "error": {"code": code, "message": f"batchGraphql proxy error: {err_msg}", "type": "proxy_error"}
                        })

                elapsed = time.time() - start_time
                print(f"❌ [Studio] {base_model_name} | 重试 {retry_max} 次后仍失败 | {elapsed:.1f}s")
                return JSONResponse(status_code=429, content={
                    "error": {"message": "请求被限流，已重试多次仍失败。请稍后再试。", "type": "rate_limit_error"}
                })

            if request_obj.stream:
                async def result_once():
                    yield await nonstream_response()

                async def buffered_stream():
                    events = with_heartbeats(result_once(), fastapi_request)
                    try:
                        yield HEARTBEAT_FRAME
                        async for result in events:
                            if result is HEARTBEAT:
                                yield HEARTBEAT_FRAME
                                continue
                            payload = json.loads(result.body)
                            if result.status_code >= 400:
                                stats.add_error()
                                error = payload.get("error")
                                if isinstance(error, dict):
                                    error.setdefault("code", result.status_code)
                                yield "data: " + json.dumps(payload, ensure_ascii=False) + "\n\n"
                                yield "data: [DONE]\n\n"
                                return
                            async for frame in buffered_sse(payload, include_usage=want_usage).body_iterator:
                                yield frame
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        stats.add_error()
                        code, message = extract_upstream_error(exc)
                        error = create_openai_error_response(code, message[:1024], "upstream_error")
                        yield "data: " + json.dumps(error, ensure_ascii=False) + "\n\n"
                        yield "data: [DONE]\n\n"
                    finally:
                        await events.aclose()

                return StreamingResponse(buffered_stream(), media_type="text/event-stream")
            return await nonstream_response()

"""Cookie HTTP event transport shared by OpenAI and Gemini adapters.

Owns upstream error classification, raw event reads, tool-protocol fallback and
first-reply/proxy handling. It does not construct downstream API responses.
"""

import re
import time
from typing import Any, AsyncGenerator

from cookie_auth import BATCH_GRAPHQL_URL
from first_reply import FirstReplyGuard, FirstReplyTimeout, timeout_seconds
from request_tracking import upstream_attempt, upstream_chunk
from response_validation import (
    PendingMetadataGate,
    has_finish_reason,
)
from retry_policy import (
    is_retryable_exception,
    is_retryable_message,
    is_retryable_status,
    wait_for_retry,
)
from streaming import HEARTBEAT_FRAME
from upstream_errors import extract_upstream_error
from upstreams.cookie_response import (
    RawSampler,
    extract_events,
    iter_json_objects,
)

COOKIE_EXPIRED_KEYWORDS = [
    # 短语级匹配：2026-10 收窄——旧的单词级 "denied"/"permission" 会把安全拒绝类
    # 文案（如 "request denied by safety ..."）误报成 Cookie 失效，误导用户刷新
    # 本来健康的 Cookie。真实验权失败优先由结构化状态码兜住（401/403 HTTP 或
    # gRPC 7/16 → api_error_details 映射），文本只是最后防线。
    "permission denied",
    "unauthenticated",
    "login required",
    "session expired",
    "invalid credentials",
    "not authorized",
    "aiplatform.endpoints.predict",
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


def is_project_error(error_msg: str) -> bool:
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


def is_retryable_error(error_msg: str) -> bool:
    """判断错误是否可重试（429 限流类）；与 Express 通道共用 retry_policy 的单一口径。"""
    return is_retryable_message(error_msg)


def is_cookie_expired_error(error_msg: str) -> bool:
    """判断是否为 Cookie 过期/权限失效错误"""
    lower = error_msg.lower()
    return any(kw in lower for kw in COOKIE_EXPIRED_KEYWORDS)


def is_native_tool_error(error_msg: str) -> bool:
    """Narrow detector for schema/model rejection eligible for one safe degrade."""
    lower = (error_msg or "").lower()
    tool_terms = (
        "functiondeclaration",
        "function declaration",
        "functioncallingconfig",
        "function calling",
        "toolconfig",
        "tool config",
        "variables.tools",
    )
    reject_terms = (
        "unsupported",
        "not supported",
        "unknown field",
        "unrecognized",
        "invalid argument",
        "invalid value",
        "cannot use",
        "not enabled",
    )
    return any(term in lower for term in tool_terms) and any(
        term in lower for term in reject_terms
    )


def is_native_tool_error_response(status_code: int, payload: str) -> bool:
    """Avoid mistaking ordinary model text about tools for a protocol error."""
    if status_code != 200:
        return is_native_tool_error(payload)
    has_error_envelope = bool(re.search(r'"errors?"\s*:', payload or ""))
    return has_error_envelope and is_native_tool_error(payload)


# gRPC 状态码 → HTTP：batchGraphql 的 GraphQL 错误块带 extensions.status.code
# （gRPC 码），结构化信号优先于文本关键词（8=RESOURCE_EXHAUSTED 实测回传）。
_GRPC_TO_HTTP = {
    2: 500,  # UNKNOWN
    3: 400,  # INVALID_ARGUMENT
    4: 504,  # DEADLINE_EXCEEDED
    5: 404,  # NOT_FOUND
    7: 403,  # PERMISSION_DENIED
    8: 429,  # RESOURCE_EXHAUSTED
    9: 400,  # FAILED_PRECONDITION
    13: 500,  # INTERNAL
    14: 503,  # UNAVAILABLE
    15: 500,  # DATA_LOSS
    16: 401,  # UNAUTHENTICATED
}


def grpc_status_code(data) -> int | None:
    """从 GraphQL 错误块提取 gRPC 状态码（extensions.status.code）。"""
    if not isinstance(data, dict):
        return None
    extensions = data.get("extensions")
    if not isinstance(extensions, dict):
        return None
    status = extensions.get("status")
    if not isinstance(status, dict):
        return None
    code = status.get("code")
    if isinstance(code, bool) or not isinstance(code, int):
        return None
    return code


def api_error_details(data) -> tuple[int, str]:
    raw = data.get("message") if isinstance(data, dict) else data
    message = raw if isinstance(raw, str) and raw.strip() else str(data)
    code = data.get("code") if isinstance(data, dict) else None
    if not isinstance(code, int) or not 400 <= code <= 599:
        # 结构化信号优先：gRPC 状态码（extensions.status.code）→ HTTP；
        # 都没有再落到文本关键词（旧口径，兜底）。
        code = _GRPC_TO_HTTP.get(grpc_status_code(data))
    if not isinstance(code, int) or not 400 <= code <= 599:
        code = 429 if is_retryable_error(message) else 502
    return code, message


async def retry_heartbeats(total_sec: float, hb_interval: float = 3.0, request=None):
    """边等待边吐心跳的异步生成器：每 hb_interval 秒 yield 一次心跳，直到累计 total_sec。"""
    waited = 0.0
    step = max(0.5, min(hb_interval, total_sec)) if total_sec > 0 else 0
    while waited < total_sec:
        if not await wait_for_retry(request, min(step, total_sec - waited)):
            return
        waited += step
        yield HEARTBEAT_FRAME


class StreamRequestError(str):
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
    *,
    preserve_parts: bool = False,
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
        async with client.stream(
            "POST", BATCH_GRAPHQL_URL, headers=headers, json=body
        ) as response:
            # 1. 拦截 HTTP 状态码错误
            if response.status_code != 200:
                error_text = await response.aread()
                error_msg = error_text.decode("utf-8", errors="replace")[:1000]

                # 原生函数 Schema 被拒优先于 Cookie/项目分类（与事件流分支
                # 的判定顺序一致；旧实现反序，且 "invalid argument" 被归进
                # 项目关键词，导致可降级的协议拒绝永远走不到降级重试）。
                if is_native_tool_error(error_msg):
                    yield (
                        "native_tool_unsupported",
                        StreamRequestError(error_msg, status_code=response.status_code),
                    )
                    return

                if (
                    response.status_code in (401, 403)
                    or is_cookie_expired_error(error_msg)
                    or is_project_error(error_msg)
                ):
                    hint = (
                        PROJECT_ERROR_HINT
                        if is_project_error(error_msg)
                        else COOKIE_REFRESH_HINT
                    )
                    yield (
                        "cookie_error",
                        StreamRequestError(
                            error_msg + hint, status_code=response.status_code
                        ),
                    )
                    return
                is_retryable = is_retryable_status(response.status_code)
                error = StreamRequestError(
                    error_msg,
                    getattr(response, "headers", {}).get("retry-after"),
                    response.status_code,
                )
                yield "retryable_error" if is_retryable else "fatal_error", error
                return

            # 2. 实时遍历并抛出流式 JSON 事件块
            async for obj in iter_json_objects(response):
                if sampler is not None:
                    sampler.add(obj)
                events = list(extract_events(obj, preserve_parts=preserve_parts))
                # 「收到首个上游分片」只对真实内容/元数据分片记账：空分片与纯错误
                # 分片不算——否则 429 场景时间线会先报“收到分片”再报“上游失败”，
                # 自相矛盾（首个分片常是 streamGenerateContentV2:[] 空壳）。
                if any(et != "error" for et, _ in events):
                    upstream_chunk()
                for event_type, data in events:
                    if event_type in (
                        "text",
                        "thought",
                        "image",
                        "function_call",
                        "thought_signature",
                        "native_part",
                    ):
                        has_content = True
                        yield event_type, data

                    elif event_type in ("finish", "usage", "blocked", "safety"):
                        if event_type == "finish" and has_finish_reason(data):
                            got_finish = True
                        yield event_type, data

                    elif event_type == "error":
                        code, err_msg = api_error_details(data)
                        error = StreamRequestError(err_msg, status_code=code)
                        explicit_code = (
                            data.get("code") if isinstance(data, dict) else None
                        )

                        # 在还没发送任何有效数据前遇到错误，尝试走顶层重试
                        if is_native_tool_error(err_msg) and not has_content:
                            yield "native_tool_unsupported", error
                            return
                        if (
                            code in (401, 403)
                            or is_cookie_expired_error(err_msg)
                            or is_project_error(err_msg)
                        ) and not has_content:
                            hint = (
                                PROJECT_ERROR_HINT
                                if is_project_error(err_msg)
                                else COOKIE_REFRESH_HINT
                            )
                            yield (
                                "cookie_error",
                                StreamRequestError(err_msg + hint, status_code=code),
                            )
                            return
                        # Numeric HTTP status wins over message keywords. The
                        # consumer knows whether thoughts were actually exposed
                        # and decides whether replay is still safe.
                        retryable = (
                            is_retryable_status(code)
                            if isinstance(explicit_code, int)
                            and 400 <= explicit_code <= 599
                            else is_retryable_error(err_msg)
                        )
                        yield "retryable_error" if retryable else "fatal_error", error
                        return

            if not got_finish:
                yield (
                    "fatal_error",
                    StreamRequestError(
                        "上游连接已结束，但回复缺少结束标记，内容可能不完整。"
                        if has_content
                        else "上游连接已结束，但未收到模型回复或结束标记。",
                        status_code=502,
                    ),
                )

    except Exception as e:
        err_msg = str(e)
        is_retryable = is_retryable_exception(e)
        code, _ = extract_upstream_error(e)
        yield (
            "retryable_error" if is_retryable else "fatal_error",
            StreamRequestError(err_msg, status_code=code),
        )


async def _execute_stream_request_generator_raw(
    client: Any,
    headers: dict,
    body: dict,
    sampler: "RawSampler | None" = None,
    fallback_body: Any = None,
    *,
    preserve_parts: bool = False,
) -> AsyncGenerator[tuple[str, Any], None]:
    """Execute native tools, lazily degrading after one clear protocol rejection."""
    events = _execute_stream_request_generator_once(
        client, headers, body, sampler, preserve_parts=preserve_parts
    )
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
    # 降级状态由调用方提供的 fallback_body 回调锁定，传输层只执行替代请求。
    resolved_fallback = fallback_body() if callable(fallback_body) else fallback_body
    if hasattr(resolved_fallback, "__await__"):
        resolved_fallback = await resolved_fallback
    print(
        "⚠️ [Studio] 当前模型/协议拒绝原生函数 Schema；本次及后续重试固定降级为文本工具观测。"
    )
    events = _execute_stream_request_generator_once(
        client, headers, resolved_fallback, sampler, preserve_parts=preserve_parts
    )
    try:
        async for fallback_status, fallback_data in events:
            yield fallback_status, fallback_data
    finally:
        await events.aclose()


async def stream_events(
    client,
    headers,
    body,
    sampler=None,
    fallback_body=None,
    *,
    model="",
    preserve_parts=False,
):
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
            window = max(
                0.001, min(guard.seconds, guard.initial_deadline - time.monotonic())
            )
            guard.deadline = time.monotonic() + window
            if guard.trace:
                with guard.trace._lock:
                    guard.trace.data["first_reply_deadline"] = time.time() + window
        source = _execute_stream_request_generator_raw(
            client, headers, body, sampler, fallback_body, preserve_parts=preserve_parts
        )
        pending = PendingMetadataGate()
        switch_proxy = False
        try:
            async for item in guard.iterate(
                source,
                lambda event: (
                    event[0]
                    in ("text", "thought", "image", "function_call", "native_part")
                    and bool(event[1])
                ),
            ):
                status, data = item
                if status in (
                    "retryable_error",
                    "fatal_error",
                    "cookie_error",
                    "blocked",
                ):
                    if (
                        not guard.ready
                        and chain is not None
                        and status in ("retryable_error", "fatal_error")
                    ):
                        switch_proxy = chain.advance(
                            status=getattr(data, "status_code", None)
                        )
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
                yield "retryable_error", StreamRequestError(str(exc), status_code=504)
                return
        finally:
            await source.aclose()

"""Cookie HTTP event transport shared by OpenAI and Gemini adapters.

Owns upstream error classification, raw event reads, tool-protocol fallback and
first-reply/proxy handling. It does not construct downstream API responses.
"""

import json
import time
from typing import Any, AsyncGenerator, AsyncIterator, Callable, Optional

import httpx
from fastapi import Request

from cookie_auth import BATCH_GRAPHQL_URL
from json_encoding import estimate_json_chars
from media_tasks import run_media_task
from http_io import UNAVAILABLE_DIAGNOSTIC, read_status_body
from first_reply import FirstReplyGuard, FirstReplyTimeout, native_has_reply, timeout_seconds
from request_tracking import upstream_attempt, upstream_chunk
from response_validation import (
    IncompleteResponseError,
    PendingMetadataGate,
    ToolStreamError,
    has_finish_reason,
)
from retry_policy import (
    is_retryable_exception,
    is_retryable_message,
    is_retryable_status,
    response_retry_after,
    retry_after_seconds,
    retry_delay,
    wait_for_retry,
)
from streaming import HEARTBEAT_FRAME
from upstream_errors import extract_upstream_error
from upstreams.cookie_response import (
    BODY_EVENT_TYPES,
    RawSampler,
    envelope_errors,
    CookieCandidateBoundary,
    extract_events,
    has_pending_inline_image,
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
    lower = (error_msg or "")[:4096].lower()
    tool_terms = (
        "functiondeclaration",
        "function declaration",
        "functionresponse",
        "function response",
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
        "not defined",
        "unrecognized",
        "invalid argument",
        "invalid value",
        "cannot use",
        "not enabled",
    )
    return any(term in lower for term in tool_terms) and any(
        term in lower for term in reject_terms
    )


def is_native_tool_rejection(error: Any) -> bool:
    """A documented error message, not a stringified dictionary of arbitrary data."""
    if isinstance(error, dict):
        code = error.get("code")
        if isinstance(code, int) and 400 <= code <= 599 and code not in (400, 422):
            return False
        grpc = grpc_status_code(error)
        if grpc in _GRPC_TO_HTTP and _GRPC_TO_HTTP[grpc] != 400:
            return False
        error = error.get("message")
    return isinstance(error, str) and is_native_tool_error(error)


def is_native_tool_error_response(status_code: int, payload: str | bytes) -> bool:
    """Only inspect bounded HTTP validation diagnostics. HTTP 200 uses parsed events."""
    if status_code not in (400, 422) or len(payload) > 16 * 1024:
        return False
    text = (payload.decode("utf-8", errors="replace") if isinstance(payload, bytes) else payload).strip()
    text = text.removeprefix(")]}'").lstrip()
    if not text.startswith(("{", "[")):
        # Some older Studio endpoints return the validation message as plain text.
        return not (text.startswith(("<", '"')) or "{" in text) and is_native_tool_error(text)
    try:
        value = json.loads(text)
        for obj in value if isinstance(value, list) else [value]:
            if not isinstance(obj, dict):
                continue
            if any(is_native_tool_rejection(err) for err in envelope_errors(obj)):
                return True
            results = obj.get("results")
            if isinstance(results, list):
                for result in results:
                    errors = result.get("errors") if isinstance(result, dict) else None
                    if isinstance(errors, list) and any(is_native_tool_rejection(err) for err in errors):
                        return True
    except (ValueError, RecursionError, IncompleteResponseError):
        pass
    return False


def _event_has_reply(event: tuple):
    kind, data = event
    if kind == "native_part":
        return native_has_reply({"candidates": [{"content": {"parts": [data]}}]})
    return kind in ("text", "thought", "image", "thought_image", "hidden_thought", "function_call") and bool(data)


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


def grpc_status_code(data: Any) -> int | None:
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


def api_error_details(data: Any) -> tuple[int, str]:
    """GraphQL 错误块 → (HTTP 状态码, 消息) 的**唯一**分类入口。

    优先级：显式 code 字段 > gRPC 状态码（extensions.status.code）>
    鉴权/项目类文本关键词 > 限流文本 > 502。文本兜底从流式路径收敛到
    这里（2026-11 修复）：此前只有流式判定 Cookie/项目失效（且文本可
    覆盖结构化码），非流式与生图整取只认结构化码——同一错误在
    GraphQL 块不带 code 字段时，流式触发续期提示与账号隔离、非流式
    返回 502 且不记账。现在流式/非流式/生图/探测四处共用本函数，
    行为一致；结构化信号仍优先于文本（与 COOKIE_EXPIRED_KEYWORDS
    的注释口径对齐）。
    """
    raw = data.get("message") if isinstance(data, dict) else data
    message = raw if isinstance(raw, str) and raw.strip() else str(data)
    code = data.get("code") if isinstance(data, dict) else None
    if not isinstance(code, int) or not 400 <= code <= 599:
        # 结构化信号优先：gRPC 状态码（extensions.status.code）→ HTTP。
        code = _GRPC_TO_HTTP.get(grpc_status_code(data))
    if not isinstance(code, int) or not 400 <= code <= 599:
        # 文本兜底：Cookie/项目权限类文案 → 403，触发续期提示与账号隔离。
        if is_cookie_expired_error(message) or is_project_error(message):
            code = 403
    if not isinstance(code, int) or not 400 <= code <= 599:
        code = 429 if is_retryable_error(message) else 502
    return code, message


async def retry_heartbeats(total_sec: float, hb_interval: float = 3.0, request: Request=None) -> AsyncIterator[str]:
    """Keep the connection alive only while the monotonic retry window is open."""
    deadline = time.monotonic() + total_sec
    step = max(0.5, hb_interval)
    while (remaining := deadline - time.monotonic()) > 0:
        if not await wait_for_retry(request, min(step, remaining)):
            return
        if time.monotonic() < deadline:
            yield HEARTBEAT_FRAME


# Cookie 通道重试退避的统一入口（2026-11 P2 重试执行器收敛）：此前同一
# 份「计算延迟 + 打日志 + 等待/吐心跳」骨架在 cookie_proxy 与
# cookie_native 里平行存在 8 份，日志格式、attempt 口径与退避注释各自
# 漂移（生图/流式/非流式/原生桥四种措辞、中文/半角冒号混用）。收敛后
# 8 个调用点只保留各自不同的部分：错误来源、断连终态与交付形态。
def _cookie_backoff_delay(*, retry_after: Optional[str], backoff_sec: float, attempt: int, jitter_sec: float, error: str, label: str, log_tag: str) -> float:
    """计算 Cookie 通道退避延迟并打统一格式的重试日志。

    Cookie 重试循环**复用同一 Cookie 身份**，上游的 Retry-After 对该身份
    仍有约束力，因此 identity_rotates=False——上限放宽为
    max(窗口上限, Retry-After)（保留 120 秒绝对上限），口径详见
    retry_policy._retry_delay_value 的参数说明。
    """
    delay = retry_delay(retry_after, backoff_sec, attempt, jitter_sec,
                        error=error, identity_rotates=False)
    print(f"⚠️ {log_tag} {label}（尝试 {attempt + 1}）: {str(error)[:300]} | {delay:.1f}s 后重试...")
    return delay


async def cookie_retry_backoff(request: Request, *, retry_after: Optional[str], backoff_sec: float, attempt: int, jitter_sec: float,
                               error: str, label: str, log_tag: str="[Studio]") -> bool:
    """Cookie 通道退避窗口（静默等待版，非流式/生图整取路径用）。

    返回 False 表示客户端已断开，调用方按各自交付形态收尾
    （499 JSONResponse / 错误字典 / 直接 return）并放弃重试。
    """
    delay = _cookie_backoff_delay(retry_after=retry_after, backoff_sec=backoff_sec,
                                  attempt=attempt, jitter_sec=jitter_sec,
                                  error=error, label=label, log_tag=log_tag)
    return await wait_for_retry(request, delay)


async def cookie_retry_backoff_heartbeats(request: Request, *, retry_after: Optional[str], backoff_sec: float, attempt: int,
                                           jitter_sec: float, error: str, label: str, log_tag: str="[Studio]") -> AsyncIterator[str]:
    """Cookie 通道退避窗口（流式心跳版，流式/原生桥路径用）。

    等待期间持续吐 SSE 心跳帧保活，前端不会在 429 长等待期间超时断开。
    retry_heartbeats 在客户端断开时自行停止；调用方在 for 循环结束后
    复查 is_disconnected() 决定终态（return / raise），再 continue。
    """
    delay = _cookie_backoff_delay(retry_after=retry_after, backoff_sec=backoff_sec,
                                  attempt=attempt, jitter_sec=jitter_sec,
                                  error=error, label=label, log_tag=log_tag)
    async for frame in retry_heartbeats(delay, request=request):
        yield frame


class StreamRequestError(str):
    """保持错误字符串兼容性，并把 HTTP 退避信息带到流式重试层。"""

    def __new__(cls, message: str, retry_after: Optional[str]=None, status_code: Optional[int]=None, *, cause: Optional[BaseException]=None):
        value = super().__new__(cls, message)
        value.retry_after = retry_after
        value.status_code = status_code
        value.cause = cause
        return value


async def _execute_stream_request_generator_once(
    client: Any,
    headers: dict,
    body: dict,
    sampler: "RawSampler | None" = None,
    *,
    preserve_parts: bool = False,
    strip_thoughts: bool = False,
) -> AsyncGenerator[tuple[str, Any], None]:
    """
    异步生成器：真正实时地拉取数据，抛出**原始事件**（不做 SSE 格式化）：
    ("text"|"thought"|"image", str)、("finish", 原始finishReason)、("usage", dict)、
    ("blocked", str；原生模式为已校验的 promptFeedback 对象)、("api_error_text", str)、("cookie_error"|"retryable_error"|"fatal_error", str)。
    由消费方负责格式化成 OpenAI chunk（便于预填充去重与无正文诊断）。
    """
    has_content = False
    got_finish = False
    boundary = CookieCandidateBoundary(check_tool_ids=not preserve_parts)
    try:
        upstream_attempt()
        async with client.stream(
            "POST", BATCH_GRAPHQL_URL, headers=headers, json=body
        ) as response:
            # 1. 拦截 HTTP 状态码错误
            if response.status_code != 200:
                error_text = await read_status_body(response)
                if error_text is None:
                    error_text = UNAVAILABLE_DIAGNOSTIC
                error_msg = error_text[:4000].decode("utf-8", errors="replace")[:1000]
                hint_value = response_retry_after(response)

                # 原生函数 Schema 被拒优先于 Cookie/项目分类（与事件流分支
                # 的判定顺序一致；旧实现反序，且 "invalid argument" 被归进
                # 项目关键词，导致可降级的协议拒绝永远走不到降级重试）。
                if is_native_tool_error_response(response.status_code, error_text):
                    yield (
                        "native_tool_unsupported",
                        StreamRequestError(error_msg, hint_value, response.status_code),
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
                            error_msg + hint, hint_value, response.status_code
                        ),
                    )
                    return
                is_retryable = is_retryable_status(response.status_code)
                error = StreamRequestError(
                    error_msg,
                    hint_value,
                    response.status_code,
                )
                yield "retryable_error" if is_retryable else "fatal_error", error
                return

            # 2. 实时遍历并抛出流式 JSON 事件块
            async for obj in iter_json_objects(response):
                if sampler is not None:
                    sampler.add(obj)
                # 上游已回 200 后的本地解析/校验错误不是坏出口的证据（同一内容
                # 换出口也会一样坏），包装为 ToolStreamError 以免触发代理轮换。
                try:
                    # PRF-P1-1：含内联图片的对象在媒体线程解析（base64 解码 +
                    # 落盘不能停在事件循环上）；纯文本对象原地零开销解析。
                    if has_pending_inline_image(obj):
                        events = await run_media_task(
                            list, extract_events(obj, preserve_parts=preserve_parts, strip_thoughts=strip_thoughts, boundary=boundary), heavy=True)
                    else:
                        events = list(extract_events(obj, preserve_parts=preserve_parts, strip_thoughts=strip_thoughts, boundary=boundary))
                except IncompleteResponseError as error:
                    raise ToolStreamError(str(error)) from error
                # Events now own the payloads they need. Keep raw diagnostic
                # samples only until genuine content arrives, never across a
                # subsequent read once the consumer is finished with them.
                del obj
                if sampler is not None and any(
                    et in BODY_EVENT_TYPES for et, _ in events
                ):
                    sampler.stop_retaining()
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
                        "thought_image",
                        "hidden_thought",
                        "function_call",
                        "thought_signature",
                        "native_part",
                    ):
                        has_content = True
                        yield event_type, data

                    elif event_type in ("finish", "usage", "blocked", "safety", "candidate_metadata"):
                        if event_type == "finish" and has_finish_reason(data):
                            got_finish = True
                        yield event_type, data

                    elif event_type == "error":
                        code, err_msg = api_error_details(data)
                        error = StreamRequestError(err_msg, response_retry_after(response), code)
                        explicit_code = (
                            data.get("code") if isinstance(data, dict) else None
                        )

                        # 在还没发送任何有效数据前遇到错误，尝试走顶层重试
                        if not has_content and not got_finish and is_native_tool_rejection(data):
                            yield "native_tool_unsupported", error
                            return
                        # 鉴权失效判定收敛进 api_error_details（2026-11）：
                        # code 已含结构化 401/403 与文本兜底（Cookie/项目类
                        # 文案 → 403），此处只看结果码；提示语仍按项目/
                        # Cookie 区分。
                        if code in (401, 403) and not has_content:
                            hint = (
                                PROJECT_ERROR_HINT
                                if is_project_error(err_msg)
                                else COOKIE_REFRESH_HINT
                            )
                            yield (
                                "cookie_error",
                                StreamRequestError(err_msg + hint, error.retry_after, code),
                            )
                            return
                        # Numeric HTTP status wins over message keywords. The
                        # consumer knows whether thoughts were actually exposed
                        # and decides whether replay is still safe.
                        retryable = (
                            is_retryable_status(code)
                            if (isinstance(explicit_code, int)
                                and 400 <= explicit_code <= 599)
                            or grpc_status_code(data) in _GRPC_TO_HTTP
                            else is_retryable_error(err_msg)
                        )
                        yield "retryable_error" if retryable else "fatal_error", error
                        return
                    del data
                del events

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
            StreamRequestError(err_msg, status_code=code, cause=e),
        )


async def _execute_stream_request_generator_raw(
    client: Any,
    headers: dict,
    body: dict,
    sampler: "RawSampler | None" = None,
    fallback_body: Any = None,
    *,
    preserve_parts: bool = False,
    strip_thoughts: bool = False,
) -> AsyncGenerator[tuple[str, Any], None]:
    """Execute native tools, lazily degrading after one clear protocol rejection."""
    while True:
        events = _execute_stream_request_generator_once(
            client, headers, body, sampler, preserve_parts=preserve_parts, strip_thoughts=strip_thoughts
        )
        try:
            async for status, data in events:
                if status != "native_tool_unsupported":
                    yield status, data
                    del data
                    continue
                if fallback_body is None or retry_after_seconds(getattr(data, "retry_after", None)):
                    yield "fatal_error", data
                    return
                break
            else:
                return
        finally:
            await events.aclose()
        break

    # 先归还被拒请求的连接，再构造/启动替代请求。
    # 降级状态由调用方提供的 fallback_body 回调锁定，传输层只执行替代请求。
    resolved_fallback = fallback_body() if callable(fallback_body) else fallback_body
    if hasattr(resolved_fallback, "__await__"):
        resolved_fallback = await resolved_fallback
    print(
        "⚠️ [Studio] 当前模型/协议拒绝原生工具字段；本次及后续重试固定降级为文本工具观测，图片保留为视觉输入。"
    )
    events = _execute_stream_request_generator_once(
        client, headers, resolved_fallback, sampler, preserve_parts=preserve_parts, strip_thoughts=strip_thoughts
    )
    try:
        async for fallback_status, fallback_data in events:
            yield ("fatal_error" if fallback_status == "native_tool_unsupported" else fallback_status), fallback_data
            del fallback_data
    finally:
        await events.aclose()


async def stream_events(
    client: httpx.AsyncClient,
    headers: dict,
    body: dict,
    sampler: Optional[RawSampler]=None,
    fallback_body: Optional[Callable]=None,
    *,
    model: str="",
    preserve_parts: bool=False,
    strip_thoughts: bool=False,
) -> AsyncIterator[tuple]:
    chain = getattr(client, "chain", None)
    # 首回复窗口每请求只读一次设置（while 循环里换出口/重试会反复新建 Guard）。
    fr_seconds = timeout_seconds(model)
    # Candidates share one fixed first-reply budget, including time spent on
    # earlier failures. Never extend an elapsed window by even a timer tick.
    guard = FirstReplyGuard(model, seconds=fr_seconds)
    while True:
        source = _execute_stream_request_generator_raw(
            client, headers, body, sampler, fallback_body, preserve_parts=preserve_parts, strip_thoughts=strip_thoughts
        )
        pending = PendingMetadataGate()
        switch_proxy = False
        try:
            async for item in guard.iterate(
                source,
                _event_has_reply,
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
                        # Preserve exception identity. A local parser's 502 is
                        # not an HTTP 502 returned by the selected proxy.
                        cause = getattr(data, "cause", None)
                        switch_proxy = chain.advance(cause,
                            status=getattr(data, "status_code", None) if cause is None else None,
                            retry_after=getattr(data, "retry_after", None))
                        if switch_proxy:
                            break
                    # 错误前已缓冲的元数据（finish/usage/safety）先冲刷再报错，
                    # 与正常收尾路径一致，不静默丢弃上游给出的诊断线索。
                    for prefix in pending.drain():
                        yield prefix
                    yield item
                    return
                if not guard.ready:
                    # PRF-P2-1（2026-11 收敛）：元数据计量不再 str(data)
                    # 物化——native_part 含数十 MB base64 时先拼整串再
                    # UTF-8 编码只为算字节数；改无拷贝递归估长（str/bytes
                    # 直接 len，容器走结构估算），上限闸粗估口径不变。
                    if isinstance(data, str):
                        pending_size = len(data)
                    elif isinstance(data, (bytes, bytearray)):
                        pending_size = len(data)
                    else:
                        pending_size = estimate_json_chars(data)
                    pending.add(item, pending_size)
                    continue
                for prefix in pending.drain():
                    yield prefix
                    del prefix
                yield item
                del item, data
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

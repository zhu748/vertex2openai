"""Gemini 原生透传编排引擎（QUA-P2-9 自 routes/gemini_native.py 拆出）。

单次上游请求（_passthrough_once）→ 重试编排（_passthrough_request*：
Key 轮询 / 钉定路径回退 / 429-5xx 退避）→ 流式与非流式转发（_relay_stream
/ _native_json_chunks / _native_sse_frames）→ 用量日志与断连续传
（_resumable_native）。Google 风格错误形状（NativeAPIError / _native_error）
也在此层：引擎是主要消费者，路由层的鉴权依赖经 re-export 复用。
"""

import asyncio
import codecs
import hashlib
import json
import re
import time
from functools import partial
from typing import Any, AsyncIterator, Iterable, Optional

from urllib.parse import parse_qs

import anyio
import httpx
from fastapi import Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from google.genai.types import BlockedReason
from starlette.background import BackgroundTasks
from request_connection import ClientConnection
from request_ingress import hold_ingress
from replay_event import ReplayEvent, data_only_frame

import config as app_config
from retry_policy import advisory_fits, get_retry_settings, is_retryable_exception, is_retryable_status, note_retry_deadline
from retry_policy import response_requests_delay, response_retry_after, retry_delay as _retry_delay, wait_for_retry as _wait_for_retry
from upstream_errors import extract_upstream_error, google_status_detail
from usage import merge_usage_metadata
from protojson import int32_value
from response_validation import FINISH_CONTENT_FILTER, IncompleteResponseError, has_finish_reason
from admission import (count_slow_setup_failure, GenerationCapacityError, limit_generation, ManagedStreamingResponse,
                       run_with_image_limit, SETUP_FAST_WINDOW_SECONDS)
from logger import stats
from request_tracking import upstream_attempt, upstream_chunk
import first_reply as first_reply_module
from first_reply import FirstReplyGuard, native_has_reply
from json_stream import MAX_JSON_OBJECT_BYTES, JsonObjects
from http_io import (MAX_UPSTREAM_ERROR_BYTES, buffer_upstream_response, close_httpx_response as _close_upstream,
                     read_upstream_bytes, send_owned_httpx)
from runtime_state import app_state
from express_endpoint import is_location_pin_failure
from express_fingerprint import build_direct_headers
from client_pool import passthrough_client_pool
from proxy_resolver import ProxyResolveError, resolve_proxy_uri
from proxy_failover import ProxyChain
from proxy_tls import build_httpx_proxy
from stream_cleanup import AttemptCancellation
from streaming import (SSE_NO_BUFFER_HEADERS, sse_frames, with_heartbeats, HEARTBEAT, GEMINI_HEARTBEAT_FRAME,
                       StreamDeadlineExceeded, reads_before_deadline, sse_separator)
from resumable import (resumable_store, resolve_buffer, ResumableBuffer, ResumeRequestError, BufferCapacityError,
                       DetachedRequest, ReplayStreamResponse, within_request_deadline, buffered_setup_result,
                       fast_setup_completed, request_timeout_seconds, resume_cursor)
from upstreams.native_catalog import (
    _build_upstream_url,
    _resolve_upstream_model_path,
)

# A concrete prompt block is a deliverable terminal response, not model output.
_PROMPT_BLOCK_REASONS = frozenset(reason.value for reason in BlockedReason
                                  if reason is not BlockedReason.BLOCKED_REASON_UNSPECIFIED)
_UPSTREAM_TIMEOUT = httpx.Timeout(connect=30.0, read=600.0, write=60.0, pool=10.0)
# The audited client parser treats non-data SSE lines as JSON. This is a
# client-version match, not the server's installed SDK version or an identity.
_DATA_ONLY_REPLAY_SDK = "google-genai-sdk/2.31.0"
_PYTHON_RUNTIME_LABEL = re.compile(r"gl-python/[0-9]+\.[0-9]+\.[0-9]+(?:(?:a|b|rc)[0-9]+)?\+?")


class NativeAPIError(Exception):
    """携带 Google 风格 JSONResponse 的鉴权/参数异常，由 main.py 注册的
    全局 exception_handler 转成响应（依赖函数里不能直接 raise Response）。"""

    def __init__(self, response: JSONResponse):
        self.response = response
        super().__init__("NativeAPIError")


def _native_error(status: int, message: str, status_detail: Optional[str] = None) -> JSONResponse:
    """构造 Google 风格错误体（原生 SDK 会按 error.message 展示）。"""
    detail = status_detail or google_status_detail(status)
    return JSONResponse(status_code=status, content={
        "error": {"code": status, "message": message, "status": detail}
    })


def _looks_like_pin_failure(status_code: int, body: bytes) -> bool:
    """仅项目/模型权限错误允许切回裸模型路径，复用 SDK 判据。"""
    return status_code in (403, 404) and is_location_pin_failure(body.decode("utf-8", "ignore"))


async def _passthrough_once(client: httpx.AsyncClient, express_key: str,
                            model_path: str, action: str, query: str,
                            payload: bytes, *, stream: bool) -> httpx.Response:
    """向上游发一次请求。stream=True 时返回**未消费**的响应
    （供 aiter_bytes 逐块转发）；非流式成功响应在传输边界缓冲。已知 HTTP
    错误交给按状态码处理的调用方限量读取，即使诊断正文损坏也保留状态与退避头。

    指纹保真三要点（MockTransport 与官方 SDK 抓包逐字节对照得出）：
    1. 头集合与线序 = build_direct_headers(express_key)，即 SDK 原生形态；
       不再钉 accept-encoding: identity——真实 SDK 走 httpx 默认的
       `gzip, deflate`，钉 identity 反而是全网 SDK 用户里没有的异类形态。
    2. payload 是客户端请求体**原始字节**：不解析重组，客户端的 SDK
       序列化出什么样就转发什么样（等价于“客户端直连 Google”）。
    3. 压缩由 httpx 透明处理：流式 aiter_bytes / 非流式 aread 都会自动
       解压，且本服务不透传 content-encoding 头，客户端拿到的就是
       上游解压后的原始载荷。"""
    url = _build_upstream_url(model_path, action)
    if query:
        url = f"{url}?{query}"
    req = client.build_request(
        "POST", url, content=payload,
        headers=build_direct_headers(express_key),
    )
    response = await send_owned_httpx(client.send, req)
    if stream or response.is_error:
        # The status and Retry-After are authoritative as soon as headers arrive.
        # A broken diagnostic body must not turn a known 400 into a retryable
        # connection error (or send a known 429 back without its retry hint).
        return response
    try:
        return await buffer_upstream_response(response)
    finally:
        await _close_upstream(response)


@limit_generation
async def _passthrough_request(fastapi_request: Request, model: str, action: str,
                               query: str, payload: bytes):
    return await run_with_image_limit(
        model.rpartition("/")[2] if action in ("generateContent", "streamGenerateContent") else "",
        lambda: _passthrough_request_impl(fastapi_request, model, action, query, payload),
    )


def _classify_attempt_error(e: BaseException, upstream: Optional[httpx.Response]) -> tuple:
    """尝试异常的错误分类（拆分自 _passthrough_request_impl）。

    返回 (status, body, retry-after 头)：NativeStreamError 携带上游真实
    状态码与载荷；超时归 504；不完整响应归 502；其余连接失败 502。
    upstream 可能已关闭（headers 仍可读）或为 None（未建连）。
    """
    headers = {}
    if upstream is not None:
        headers = {k: v for k, v in upstream.headers.items()
                   if k.lower() == "retry-after"}
    if isinstance(e, NativeStreamError):
        return e.code, e.payload, headers
    if isinstance(e, (TimeoutError, httpx.TimeoutException)):
        return 504, _native_error(504, str(e) or "Upstream timeout", "DEADLINE_EXCEEDED").body, headers
    if isinstance(e, IncompleteResponseError):
        return 502, _native_error(502, str(e), "UNAVAILABLE").body, headers
    return 502, _native_error(502, f"Upstream connection failed ({type(e).__name__})", "UNAVAILABLE").body, headers


async def _prefetch_stream_reply(upstream: httpx.Response, first_reply: FirstReplyGuard,
                                    stream_state: "_NativeStreamState", is_sse_request: bool) -> Optional[AsyncIterator]:
    """流式上游响应的帧源建立与首回复预取（拆分自 _passthrough_request_impl）。

    alt=sse 要求 text/event-stream 形态；不带 alt=sse 的
    streamGenerateContent 返回分块 JSON 数组是合法形态——单个 JSON
    对象可能承载整张生成图，其部分原始字节是响应数据而非 SSE 元数据，
    预取上限放宽到单对象上限。
    """
    if "text/event-stream" in upstream.headers.get("content-type", "").lower():
        source = _native_sse_frames(upstream, reply_guard=first_reply, state=stream_state)
        return await first_reply.prefetch(source, lambda _frame: first_reply.ready)
    if not is_sse_request:
        source = _native_json_chunks(upstream, first_reply, state=stream_state)
        return (await first_reply.prefetch(source, lambda _chunk: first_reply.ready,
                                           max_prefix_bytes=MAX_JSON_OBJECT_BYTES)
                if first_reply.seconds else source)
    return None


async def _buffered_upstream_result(upstream: httpx.Response) -> tuple:
    """缓冲整取响应（错误或非流式）并关闭上游（拆分自 _passthrough_request_impl）。

    返回 (status, ctype, headers, body)；读取失败时空正文不能冒充完整
    响应——已知错误码保留、成功码改 502 并回退 JSON 错误体；retry-after
    是客户端退避的关键依据，一并透传。
    """
    cancellation = AttemptCancellation()
    status = upstream.status_code
    ctype = upstream.headers.get("content-type", "application/json")
    headers = {k: v for k, v in upstream.headers.items() if k.lower() == "retry-after"}
    try:
        body = await read_upstream_bytes(upstream)
    except Exception:
        status = status if status >= 400 else 502
        body = _native_error(status, "上游响应未完整传输，请重试。").body
        ctype = "application/json"
    finally:
        try:
            await _close_upstream(upstream)
        finally:
            # Do not let a late diagnostic unlock a key or model-path retry.
            await cancellation.check()
    return status, ctype, headers, body


def native_expects_one_candidate(body: dict) -> bool:
    """Default is one; never infer a lone terminal for ambiguous config/aliases."""
    configs = [body[key] for key in ("generationConfig", "generation_config") if key in body]
    if not configs or configs == [None]:
        return True
    if len(configs) != 1 or not isinstance(configs[0], dict):
        return False
    values = [configs[0][key] for key in ("candidateCount", "candidate_count") if key in configs[0]]
    return not values or (len(values) == 1 and (values[0] is None or int32_value(values[0]) == 1))


async def _passthrough_request_impl(fastapi_request: Request, model: str, action: str,
                                   query: str, payload: bytes):
    """选 Key → 透传 → 429/5xx 换 Key 重试 → 钉定失败兜底。返回 Response。

    客户端生命周期：共享池化客户端（app/client_pool.py），同一代理形态下
    全部透传请求复用同一连接池（keep-alive），重试也不重新握手；控制台
    热改代理后新请求自动换新客户端，旧客户端延迟回收。因此本函数与
    _relay_stream 都**不关闭**共享客户端，只负责关闭上游响应。

    两类重试互相独立、不共用预算：
    - Key 重选重试（429/5xx/网络异常）：读取控制台 retry_max，每次按策略
      重新选择 Express Key，单 Key 或一轮用完后也继续重试；重试间隔为
      固定值 + 抖动（retry_backoff_seconds / retry_jitter_seconds，不随
      次数放大），有效 Retry-After 不缩短，超过本地预算则直接交还原错误；
    - 钉定失败兜底（403/404 且报错像项目/区域问题，见 _looks_like_pin_failure）：
      不换 Key、不占上面的预算，仅把模型路径退回裸模型名再试一次——
      与 SDK 通道（express_pipeline.execute_gemini_call 的 fallback_model）对齐，
      避免「OpenAI 接口能自动兜底、原生透传却把 404 原样抛给客户端」。
    """
    started_at = time.monotonic()
    deadline = note_retry_deadline(fastapi_request, started_at + request_timeout_seconds())
    manager = fastapi_request.app.state.express_key_manager
    if manager.get_total_keys() == 0:
        return _native_error(
            403,
            "该端点需要至少一个 Express API Key（字节级透传）或一个启用的 Cookie 账号"
            "（协议级桥接，仅 generateContent / streamGenerateContent）。",
            status_detail="PERMISSION_DENIED",
        )

    last_error_body = b""
    last_error_status = 502
    last_error_headers: dict[str, str] = {}
    # 每轮尝试的流式契约状态（帧源与 _relay_stream 共享；成功路径下在
    # 拿到 2xx 流式响应时创建，重试换轮自动丢弃旧用量）。
    stream_state: Optional[_NativeStreamState] = None

    def _new_client(proxy_raw: str = "") -> httpx.AsyncClient:
        """按指定代理（原始 URI）建透传客户端。

        调用前池签名已对同一 URI resolve 过一次（命中缓存），
        这里的 resolve 必然拿到同一结果，不会二次启动 sing-box。
        """
        client_kwargs: dict[str, Any] = {"timeout": _UPSTREAM_TIMEOUT, "follow_redirects": False, "trust_env": False}
        proxy_url = resolve_proxy_uri(proxy_raw)
        if proxy_url:
            client_kwargs["proxy"] = build_httpx_proxy(proxy_url, verify=app_config.SSL_CERT_FILE or True)
        if app_config.SSL_CERT_FILE:
            client_kwargs["verify"] = app_config.SSL_CERT_FILE
        return httpx.AsyncClient(**client_kwargs)

    is_stream = action == "streamGenerateContent"
    # query 在整个请求生命周期不变：提前解析一次，重试循环里
    # 三处分支不再重复 parse_qs 同一字符串。
    is_sse_request = "sse" in parse_qs(query).get("alt", [])
    retry_max, backoff_seconds, jitter_seconds = get_retry_settings()
    # 请求级设置快照：重试循环内 _resolve_upstream_model_path 每次都
    # get_settings()（锁内全量深拷贝），最多 51 次；拍一次快照复用，
    # 与 models_api/settings_api 已有 base= 模式一致。
    base_settings = app_state.get_settings()
    # 首回复窗口同样预读一次：FirstReplyGuard(model) 裸构造在每次换 Key
    # 重试时都会走 timeout_seconds → get_effective_settings 的全量深拷贝
    #（与 cookie_proxy/api_helpers 的 fr_seconds 预读模式对齐，2026-10 优化）。
    # 注意经模块属性调用：测试用 mock.patch.object(first_reply, "timeout_seconds")
    # 替换模块属性，直接 from-import 的函数绑定会绕过 patch，把窗口变成
    # 真实默认 0（关闭；回归会直接暴露）。
    model_id = model.rpartition("/")[2]
    fr_seconds = first_reply_module.timeout_seconds(model_id)
    single_candidate = False
    if fr_seconds and is_stream:
        # The route has already parsed the (possibly image-sized) request.
        # Standalone callers without route state can derive the same policy.
        single_candidate = getattr(fastapi_request, "scope", {}).get("state", {}).get("native_single_candidate")
        if single_candidate is None:
            single_candidate = native_expects_one_candidate(json.loads(payload))
    record = None
    previous_key = None
    key_attempts = 0           # 上游错误已重试次数（不含首次）
    pin_fallback_used = False  # 每次选 Key 后最多兜底一次

    while True:
        if await fastapi_request.is_disconnected():
            return Response(status_code=499)

        if record is None:
            record = manager.get_express_record(exclude_key=previous_key)
            if record is None:
                break
            if key_attempts:
                # 仅实际发起的重试计入大盘，不把退避中断计为重试。
                stats.add_retry()
            express_key = record["key"]
            if express_key != previous_key:
                pin_fallback_used = False
            previous_key = express_key
            # Key 级代理：该 Key 自己的出口优先，全局兑底（vless 等分享链接
            # 在池签名里转本地 socks5）。解析失败（sing-box 缺失/链接非法）
            # 不静默直连：预算内重选 Key，耗尽后返回明确错误。
            proxy_chain = ProxyChain(record)
            def client_for(raw: dict, resolved: Any):
                return passthrough_client_pool.get(lambda: _new_client(raw), raw)
            try:
                # 预热（sing-box 首启放线程池）；随后池签名/工厂里的同步
                # resolve 命中缓存，不卡事件循环。
                await proxy_chain.prepare(client_for)
                # 与 Express SDK 通道同款出站可观测性：本次请求的实际首选出口
                # （Key 级优先、全局兑底）；直连（无候选）静默。
                if proxy_chain.current:
                    proxy_chain.log_current()
            except ProxyResolveError as e:
                print(f"❌ [Gemini 透传] 出站代理不可用：{e}")
                if key_attempts >= retry_max:
                    return _native_error(502, f"出站代理不可用：{e}", "UNAVAILABLE")
                if not await _wait_for_retry(fastapi_request, _retry_delay(None, backoff_seconds, key_attempts, jitter_seconds)):
                    return Response(status_code=499)
                key_attempts += 1
                record = None
                continue

            # A new logical (Key) attempt owns one absolute first-reply window.
            # Switching transport candidates, even after HTTP headers, does not reset it.
            # 非流式透传是整取路径：不参与首回复计时（2026-11 语义，见
            # first_reply.timed_call）——窗口置 0 后 wait 直接放行，挂起由
            # 传输层读超时（read=600s）与整轮期限兜底；真流式保持窗口语义
            # （响应头到达 + prefetch 首个有效帧都在窗口内）。
            first_reply = FirstReplyGuard(
                model_id, seconds=fr_seconds if is_stream else 0.0)

        if pin_fallback_used:
            # 钉定兜底轮：复用同一把 Key（换的是模型路径，不是 Key）
            model_path = model
        else:
            model_path = _resolve_upstream_model_path(model, record.get("project_id", ""),
                                                     settings=base_settings)

        upstream = None
        prefetched = None
        try:
            upstream_attempt()
            async def send(raw: dict, resolved: Any,
                           _key: str = express_key, _path: str = model_path):  # B023：默认参数绑定重试循环变量
                return await _passthrough_once(
                    client_for(raw, resolved), _key, _path, action, query, payload,
                    stream=is_stream)
            upstream = await first_reply.wait(proxy_chain.run(send))
            if not is_stream:
                upstream_chunk()
                if upstream.status_code < 400:
                    first_reply.accept()
            elif upstream.status_code < 400:
                # state 由本层创建并共享给帧源与 _relay_stream：帧源负责
                # 契约校验（含首回复 guard 的 accept），收尾时从同一 state
                # 读 usageMetadata 打 💰 日志。reply_guard 必须传入：帧源
                # 收到首个有效回复时通过它结束首回复计时。
                stream_state = _NativeStreamState(reply_guard=first_reply, single_candidate=single_candidate)
                prefetched = await _prefetch_stream_reply(
                    upstream, first_reply, stream_state, is_sse_request)
        except asyncio.CancelledError:
            if upstream is not None:
                with anyio.CancelScope(shield=True):
                    await _close_upstream(upstream)
            raise
        except Exception as e:
            if upstream is not None:
                await _close_upstream(upstream)
            # The HTTP handshake can succeed and the body then fail before
            # prefetch has exposed any output. Try the same account's next
            # proxy before consuming model retry budget or selecting a Key.
            prefetch_hint = response_retry_after(upstream) if isinstance(e, NativeStreamError) else None
            if proxy_chain.advance(e, retry_after=prefetch_hint):
                continue
            last_error_status, last_error_body, last_error_headers = _classify_attempt_error(e, upstream)
            if (key_attempts >= retry_max or not is_retryable_exception(e)
                    or not advisory_fits(last_error_headers.get("retry-after"), fastapi_request, deadline=deadline)):
                break
            wait = _retry_delay(last_error_headers.get("retry-after"), backoff_seconds, key_attempts,
                                jitter_seconds, error=e, identity_rotates=False)
            print(f"⚠️ [Gemini 透传] 上游请求失败（HTTP {last_error_status}，{type(e).__name__}），"
                  f"{wait:.1f}s 后重试（{key_attempts + 1}/{retry_max}）。")
            if not await _wait_for_retry(fastapi_request, wait):
                return Response(status_code=499)
            key_attempts += 1
            record = None
            continue

        # 出站可观测性：此前「选 Key 之后 → 响应/重试」之间零日志，上游挂起
        # 或返回非预期形态时无从排查（v1/v1beta1 问题的定位就卡在这里）。
        # 默认一行摘要（状态码 + content-type）；debug_outbound 开启时附完整 URL。
        _ct = upstream.headers.get("content-type", "?")
        print(f"{'✅' if upstream.status_code < 400 else '⚠️'} [Gemini 透传] "
              f"上游响应 HTTP {upstream.status_code}（{_ct}）"
              + (f" ← {upstream.request.url}"
                 if app_state.get_setting("debug_outbound", False) else ""))
        if (is_stream and upstream.status_code < 400
                and is_sse_request
                and "text/event-stream" not in _ct.lower()):
            # 仅 alt=sse 的请求才要求 SSE 形态：不带 alt=sse 的 streamGenerateContent
            # 返回分块 JSON 数组是合法形态（上方 _native_json_chunks 专门支持），
            # 旧实现对每个此类正常请求都打「等不到 data: 行」的误导性严重告警。
            print("⚠️ [Gemini 透传] 流式请求的上游响应不是 text/event-stream——"
                  "客户端若按 SSE 解析将等不到 data: 行（v1 端点曾出现此形态，"
                  "现已钉 v1beta1；若复现请把 debug_outbound 打开后上报 URL）。")
            await _close_upstream(upstream)
            return _native_error(502, "上游未返回 SSE 流，无法完成 alt=sse 请求。", "UNAVAILABLE")

        # 429/5xx：按控制台预算重选 Key，不能因单 Key / 池已用遍而提前退出。
        retryable_status = is_retryable_status(upstream.status_code)
        if retryable_status and key_attempts < retry_max:
            last_error_status, ctype, last_error_headers, last_error_body = await _buffered_upstream_result(upstream)
            hint = last_error_headers.get("retry-after")
            if not advisory_fits(hint, fastapi_request, deadline=deadline):
                # Recheck after the diagnostic read and close: all of it spends
                # the same budget. Preserve the actual status/body/advisory.
                return Response(content=last_error_body, status_code=last_error_status, media_type=ctype,
                                headers=last_error_headers)
            wait = _retry_delay(hint, backoff_seconds, key_attempts, jitter_seconds, identity_rotates=False)
            print(f"⚠️ [Gemini 透传] 上游 HTTP {upstream.status_code}，"
                  f"{wait:.1f}s 后重试（{key_attempts + 1}/{retry_max}，单 Key 同样适用）。")
            if not await _wait_for_retry(fastapi_request, wait):
                return Response(status_code=499)
            key_attempts += 1
            record = None
            continue

        # 成功 / 不可重试错误 / Key 轮换预算耗尽 → 透传
        if is_stream and upstream.status_code < 400:
            return await _relay_stream(
                upstream, keepalive=is_sse_request,
                deadline=deadline, source=prefetched,
                model=model, started_at=started_at,
                state=stream_state if stream_state is not None else None)

        # 错误（或非流式响应）：缓冲 body 再透传，顺带供钉定失败判定
        status, ctype, headers, body = await _buffered_upstream_result(upstream)

        # 钉定路径失败 → 退回裸模型名再试一次（不占 Key 轮换预算）
        if (not pin_fallback_used and model_path != model
                and _looks_like_pin_failure(status, body) and not response_requests_delay(upstream)):
            pin_fallback_used = True
            last_error_status, last_error_body = status, body
            last_error_headers = headers
            print(f"↩️ [Gemini 透传] 钉定路径调用失败（HTTP {status}："
                  f"{body[:120].decode('utf-8', 'ignore')}），"
                  f"已退回裸模型名 {model} 重试一次。如持续出现，请确认「通道与凭证」里"
                  f"该 Key 的 Project ID 正确且已开启计费，或把「标准模式 location」"
                  f"设为“默认（后端自选）”。")
            continue

        # 原样透传（保留上游真实状态码与 body）
        if status < 400:
            stats.add_success()
            # 与 OpenAI 通道的完成日志同构：模型 | 结果 | 耗时 | 体量 + 💰 用量
            print(f"✅ [Gemini 透传] {model} | 非流式响应完成 | "
                  f"耗时 {time.monotonic() - started_at:.1f}s | 共 {len(body)} 字节")
            _log_native_usage(_native_body_usage(body))
        return Response(content=body, status_code=status, media_type=ctype,
                        headers=headers or None)

    return Response(
        content=last_error_body or _native_error(last_error_status, "Upstream unreachable").body,
        status_code=last_error_status,
        media_type="application/json",
        headers=last_error_headers or None,
    )


class NativeStreamError(RuntimeError):
    def __init__(self, payload: dict):
        self.payload = json.dumps(payload, ensure_ascii=False).encode()
        error = payload.get("error")
        code = error.get("code") if isinstance(error, dict) else None
        self.code = code if isinstance(code, int) and 400 <= code <= 599 else 502
        super().__init__(str(error))


class _NativeStreamState:
    """One completion/error contract for native SSE and JSON array streams."""
    def __init__(self, reply_guard: Optional[FirstReplyGuard]=None, *, single_candidate: bool=True):
        self.reply_guard = reply_guard
        self.single_candidate = single_candidate
        self.seen: set[int] = set()
        self.finished: dict[int, bytes | None] = {}
        self.received = False
        self.blocked = False
        # 累积快照按已出现的字段更新；尾帧仅含输出用量时保留先前输入用量。
        self.usage: Optional[dict] = None

    def inspect(self, payload: dict):
        self.inspect_batch(payload if isinstance(payload, list) else (payload,))

    def inspect_batch(self, objects: Iterable[dict]) -> None:
        # A single outbound frame/chunk can contain several upstream objects.
        # Do not release the shared gate for a batch later rejected before delivery.
        generated = None
        refusal_terminal = False
        for obj in objects:
            if not isinstance(obj, dict):
                raise IncompleteResponseError("上游事件不是 Gemini 响应对象。")
            self.received = True
            if obj.get("error"):
                raise NativeStreamError(obj)
            usage = obj.get("usageMetadata")
            if isinstance(usage, dict):
                self.usage = merge_usage_metadata(self.usage, usage)
            feedback = obj.get("promptFeedback")
            reason = feedback.get("blockReason") if isinstance(feedback, dict) else None
            if has_finish_reason(reason):
                self.blocked = True
            if not isinstance(obj.get("candidates", []), list):
                raise IncompleteResponseError("上游 candidates 格式错误。")
            candidates = obj.get("candidates") or []
            if len(candidates) > 1:
                self.single_candidate = False
            for index, candidate in enumerate(candidates):
                if not isinstance(candidate, dict):
                    raise IncompleteResponseError("上游候选格式错误。")
                content = candidate.get("content") or {}
                if not isinstance(content, dict):
                    raise IncompleteResponseError("上游候选内容格式错误。")
                parts = content.get("parts") or []
                if not isinstance(parts, list) or any(not isinstance(part, dict) for part in parts):
                    raise IncompleteResponseError("上游候选 parts 格式错误。")
                index = candidate.get("index", index)
                if type(index) is not int or index < 0:  # JSON booleans are not candidate indices
                    raise IncompleteResponseError("上游候选 index 格式错误。")
                if index != 0:
                    self.single_candidate = False
                finish_reason = candidate.get("finishReason")
                if finish_reason in FINISH_CONTENT_FILTER:
                    refusal_terminal = True
                if index in self.finished and parts:
                    raise IncompleteResponseError("上游候选已结束，却继续返回内容。")
                if parts:
                    self.seen.add(index)
                if has_finish_reason(finish_reason):
                    # Compare raw string labels, including future provider values;
                    # don't normalize enums or pin large unknown labels after yield.
                    label = (hashlib.blake2s(finish_reason.encode("utf-8", "surrogatepass")).digest()
                             if isinstance(finish_reason, str) else None)
                    previous = self.finished.get(index)
                    if label is not None and previous is not None and label != previous:
                        raise IncompleteResponseError("上游候选返回了相互矛盾的结束原因。")
                    self.finished[index] = label if label is not None else previous
            if self.reply_guard and not self.reply_guard.ready and generated is not True:
                if native_has_reply(obj):
                    generated = True
                elif isinstance(reason, str) and reason in _PROMPT_BLOCK_REASONS:
                    # An explicit prompt block is deliverable but not a first-token sample.
                    generated = False
        if self.reply_guard and not self.reply_guard.ready:
            if generated is not None:
                self.reply_guard.accept(generated=generated)
            elif self.single_candidate and refusal_terminal:
                # A conclusive model refusal is deliverable as-is. Never count
                # it as a first token or stop checking/forwarding later usage.
                self.reply_guard.accept(generated=False)

    def finish(self):
        if not self.received:
            raise IncompleteResponseError("上游流已结束，但未收到响应数据。")
        if self.seen.difference(self.finished):
            raise IncompleteResponseError("上游连接已结束，但候选回复缺少结束标记。")
        if not self.finished and not self.blocked:
            raise IncompleteResponseError("上游流已结束，但未收到模型结束标记或请求拦截结果。")


async def _native_json_chunks(upstream: httpx.Response, reply_guard: Optional[FirstReplyGuard]=None, state: Optional[_NativeStreamState]=None):
    """Validate complete JSON objects in linear time, preserving every raw byte."""
    decoder = codecs.getincrementaldecoder("utf-8")()
    parser = JsonObjects(document=True)
    state = state or _NativeStreamState(reply_guard)
    try:
        async for chunk in upstream.aiter_bytes():
            if chunk:
                upstream_chunk()
            state.inspect_batch(parser.feed(decoder.decode(chunk)))
            yield chunk
        state.inspect_batch(parser.feed(decoder.decode(b"", final=True)))
    except UnicodeError as exc:
        raise IncompleteResponseError("上游 JSON 流包含无效 UTF-8。") from exc
    parser.finish()
    state.finish()


def _inspect_sse_frame(frame: bytes, state: _NativeStreamState) -> None:
    """Release decoded data before yielding the untouched wire frame to a slow client."""
    data = b"\n".join(line[5:].lstrip() for line in frame.splitlines() if line.startswith(b"data:"))
    if data and data != b"[DONE]":
        try:
            payload = json.loads(data)
        except (ValueError, UnicodeError):
            raise IncompleteResponseError("上游 SSE 包含无效 JSON 事件。") from None
        state.inspect(payload)


async def _native_sse_frames(upstream: httpx.Response, reply_guard: Optional[FirstReplyGuard]=None, state: Optional[_NativeStreamState]=None):
    state = state or _NativeStreamState(reply_guard)
    async def observed_bytes():
        async for chunk in upstream.aiter_bytes():
            if chunk:
                upstream_chunk()
            yield chunk
    frames = sse_frames(observed_bytes())
    try:
        async for frame in frames:
            _inspect_sse_frame(frame, state)
            yield frame
        state.finish()
    finally:
        await frames.aclose()


def _log_native_usage(usage: Optional[dict]) -> None:
    """💰 token 用量日志（与 OpenAI 通道的算力消耗统计同口径）。"""
    if not isinstance(usage, dict) or not usage:
        return
    prompt = usage.get("promptTokenCount")
    candidates = usage.get("candidatesTokenCount")
    thoughts = usage.get("thoughtsTokenCount")
    total = usage.get("totalTokenCount")
    gen_parts = [f"生成 {candidates}"] if candidates is not None else []
    if thoughts is not None:
        gen_parts.append(f"思考 {thoughts}")
    parts = [f"提示词 {prompt}"] if prompt is not None else []
    parts.extend(gen_parts)
    if total is not None:
        parts.append(f"总计 {total}")
    if parts:
        print("💰 [算力消耗统计] " + " | ".join(parts) + " Tokens")


def _native_body_usage(body: bytes) -> Optional[dict]:
    """从非流式响应 body 中提取 usageMetadata（仅当子串存在才解析，
    避免为巨型生图响应白白做一次全量 JSON 解析）。"""
    if b"usageMetadata" not in body:
        return None
    try:
        parsed = json.loads(body)
    except (ValueError, UnicodeError):
        return None
    usage = parsed.get("usageMetadata") if isinstance(parsed, dict) else None
    return usage if isinstance(usage, dict) else None


async def _relay_stream(upstream: httpx.Response, *, keepalive: bool=False, deadline: float=None, source: Optional[AsyncIterator]=None,
                        model: str = "", started_at: Optional[float] = None,
                        state: Optional[_NativeStreamState] = None) -> StreamingResponse:
    """把上游 SSE（或 JSON array 流）以解压后的字节转发给客户端。

    用 aiter_bytes()（透明解压）：上游可能按我们的 accept-encoding
    (gzip, deflate) 压缩响应，这里解压后转发；本服务不透传
    content-encoding 头，与非流式路径（aread() 同样自动解压）行为
    一致，客户端拿到的就是上游的原始载荷字节。SSE 客户端按事件
    解析，解压后的分块边界与上游压缩块边界不同也不影响。
    上游响应在转发生成器 finally 里释放（消费完最后一个 chunk 才断开
    上游）。客户端断开时 Starlette 会取消生成器，finally 同样生效。
    注：httpx 客户端是共享池化的（client_pool.passthrough_client_pool，
    由池统一管理生命周期，这里不关闭它。
    """

    started = False

    async def _relay():
        nonlocal started
        started = True
        total = 0
        completed = False
        events = None
        error_frame = None
        try:
            end = deadline if deadline is not None else time.monotonic() + request_timeout_seconds()
            if source is not None:
                events = source
            elif keepalive or "text/event-stream" in upstream.headers.get("content-type", "").lower():
                events = _native_sse_frames(upstream)
            else:
                events = _native_json_chunks(upstream)
            if keepalive:
                events = with_heartbeats(events)
                yield GEMINI_HEARTBEAT_FRAME.encode()
            async for item in reads_before_deadline(events, end):
                if item is HEARTBEAT:
                    yield GEMINI_HEARTBEAT_FRAME.encode()
                elif item:
                    total += len(item)
                    yield item
            completed = True
        except Exception as exc:
            if not keepalive:
                raise
            code, _ = extract_upstream_error(exc)
            message = str(exc) if isinstance(exc, StreamDeadlineExceeded) else "上游流未完整传输，请重新生成或检查网络。"
            body = (exc.payload if isinstance(exc, NativeStreamError) else _native_error(code, message).body)
            error_frame = b"data: " + body + b"\n\n"
        finally:
            if events:
                with anyio.CancelScope(shield=True):
                    await events.aclose()
            # 先打日志再关连接：客户端断开（任务被取消）时 finally 里的
            # await 也可能被再取消，保证中止日志一定落盘。
            elapsed = time.monotonic() - started_at if started_at is not None else 0.0
            if completed:
                stats.add_success()
                # 与 OpenAI 通道的完成日志同构：模型 | 结果 | 耗时 | 体量
                print(f"✅ [Gemini 透传] {model or '未知模型'} | 流式转发完成 | "
                      f"耗时 {elapsed:.1f}s | 共 {total} 字节")
                _log_native_usage(state.usage if state is not None else None)
            else:
                # 响应头已发送为 200，中间件无法根据状态码捕获流中失败。
                stats.add_error()
                print(f"✂️ [Gemini 透传] {model or '未知模型'} | 流式中止 | "
                      f"已转发 {total} 字节 | 耗时 {elapsed:.1f}s"
                      "（客户端断开或上游/网络异常）。")
            await _close_upstream(upstream)
        if error_frame is not None:
            yield error_frame

    async def cleanup():
        try:
            # A prefetched source already owns a read even before relay starts.
            if not started:
                stats.add_error()
                if source is not None:
                    await source.aclose()
        finally:
            await _close_upstream(upstream)

    media_type = upstream.headers.get("content-type", "text/event-stream")
    passthrough = {k: v for k, v in upstream.headers.items()
                   if k.lower() == "retry-after"}
    if keepalive:
        passthrough.update(SSE_NO_BUFFER_HEADERS)
    return ManagedStreamingResponse(_relay(), status_code=upstream.status_code, cleanup=cleanup,
                                    media_type=media_type, headers=passthrough or None)


# Error terminals must fit even when the normal replay event budget is exhausted.
_MAX_REPLAY_DIAGNOSTIC_BYTES = 8192
# Many tiny keepalives can fit in the byte budget. Bound the synchronous
# diagnostic scan too; a terminal HTTP error needs only one data dispatch.
_MAX_SETUP_SSE_DISPATCHES = 64


def _single_sse_error_data(raw: bytes) -> str | None:
    """Read one complete diagnostic dispatch from an already bounded HTTP body."""
    data = None
    start = 0
    for _ in range(_MAX_SETUP_SSE_DISPATCHES):
        match = sse_separator(raw, start, final=True)
        if match is None:
            break
        event = ReplayEvent(raw[start:match.end()].decode("utf-8-sig" if not start else "utf-8"))
        if event.has_data:
            if data is not None:
                return None  # never pick one of several competing events
            data = event.data()
        start = match.end()
    return data if not raw[start:].strip(b"\r\n") else None


def _decode_setup_error(response: Response) -> tuple[Any, bool]:
    raw = response.body
    try:
        # Bytes JSON accepts BOMs and UTF-16/32 too; only ordinary UTF-8 JSON
        # can be placed as-is inside an SSE data field.
        return json.loads(raw), not raw.startswith(codecs.BOM_UTF8) and b"\x00" not in raw
    except ValueError:
        # Some Google-style failures on streamGenerateContent arrive as a
        # real HTTP error whose diagnostic is itself already SSE-framed.
        media_type = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
        if media_type == "text/event-stream" and (data := _single_sse_error_data(raw)) is not None:
            return json.loads(data), False
        raise


def _normalize_setup_error(response: Response, limit: int, *, preserve_json_wire: bool = False) -> tuple[dict, bytes]:
    """Project a completed HTTP error into bounded Google JSON; never change its status."""
    code = response.status_code
    fallback = {"message": f"上游返回 HTTP {code}，错误详情不可用。", "status": google_status_detail(code), "code": code}
    if len(response.body) <= limit:
        try:
            document, plain_json = _decode_setup_error(response)
            error = document["error"]
            if isinstance(error, dict):
                normalized = {**fallback, **error, "code": code}
                for key in ("message", "status"):
                    if not isinstance(normalized[key], str):
                        normalized[key] = fallback[key]
                # The pinned SDK recognizes a streamed Google error when
                # "error" is the first key. HTTP sets its code; retain details.
                payload = {"error": normalized}
                if preserve_json_wire:
                    payload.update((key, value) for key, value in document.items() if key != "error")
                wire = JSONResponse(payload).body  # validate Unicode and reject NaN
                if len(wire) <= limit:
                    if (preserve_json_wire and plain_json and next(iter(document)) == "error"
                            and type(error.get("code")) is int and normalized == error):
                        wire = response.body
                    return payload, wire
        except (KeyError, ValueError, TypeError, RecursionError):
            pass
    payload = {"error": fallback}
    return payload, JSONResponse(payload).body


def native_setup_error_body(response: Response) -> bytes:
    """Error JSON for direct SSE after 200 is committed, not a new HTTP response."""
    return _normalize_setup_error(response, MAX_UPSTREAM_ERROR_BYTES, preserve_json_wire=True)[1]


def _native_setup_payload(response: Response) -> dict:
    return _normalize_setup_error(response, _MAX_REPLAY_DIAGNOSTIC_BYTES)[0]


def _acknowledge_native_http(response: Response, request: Request, buf: ResumableBuffer) -> Response:
    # The response is prepared independently of the frontend. Do not consume it
    # at construction, on a failed send, or on an observed silently dropped send.
    response.headers["X-Resume-Id"] = buf.id
    background = BackgroundTasks()
    background.add_task(buf.acknowledge_http_send, ClientConnection.for_request(request))
    if response.background is not None:
        background.tasks.append(response.background)
    response.background = background
    return response


def native_replay_fingerprint(model: str, action: str, query: str, payload: bytes) -> str:
    # 指纹用原始请求体的 sha256：旧实现把已解析的 payload（可达 32MiB，
    # 含 base64 图片）重新 json.dumps 一遍，大请求数百毫秒纯 CPU。
    # 语义同步收窄为「原样重发命中同一缓冲」（与 chat_api 指纹的注释口径
    # 一致）；同一客户端重发的字节序列相同，命中行为不变。
    return "native:" + json.dumps(
        [model, action, sorted(parse_qs(query).items()),
         hashlib.sha256(payload).hexdigest()], sort_keys=True,
        ensure_ascii=False, separators=(",", ":"))


def _sdk_native_replay_view(request: Request) -> bool:
    for name in ("x-goog-api-client", "user-agent"):
        labels = request.headers.get(name, "").split()
        if _DATA_ONLY_REPLAY_SDK in labels and any(_PYTHON_RUNTIME_LABEL.fullmatch(label) for label in labels):
            return True
    return False


async def _resumable_native(request: Request, api_key: str, model: str, action: str, query: str, payload: bytes,
                            *, fingerprint: Optional[str] = None, allow_admit: bool = True) -> Optional[Response]:
    if fingerprint is None:
        fingerprint = native_replay_fingerprint(model, action, query, payload)
    try:
        resolved = await resolve_buffer(resumable_store, request, api_key, fingerprint, model=model,
                                        kind="native", allow_admit=allow_admit)
    except (ResumeRequestError, BufferCapacityError) as exc:
        return _native_error(exc.code, str(exc))
    if resolved is None:
        return None  # a lookup cannot allocate a buffer for a fresh request
    buf, new, start, _source = resolved

    async def close_response(response: Response):
        # getattr 默认值不能急切求值：传入无 body_iterator 的 Response 时
        # 会在判定前就抛 AttributeError（同类代码 streaming.py 已有保护）。
        close = getattr(response, "aclose", None) or response.body_iterator.aclose
        with anyio.CancelScope(shield=True):
            await close()

    async def pump(response: Response):
        terminal_error = False
        try:
            async for frame in response.body_iterator:
                text = frame.decode("utf-8") if isinstance(frame, bytes) else frame
                event = ReplayEvent(text)
                if not event.has_data:
                    continue
                if await buf.append(buf.event_frame(event)) is None:
                    return
                error = event.error()
                if error:
                    code = error.get("code") if isinstance(error, dict) else None
                    await buf.finish("error", error=error, status_code=(
                        code if isinstance(code, int) and 400 <= code <= 599 else 502))
                    terminal_error = buf.status == "error" and buf.status_code != 499
                    return
            await buf.finish()
        except (asyncio.CancelledError, TimeoutError):
            raise  # the deadline owner preserves upstream vs total timeout
        except Exception as exc:
            await buf.fail(str(exc), getattr(exc, "code", 502), within_deadline=True)
        finally:
            with anyio.CancelScope(shield=True):
                iterator = response.body_iterator
                close_terminal = getattr(iterator, "aclose_terminal", None)
                if terminal_error and close_terminal is not None:
                    # The buffered error is terminal even if no reader remains
                    # to observe it. Close the original lease as error, not cancel.
                    await close_terminal("error")
                else:
                    await iterator.aclose()

    if new:
        async def setup():
            try:
                response = await _passthrough_request(
                    DetachedRequest(request), model, action, query, payload)
                if not isinstance(response, StreamingResponse):
                    if 400 <= response.status_code <= 599:
                        hint = response_retry_after(response)
                        if hint is None and "retry-after" in response.headers:
                            del response.headers["retry-after"]
                        await buf.fail_native_setup(_native_setup_payload(response), hint)
                    else:
                        message = response.body.decode("utf-8", "replace")[:1024]
                        await buf.fail(message, response.status_code, within_deadline=True)
                    return response
                if buf.done:
                    await close_response(response)
                    return _native_error(buf.status_code, buf.error or "响应缓冲已释放。")
                task = hold_ingress(asyncio.create_task(within_request_deadline(buf, lambda: pump(response))))
                resumable_store.register_pump(buf.id, task, buffer=buf, close=lambda: close_response(response))
                return response
            except GenerationCapacityError as exc:
                # This buffer already has readers, but no provider was contacted.
                # Keep the local HTTP receipt and the original single native event.
                response = _native_error(exc.code, str(exc))
                await buf.fail_native_setup(_native_setup_payload(response), None, upstream=False)
                return response
            except TimeoutError:
                raise  # within_request_deadline classifies this before publishing
            except Exception as exc:
                await buf.fail(str(exc), getattr(exc, "code", 502), within_deadline=True)
                raise
        task = hold_ingress(asyncio.create_task(within_request_deadline(buf, setup)))
        resumable_store.register_pump(buf.id, task, buffer=buf)
        if await fast_setup_completed(buf, task, SETUP_FAST_WINDOW_SECONDS):
            response = buffered_setup_result(buf, task)
            if not isinstance(response, StreamingResponse):
                if buf.status_code == response.status_code and buf.done:
                    return _acknowledge_native_http(response, request, buf)
                return response
        else:
            # 慢路径统计补记（2026-11 P2-8 修复；单一实现见
            # admission.count_slow_setup_failure，与 chat_api 共用）。
            task.add_done_callback(partial(count_slow_setup_failure, on_error=stats.add_error))
    headers = {"X-Resume-Id": buf.id, **SSE_NO_BUFFER_HEADERS,
               **({"X-Resumable-Replay": "1"} if not new else {})}
    if buf.native_setup_headers is not None:
        headers.update(buf.native_setup_headers)
        if not new and not resume_cursor(request)[2]:
            # No stream has started on this connection. Preserve the HTTP result
            # for an undelivered retry or explicit Idempotency-Key. A real cursor
            # still addresses the numbered SSE event, not a brand-new HTTP result.
            return _acknowledge_native_http(
                JSONResponse(buf.payload, status_code=buf.status_code, headers=headers), request, buf)
    if _sdk_native_replay_view(request):
        return ReplayStreamResponse(request, buf, start, headers=headers, heartbeat_frame=GEMINI_HEARTBEAT_FRAME,
                                    render_frame=data_only_frame)
    return ReplayStreamResponse(request, buf, start, headers=headers)



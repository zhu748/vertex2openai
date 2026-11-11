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
import time
from functools import partial
from typing import Any, AsyncIterator, Callable, Optional

from urllib.parse import parse_qs

import anyio
import httpx
from fastapi import Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from request_ingress import hold_ingress

import config as app_config
from retry_policy import get_retry_settings, is_retryable_exception, is_retryable_status
from retry_policy import retry_delay as _retry_delay, wait_for_retry as _wait_for_retry
from upstream_errors import extract_upstream_error, google_status_detail
from usage import merge_usage_metadata
from response_validation import IncompleteResponseError, has_finish_reason
from admission import (count_slow_setup_failure, limit_generation, run_with_image_limit,
                       SETUP_FAST_WINDOW_SECONDS)
from logger import stats
from request_tracking import upstream_attempt, upstream_chunk
import first_reply as first_reply_module
from first_reply import FirstReplyGuard, native_has_reply
from json_stream import MAX_JSON_OBJECT_BYTES, JsonObjects
from http_io import buffer_upstream_response, read_upstream_bytes
from runtime_state import app_state
from express_endpoint import is_location_pin_failure
from express_fingerprint import build_direct_headers
from client_pool import passthrough_client_pool
from proxy_resolver import ProxyResolveError, resolve_proxy_uri
from proxy_failover import ProxyChain
from proxy_tls import build_httpx_proxy
from streaming import SSE_NO_BUFFER_HEADERS, sse_frames, with_heartbeats, HEARTBEAT, HEARTBEAT_FRAME
from resumable import (resumable_store, resolve_buffer, ResumeRequestError, BufferCapacityError,
                       DetachedRequest, sse_from_buffer, within_request_deadline,
                       request_timeout_seconds)
from upstreams.native_catalog import (
    _build_upstream_url,
    _resolve_upstream_model_path,
)

_UPSTREAM_TIMEOUT = httpx.Timeout(connect=30.0, read=600.0, write=60.0, pool=10.0)


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


async def _close_upstream(upstream: httpx.Response) -> None:
    # Starlette 用取消作用域终止断连请求；清理需要屏蔽该作用域的持续取消。
    with anyio.CancelScope(shield=True):
        await upstream.aclose()


async def _passthrough_once(client: httpx.AsyncClient, express_key: str,
                            model_path: str, action: str, query: str,
                            payload: bytes, *, stream: bool) -> httpx.Response:
    """向上游发一次请求。stream=True 时返回**未消费**的响应
    （供 aiter_bytes 逐块转发）；stream=False 时返回已缓冲的响应。

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
    response = await client.send(req, stream=True)
    if stream:
        return response
    try:
        return await buffer_upstream_response(response)
    finally:
        await _close_upstream(response)


@limit_generation
async def _passthrough_request(fastapi_request: Request, model: str, action: str,
                               query: str, payload: bytes):
    return await run_with_image_limit(
        model if action in ("generateContent", "streamGenerateContent") else "",
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
        await _close_upstream(upstream)
    return status, ctype, headers, body


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
      次数放大），更短的上游 retry-after 会被采纳；
    - 钉定失败兜底（403/404 且报错像项目/区域问题，见 _looks_like_pin_failure）：
      不换 Key、不占上面的预算，仅把模型路径退回裸模型名再试一次——
      与 SDK 通道（express_pipeline.execute_gemini_call 的 fallback_model）对齐，
      避免「OpenAI 接口能自动兜底、原生透传却把 404 原样抛给客户端」。
    """
    started_at = time.monotonic()
    deadline = started_at + request_timeout_seconds()
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
    # 真实默认 15 秒（本路由的外层限时较短，回归会直接暴露）。
    fr_seconds = first_reply_module.timeout_seconds(model)
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
            # 非流式透传是整取路径：不参与首回复计时（2026-11 语义，见
            # first_reply.timed_call）——窗口置 0 后 wait 直接放行，挂起由
            # 传输层读超时（read=600s）与整轮期限兜底；真流式保持窗口语义
            # （响应头到达 + prefetch 首个有效帧都在窗口内）。
            first_reply = FirstReplyGuard(
                model, seconds=fr_seconds if is_stream else 0.0)
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
                stream_state = _NativeStreamState(reply_guard=first_reply)
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
            if proxy_chain.advance(e):
                continue
            last_error_status, last_error_body, last_error_headers = _classify_attempt_error(e, upstream)
            if key_attempts >= retry_max or not is_retryable_exception(e):
                break
            wait = _retry_delay(last_error_headers.get("retry-after"), backoff_seconds, key_attempts,
                                jitter_seconds, error=e)
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
            last_error_status = upstream.status_code
            last_error_headers = {k: v for k, v in upstream.headers.items()
                                  if k.lower() == "retry-after"}
            # 读错误正文供重试诊断；取消和读取失败也必须释放响应。
            try:
                last_error_body = await read_upstream_bytes(upstream)
            except Exception:
                last_error_body = _native_error(
                    upstream.status_code, "上游错误响应未完整传输。").body
            finally:
                await _close_upstream(upstream)
            wait = _retry_delay(upstream.headers.get("retry-after"), backoff_seconds, key_attempts, jitter_seconds)
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
                and _looks_like_pin_failure(status, body)):
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
    def __init__(self, reply_guard: Optional[FirstReplyGuard]=None):
        self.reply_guard = reply_guard
        self.seen, self.finished = set(), set()
        self.received = False
        self.blocked = False
        # 累积快照按已出现的字段更新；尾帧仅含输出用量时保留先前输入用量。
        self.usage: Optional[dict] = None

    def inspect(self, payload: dict):
        for obj in payload if isinstance(payload, list) else [payload]:
            if not isinstance(obj, dict):
                raise IncompleteResponseError("上游事件不是 Gemini 响应对象。")
            self.received = True
            if obj.get("error"):
                raise NativeStreamError(obj)
            usage = obj.get("usageMetadata")
            if isinstance(usage, dict):
                self.usage = merge_usage_metadata(self.usage, usage)
            feedback = obj.get("promptFeedback")
            if isinstance(feedback, dict) and has_finish_reason(feedback.get("blockReason")):
                self.blocked = True
            if not isinstance(obj.get("candidates", []), list):
                raise IncompleteResponseError("上游 candidates 格式错误。")
            for index, candidate in enumerate(obj.get("candidates") or []):
                if not isinstance(candidate, dict):
                    raise IncompleteResponseError("上游候选格式错误。")
                content = candidate.get("content") or {}
                if not isinstance(content, dict):
                    raise IncompleteResponseError("上游候选内容格式错误。")
                parts = content.get("parts") or []
                if not isinstance(parts, list) or any(not isinstance(part, dict) for part in parts):
                    raise IncompleteResponseError("上游候选 parts 格式错误。")
                index = candidate.get("index", index)
                if not isinstance(index, int) or index < 0:
                    raise IncompleteResponseError("上游候选 index 格式错误。")
                if index in self.finished and parts:
                    raise IncompleteResponseError("上游候选已结束，却继续返回内容。")
                if parts:
                    self.seen.add(index)
                if has_finish_reason(candidate.get("finishReason")):
                    self.finished.add(index)
            if self.reply_guard and native_has_reply(obj):
                self.reply_guard.accept()

    def finish(self):
        if not self.received:
            raise IncompleteResponseError("上游流已结束，但未收到响应数据。")
        if self.seen - self.finished:
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
            for obj in parser.feed(decoder.decode(chunk)):
                state.inspect(obj)
            yield chunk
        for obj in parser.feed(decoder.decode(b"", final=True)):
            state.inspect(obj)
    except UnicodeError as exc:
        raise IncompleteResponseError("上游 JSON 流包含无效 UTF-8。") from exc
    parser.finish()
    state.finish()


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
            data = b"\n".join(line[5:].lstrip() for line in frame.splitlines() if line.startswith(b"data:"))
            if data and data != b"[DONE]":
                try:
                    payload = json.loads(data)
                except (ValueError, UnicodeError):
                    raise IncompleteResponseError("上游 SSE 包含无效 JSON 事件。") from None
                state.inspect(payload)
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
        try:
            remaining = (deadline or time.monotonic() + request_timeout_seconds()) - time.monotonic()
            async with asyncio.timeout(max(0.001, remaining)):
                if keepalive:
                    events = with_heartbeats(source if source is not None else _native_sse_frames(upstream))
                    yield HEARTBEAT_FRAME.encode()
                    async for item in events:
                        if item is HEARTBEAT:
                            yield HEARTBEAT_FRAME.encode()
                        else:
                            total += len(item)
                            yield item
                else:
                    if source is not None:
                        events = source
                    elif "text/event-stream" in upstream.headers.get("content-type", "").lower():
                        events = _native_sse_frames(upstream)
                    else:
                        events = _native_json_chunks(upstream)
                    async for chunk in events:
                        if chunk:
                            total += len(chunk)
                            yield chunk
            completed = True
        except Exception as exc:
            if not keepalive:
                raise
            code, _ = extract_upstream_error(exc)
            body = (exc.payload if isinstance(exc, NativeStreamError) else
                    _native_error(code, "上游流未完整传输，请重新生成或检查网络。").body)
            yield b"data: " + body + b"\n\n"
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

    class NativeStreamingResponse(StreamingResponse):
        async def aclose(self):
            with anyio.CancelScope(shield=True):
                try:
                    await self.body_iterator.aclose()
                finally:
                    if source is not None:
                        await source.aclose()
                    await _close_upstream(upstream)

        async def __call__(self, scope: dict, receive: Callable, send: Callable):
            try:
                await super().__call__(scope, receive, send)
            finally:
                # send() 可能在生成器第一次执行前或暂停于 yield 时失败；
                # 两种情况下都由响应外层负责关闭，不能仅依赖生成器 finally。
                with anyio.CancelScope(shield=True):
                    if not started:
                        stats.add_error()
                    await self.aclose()

    media_type = upstream.headers.get("content-type", "text/event-stream")
    passthrough = {k: v for k, v in upstream.headers.items()
                   if k.lower() == "retry-after"}
    if keepalive:
        passthrough.update(SSE_NO_BUFFER_HEADERS)
    return NativeStreamingResponse(_relay(), status_code=upstream.status_code,
                                   media_type=media_type, headers=passthrough or None)


async def _resumable_native(request: Request, api_key: str, model: str, action: str, query: str, payload: dict, parsed: dict):
    # 指纹用原始请求体的 sha256：旧实现把已解析的 payload（可达 32MiB，
    # 含 base64 图片）重新 json.dumps 一遍，大请求数百毫秒纯 CPU。
    # 语义同步收窄为「原样重发命中同一缓冲」（与 chat_api 指纹的注释口径
    # 一致）；同一客户端重发的字节序列相同，命中行为不变。
    fingerprint = "native:" + json.dumps(
        [model, action, sorted(parse_qs(query).items()),
         hashlib.sha256(payload).hexdigest()], sort_keys=True,
        ensure_ascii=False, separators=(",", ":"))
    try:
        buf, new, start, _source = await resolve_buffer(
            resumable_store, request, api_key, fingerprint, model=model, kind="native")
    except (ResumeRequestError, BufferCapacityError) as exc:
        return _native_error(exc.code, str(exc))

    async def close_response(response: Response):
        # getattr 默认值不能急切求值：传入无 body_iterator 的 Response 时
        # 会在判定前就抛 AttributeError（同类代码 streaming.py 已有保护）。
        close = getattr(response, "aclose", None) or response.body_iterator.aclose
        with anyio.CancelScope(shield=True):
            await close()

    async def pump(response: Response):
        failed = False
        try:
            async for frame in response.body_iterator:
                text = frame.decode("utf-8") if isinstance(frame, bytes) else frame
                if text.lstrip().startswith(":"):
                    continue
                for line in text.splitlines():
                    if line.startswith("data:"):
                        try:
                            obj = json.loads(line[5:])
                        except ValueError:
                            continue
                        if isinstance(obj, dict) and obj.get("error"):
                            error = obj["error"]
                            await buf.append(buf.event_frame(text))
                            code = error.get("code") if isinstance(error, dict) else None
                            await buf.finish("error", error=str(error), status_code=(
                                code if isinstance(code, int) and 400 <= code <= 599 else 502))
                            failed = True
                            return
                if await buf.append(buf.event_frame(text)) is None:
                    return
            if not failed:
                await buf.finish()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await buf.fail(str(exc), getattr(exc, "code", 502))
        finally:
            with anyio.CancelScope(shield=True):
                await response.body_iterator.aclose()

    if new:
        async def setup():
            try:
                response = await _passthrough_request(
                    DetachedRequest(request), model, action, query, payload)
                if not isinstance(response, StreamingResponse):
                    message = response.body.decode("utf-8", "replace")[:1024]
                    await buf.fail(message, response.status_code)
                    return response
                if buf.done:
                    await close_response(response)
                    return _native_error(buf.status_code, buf.error or "响应缓冲已释放。")
                task = hold_ingress(asyncio.create_task(within_request_deadline(buf, lambda: pump(response))))
                task.add_done_callback(lambda _task: resumable_store.schedule_cleanup(lambda: close_response(response)))
                resumable_store.register_pump(buf.id, task, buffer=buf)
                return response
            except Exception as exc:
                await buf.fail(str(exc), getattr(exc, "code", 502))
                raise
        task = hold_ingress(asyncio.create_task(within_request_deadline(buf, setup)))
        resumable_store.register_pump(buf.id, task, buffer=buf)
        done, _ = await asyncio.wait({task}, timeout=SETUP_FAST_WINDOW_SECONDS)
        if done:
            response = task.result()
            if not isinstance(response, StreamingResponse):
                return response
        else:
            # 慢路径统计补记（2026-11 P2-8 修复；单一实现见
            # admission.count_slow_setup_failure，与 chat_api 共用）。
            task.add_done_callback(partial(count_slow_setup_failure, on_error=stats.add_error))
    return StreamingResponse(
        sse_from_buffer(buf, start), media_type="text/event-stream",
        headers={"X-Resume-Id": buf.id, **SSE_NO_BUFFER_HEADERS,
                 **({"X-Resumable-Replay": "1"} if not new else {})})



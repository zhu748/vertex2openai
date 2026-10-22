"""Gemini 原生 API 格式透传路由（/v1beta & /v1 双前缀）。

为什么需要它：本服务此前只暴露 OpenAI 兼容接口（/v1/chat/completions），
而 Gemini 原生生态（Google AI SDK、Cherry Studio、LobeChat、各种支持
Gemini 直连的客户端）说的是另一套协议：

    POST /v1beta/models/{model}:generateContent      （非流式）
    POST /v1beta/models/{model}:streamGenerateContent?alt=sse （SSE 流式）
    POST /v1beta/models/{model}:countTokens
    GET  /v1beta/models                              （模型列表）

本模块把这些端点**原样透传**到 Gemini Express（api_key）端点，不做任何
字段转换——tools / function calling / thinkingConfig / safetySettings /
cachedContent / responseSchema 等字段保持原请求体，兼容性取决于上游模型与端点；
认证从本服务 Key 替换为 Express Key。

实现要点：
- 认证兼容三种原生写法：Authorization: Bearer、x-goog-api-key、?key=
  （值必须等于本服务的 API_KEY，否则 401）。
- Key 池轮询/随机、location 钉定、出站代理全部复用现有 Express 通道逻辑。
- 模型路径拼接对齐 SDK 的 t_model()：完整资源路径（projects/ 等开头）
  原样透传，不再叠加 publishers 前缀（旧版双重前缀会让上游直接返回
  404 空 body，客户端只能显示 Unknown error: 404）。
- 上游 URL 钉 v1beta1（SDK 在 vertex 模式的真实默认 api_version；写死
  v1 会让流式请求拿到非 SSE 载荷，客户端无限转圈且无任何报错）。
- 出站可观测性：上游响应摘要日志（状态码 + content-type，
  debug_outbound 时附完整 URL）、SSE 形态异常告警、流式转发完成/中止日志。
- 上游响应（含错误）状态码与 body 原样转发；流式以原始字节流转发 SSE。
- 429/5xx 按控制台次数与间隔重新选 Key 重试，单 Key / 池轮转后仍可重试；钉定路径失败
  （404 Publisher model not found / 403 requires billing）自动退回
  裸模型名重试一次，与 OpenAI 兼容通道的 fallback 行为对齐。
- 模型列表用内部模型清单映射成 Gemini 原生格式（models/get 也支持）。

限制：Cookie 通道（batchGraphql）是私有 GraphQL 协议，无法承载原生 REST
请求，故本透传始终走 Express 通道；未配置任何 Express Key 时返回明确错误。
"""

import json
import asyncio
import codecs
import hashlib
import time
from typing import Any, Optional
from urllib.parse import parse_qs, quote

import anyio
import httpx
from fastapi import APIRouter, Depends, Header, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from retry_policy import get_retry_settings, is_retryable_exception, is_retryable_status
from upstream_errors import extract_upstream_error, google_status_detail
from response_validation import IncompleteResponseError, has_finish_reason
from admission import limit_generation, GenerationCapacityError
from retry_policy import retry_delay as _retry_delay, wait_for_retry as _wait_for_retry
from auth import constant_time_equal
from config import API_KEY
from logger import stats
from request_tracking import configure as track_configure, upstream_attempt, upstream_chunk
# 别名导入：_passthrough_request 内有同名局部变量 first_reply（guard 实例），
# 直接 import first_reply 会被局部变量遮蔽成 UnboundLocalError。
import first_reply as first_reply_module
from first_reply import FirstReplyGuard, native_has_reply
from json_stream import MAX_JSON_OBJECT_BYTES, JsonObjects
from streaming import prepare_sse
from runtime_state import app_state
import config as app_config
from express_endpoint import resolve_express_model_path, is_location_pin_failure
from model_loader import get_express_models
from express_fingerprint import build_direct_headers
from client_pool import passthrough_client_pool
from proxy_resolver import ProxyResolveError, resolve_proxy_uri
from proxy_failover import ProxyChain
from streaming import sse_frames, with_heartbeats, HEARTBEAT, HEARTBEAT_FRAME
from resumable import (resumable_store, resolve_buffer, ResumeRequestError, BufferCapacityError,
                       DetachedRequest, sse_from_buffer, within_request_deadline,
                       request_timeout_seconds, is_enabled as resumable_enabled)

router = APIRouter()

# Express 端点（与 SDK 解析结果一致，见 express_sdk._log_resolved_endpoint）
EXPRESS_BASE = app_config.VERTEX_BASE_URL or "https://aiplatform.googleapis.com"
# ⚠️ 必须钉 v1beta1：google-genai SDK 在 vertex 模式下的默认 api_version 就是
# v1beta1（2.31.0 _api_client.py 的 vertexai 分支；MockTransport 实测流式 URL
# 为 …/v1beta1/publishers/google/models/{m}:streamGenerateContent?alt=sse）。
# 此前写死 v1 是错的：① SDK 通道（OpenAI 格式）落在 v1beta1、透传落在 v1，
# 同一服务的出站形态自相矛盾；② SDK 从不产生 /v1/ 的 URL，自称
# google-genai-sdk 却请求 /v1/ 是指纹层面的自曝；③ v1 端点的
# streamGenerateContent 对 alt=sse 的行为与 v1beta1 不一致——实测症状：
# 客户端无限转圈且服务端无任何报错（上游 200 但非 SSE 载荷，SSE 解析器
# 等不到 data: 行），而 OpenAI 格式同 Key 同模型一切正常。
EXPRESS_API_VERSION = "v1beta1"

# 支持透传的 :action 后缀
_SUPPORTED_ACTIONS = {"generateContent", "streamGenerateContent", "countTokens", "embedContent"}

# 透传请求体体积上限：原始 bytes 在解析、指纹、重试全程被持有，不设限
# 等于允许认证后的内存放大。32MiB 远大于任何正常请求（含数张 4K 图的
# base64）；先查 Content-Length 头再复查实际长度（防谎报头的绕过）。
MAX_NATIVE_BODY_BYTES = 32 * 1024 * 1024

# 上游透传客户端超时：流式长思考可能持续数分钟，read 放宽。
_UPSTREAM_TIMEOUT = httpx.Timeout(connect=30.0, read=600.0, write=60.0, pool=10.0)


# ============================================================
# 认证：兼容 Bearer / x-goog-api-key / ?key= 三种原生写法
# ============================================================
async def _require_native_key(request: Request, key: Optional[str]) -> bool:
    """校验客户端凭据是否等于本服务 API_KEY（常量时间比较）。"""
    if not key:
        return False
    return bool(API_KEY) and constant_time_equal(str(key), API_KEY)


async def get_gemini_auth(
    request: Request,
    authorization: Optional[str] = Header(None, include_in_schema=False),
    x_goog_api_key: Optional[str] = Header(None, include_in_schema=False),
):
    """Gemini 原生端点鉴权依赖：三种取 Key 方式任选其一。"""
    bearer = ""
    if authorization and authorization.startswith("Bearer "):
        bearer = authorization[len("Bearer "):]
    candidate = x_goog_api_key or bearer or request.query_params.get("key", "")
    if not await _require_native_key(request, candidate):
        # 错误格式对齐 Google 风格，原生客户端能正常展示
        raise NativeAPIError(_native_error(
            401,
            "API key not valid. Please pass a valid API key.",
            status_detail="UNAUTHENTICATED",
        ))
    return candidate


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


def _native_model_entry(model_id: str) -> dict:
    """Gemini 原生 models.list / models.get 共用的目录条目。"""
    return {
        "name": f"models/{model_id}",
        "displayName": model_id,
        "description": f"Vertex Express 模型（经 vertex2openai 透传）：{model_id}",
        "inputTokenLimit": 1048576,
        "outputTokenLimit": 65536,
        "supportedGenerationMethods": ["generateContent", "streamGenerateContent", "countTokens"],
    }


# ============================================================
# 模型名解析
# ============================================================
def _split_model_action(model_action: str) -> tuple[str, str]:
    """把 `gemini-3.6-pro:generateContent` 拆成 (model, action)。

    模型名本身不含冒号（Gemini 命名规范），第一个冒号即为分隔符。
    客户端若传了 `models/gemini-xxx:action` 形式，也一并剥掉前缀。
    """
    name = model_action
    if name.startswith("models/"):
        name = name[len("models/"):]
    if ":" in name:
        model, _, action = name.partition(":")
        return model.strip(), action.strip()
    return name.strip(), ""


def _resolve_upstream_model_path(model: str, key_project_id: str,
                                 settings: dict | None = None) -> str:
    """复用 Express 通道的 location 钉定规则。

    settings 可由调用方传入请求级快照：重试循环里逐次调用时免掉
    get_settings() 的全量深拷贝（retry_max 上限 50，最坏一次请求 51 次）。
    """
    if settings is None:
        settings = app_state.get_settings()
    return resolve_express_model_path(model, settings, key_project_id=key_project_id)


# ============================================================
# 上游转发核心
# ============================================================
def _build_upstream_url(model_path: str, action: str) -> str:
    """把模型路径拼成上游 URL，规则对齐 google-genai SDK 的 t_model()：

    - 裸模型名 → publishers/google/models/{model}（Express 端点格式）；
    - 完整资源路径（projects/、publishers/、models/、tunedModels/、
      endpoints/ 开头——包括 location 钉定生成的
      projects/{p}/locations/{l}/publishers/google/models/{m} 与客户端
      自带的完整路径）→ 原样拼接，**不能再叠 publishers 前缀**。

    修复前的 bug：这里无条件拼 publishers/google/models/，location 钉定
    生效时（express_location 默认 global）会请求
    …/v1/publishers/google/models/projects/… 双重前缀的畸形 URL，
    Google 前端直接返回 404 空 body（客户端只能显示 Unknown error: 404）
    ——这正是「OpenAI 兼容接口正常、Gemini 原生透传一律 404」的根因
    （OpenAI 路径走 SDK，t_model() 对 projects/ 开头原样透传，URL 正确）。
    """
    if model_path.startswith(("projects/", "publishers/", "models/",
                              "tunedModels/", "endpoints/")):
        model_segment = model_path
    else:
        model_segment = f"publishers/google/models/{model_path}"
    return f"{EXPRESS_BASE.rstrip('/')}/{EXPRESS_API_VERSION}/{model_segment}:{action}"


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
    return await client.send(req, stream=stream)


def _looks_like_pin_failure(status_code: int, body: bytes) -> bool:
    """仅项目/模型权限错误允许切回裸模型路径，复用 SDK 判据。"""
    return status_code in (403, 404) and is_location_pin_failure(body.decode("utf-8", "ignore"))


async def _close_upstream(upstream: httpx.Response) -> None:
    # Starlette 用取消作用域终止断连请求；清理需要屏蔽该作用域的持续取消。
    with anyio.CancelScope(shield=True):
        await upstream.aclose()


@limit_generation
async def _passthrough_request(fastapi_request: Request, model: str, action: str,
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
      与 SDK 通道（api_helpers.execute_gemini_call 的 fallback_model）对齐，
      避免「OpenAI 接口能自动兜底、原生透传却把 404 原样抛给客户端」。
    """
    started_at = time.monotonic()
    deadline = started_at + request_timeout_seconds()
    manager = fastapi_request.app.state.express_key_manager
    if manager.get_total_keys() == 0:
        return _native_error(
            403,
            "该透传端点需要至少一个 Express API Key（控制台「通道与凭证」页添加，"
            "或设环境变量 VERTEX_EXPRESS_API_KEY）。Cookie 直连通道是私有 GraphQL 协议，"
            "无法承载 Gemini 原生 REST 请求。",
            status_detail="PERMISSION_DENIED",
        )

    last_error_body = b""
    last_error_status = 502
    last_error_headers: dict[str, str] = {}

    def _new_client(proxy_raw: str = "") -> httpx.AsyncClient:
        """按指定代理（原始 URI）建透传客户端。

        调用前池签名已对同一 URI resolve 过一次（命中缓存），
        这里的 resolve 必然拿到同一结果，不会二次启动 sing-box。
        """
        client_kwargs: dict[str, Any] = {"timeout": _UPSTREAM_TIMEOUT, "follow_redirects": False, "trust_env": False}
        proxy_url = resolve_proxy_uri(proxy_raw)
        if proxy_url:
            client_kwargs["proxy"] = proxy_url
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
            def client_for(raw, resolved):
                return passthrough_client_pool.get(lambda: _new_client(raw), raw)
            try:
                # 预热（sing-box 首启放线程池）；随后池签名/工厂里的同步
                # resolve 命中缓存，不卡事件循环。
                await proxy_chain.prepare(client_for)
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
            first_reply = FirstReplyGuard(model, seconds=fr_seconds)
            async def send(raw, resolved):
                return await _passthrough_once(
                    client_for(raw, resolved), express_key, model_path, action, query, payload,
                    stream=is_stream)
            upstream = await first_reply.wait(proxy_chain.run(send))
            if not is_stream:
                upstream_chunk()
                if upstream.status_code < 400:
                    first_reply.accept()
            elif upstream.status_code < 400:
                is_sse = is_sse_request
                if "text/event-stream" in upstream.headers.get("content-type", "").lower():
                    source = _native_sse_frames(upstream, reply_guard=first_reply)
                    prefetched = await first_reply.prefetch(source, lambda _frame: first_reply.ready)
                elif not is_sse:
                    source = _native_json_chunks(upstream, first_reply)
                    # A native JSON object may contain a whole generated image.
                    # Its partial raw bytes are response data, not SSE metadata.
                    prefetched = (await first_reply.prefetch(source, lambda _chunk: first_reply.ready,
                                                           max_prefix_bytes=MAX_JSON_OBJECT_BYTES)
                                  if first_reply.seconds else source)
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
            last_error_headers = {k: v for k, v in upstream.headers.items()
                                  if k.lower() == "retry-after"} if upstream is not None else {}
            if isinstance(e, NativeStreamError):
                last_error_status, last_error_body = e.code, e.payload
            elif isinstance(e, (TimeoutError, httpx.TimeoutException)):
                last_error_status = 504
                last_error_body = _native_error(504, str(e) or "Upstream timeout", "DEADLINE_EXCEEDED").body
            elif isinstance(e, IncompleteResponseError):
                last_error_status = 502
                last_error_body = _native_error(502, str(e), "UNAVAILABLE").body
            else:
                last_error_status = 502
                last_error_body = _native_error(502, f"Upstream connection failed ({type(e).__name__})", "UNAVAILABLE").body
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
                last_error_body = await upstream.aread()
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
                deadline=deadline, source=prefetched)

        # 错误（或非流式响应）：缓冲 body 再透传，顺带供钉定失败判定
        status = upstream.status_code
        ctype = upstream.headers.get("content-type", "application/json")
        # retry-after 是客户端退避的关键依据，一并透传
        headers = {k: v for k, v in upstream.headers.items()
                   if k.lower() == "retry-after"}
        try:
            body = await upstream.aread()
        except Exception:
            # 已知的 HTTP 错误码仍保留，但空正文不能冒充完整错误响应。
            status = status if status >= 400 else 502
            body = _native_error(status, "上游响应未完整传输，请重试。").body
            ctype = "application/json"
        finally:
            await _close_upstream(upstream)

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
        return Response(content=body, status_code=status, media_type=ctype,
                        headers=headers or None)

    return Response(
        content=last_error_body or _native_error(last_error_status, "Upstream unreachable").body,
        status_code=last_error_status,
        media_type="application/json",
        headers=last_error_headers or None,
    )


class NativeStreamError(RuntimeError):
    def __init__(self, payload):
        self.payload = json.dumps(payload, ensure_ascii=False).encode()
        error = payload.get("error")
        code = error.get("code") if isinstance(error, dict) else None
        self.code = code if isinstance(code, int) and 400 <= code <= 599 else 502
        super().__init__(str(error))


class _NativeStreamState:
    """One completion/error contract for native SSE and JSON array streams."""
    def __init__(self, reply_guard=None):
        self.reply_guard = reply_guard
        self.seen, self.finished = set(), set()
        self.received = False
        self.blocked = False

    def inspect(self, payload):
        for obj in payload if isinstance(payload, list) else [payload]:
            if not isinstance(obj, dict):
                raise IncompleteResponseError("上游事件不是 Gemini 响应对象。")
            self.received = True
            if obj.get("error"):
                raise NativeStreamError(obj)
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


async def _native_json_chunks(upstream, reply_guard=None):
    """Validate complete JSON objects in linear time, preserving every raw byte."""
    decoder = codecs.getincrementaldecoder("utf-8")()
    parser = JsonObjects(document=True)
    state = _NativeStreamState(reply_guard)
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


async def _native_sse_frames(upstream, reply_guard=None):
    state = _NativeStreamState(reply_guard)
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


async def _relay_stream(upstream: httpx.Response, *, keepalive=False, deadline=None, source=None) -> StreamingResponse:
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
            if completed:
                stats.add_success()
                print(f"📤 [Gemini 透传] 流式转发完成（共 {total} 字节）。")
            else:
                # 响应头已发送为 200，中间件无法根据状态码捕获流中失败。
                stats.add_error()
                print(f"✂️ [Gemini 透传] 流式中止（已转发 {total} 字节；"
                      "客户端断开或上游/网络异常）。")
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

        async def __call__(self, scope, receive, send):
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
        passthrough.update({"Cache-Control": "no-store, no-transform", "X-Accel-Buffering": "no"})
    return NativeStreamingResponse(_relay(), status_code=upstream.status_code,
                                   media_type=media_type, headers=passthrough or None)


# ============================================================
# 端点：generateContent / streamGenerateContent / countTokens / embedContent
# ============================================================
async def _resumable_native(request, api_key, model, action, query, payload, parsed):
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

    async def close_response(response):
        # getattr 默认值不能急切求值：传入无 body_iterator 的 Response 时
        # 会在判定前就抛 AttributeError（同类代码 streaming.py 已有保护）。
        close = getattr(response, "aclose", None) or response.body_iterator.aclose
        with anyio.CancelScope(shield=True):
            await close()

    async def pump(response):
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
                task = asyncio.create_task(within_request_deadline(buf, lambda: pump(response)))
                task.add_done_callback(lambda _task: resumable_store.schedule_cleanup(lambda: close_response(response)))
                resumable_store.register_pump(buf.id, task, buffer=buf)
                return response
            except Exception as exc:
                await buf.fail(str(exc), getattr(exc, "code", 502))
                raise
        task = asyncio.create_task(within_request_deadline(buf, setup))
        resumable_store.register_pump(buf.id, task, buffer=buf)
        done, _ = await asyncio.wait({task}, timeout=0.05)
        if done:
            response = task.result()
            if not isinstance(response, StreamingResponse):
                return response
    return StreamingResponse(
        sse_from_buffer(buf, start), media_type="text/event-stream",
        headers={"X-Resume-Id": buf.id, "Cache-Control": "no-store, no-transform",
                 "X-Accel-Buffering": "no", **({"X-Resumable-Replay": "1"} if not new else {})})


def _passthrough_endpoint():
    """为 /v1beta 与 /v1 两个前缀注册同一个透传处理。

    注：POST /v1/models/{model}:generateContent 与 OpenAI 端点的
    GET /v1/models 不冲突（方法与路径均不同）；而 Gemini 原生的
    GET /v1/models 模型列表**只注册 /v1beta 前缀**——/v1/models 是
    OpenAI 生态的既有格式，不能破坏。
    """

    async def endpoint(model_action: str, request: Request,
                       _auth: str = Depends(get_gemini_auth)):
        model, action = _split_model_action(model_action)
        if not model:
            return _native_error(400, "模型名不能为空。")
        if action not in _SUPPORTED_ACTIONS:
            return _native_error(
                404,
                f"Unsupported method :{action or '(empty)'}。支持的操作："
                + ", ".join(sorted(_SUPPORTED_ACTIONS)) + "。",
                status_detail="NOT_FOUND",
            )

        # body 校验必须是合法 JSON 对象（Gemini 请求全是 JSON 对象），
        # 但**转发原始字节**而不是解析重组——客户端的 SDK 把 body 序列化
        # 成什么样（分隔符、键序、空白），上游收到的就是什么样，与
        # “客户端直连 Google”的形态完全一致（反代最忌讳的就是重组后的
        # body 与自称的 SDK 形态对不上号）。
        # 体积上限：解析、指纹、重试全程持有原始 bytes，不设限等于允许
        # 认证后的内存放大（32MiB 远大于任何正常请求，含数张 4K 图）。
        declared_length = request.headers.get("content-length", "")
        if declared_length.isdigit() and int(declared_length) > MAX_NATIVE_BODY_BYTES:
            return _native_error(413, f"请求体超过 {MAX_NATIVE_BODY_BYTES // (1024 * 1024)}MiB 上限。")
        raw_body = await request.body()
        if len(raw_body) > MAX_NATIVE_BODY_BYTES:
            return _native_error(413, f"请求体超过 {MAX_NATIVE_BODY_BYTES // (1024 * 1024)}MiB 上限。")
        try:
            parsed = json.loads(raw_body)
        except Exception:
            return _native_error(400, "请求体必须是 JSON 对象（Gemini 原生格式）。")
        if not isinstance(parsed, dict):
            return _native_error(400, "请求体必须是 JSON 对象（Gemini 原生格式）。")

        # 透传 query（alt=sse 等），但剔除本服务鉴权用的 key 参数。
        # query_params 取出的是已解码值，重新下发前必须再编码，否则值里
        # 带 & / = / 空格会把 URL 拼坏（alt=sse 之外的参数被截断）。
        qp = [(k, v) for k, v in request.query_params.multi_items() if k not in ("key", "resume_from")]
        query = "&".join(f"{quote(k, safe='')}={quote(v, safe='')}" for k, v in qp)

        is_stream = action == "streamGenerateContent"
        track_configure(model=model, protocol="gemini", channel="express", stream=is_stream,
                        mode="stream" if is_stream else "json")
        print(f"🔁 [Gemini 透传] {model}:{action} | {'流式' if is_stream else '非流式'}"
              f" | 原生协议透传（Express 通道）")
        try:
            sse = is_stream and "sse" in parse_qs(query).get("alt", [])
            # 续传识别与 chat 通道（resume_cursor）同口径：x-resumable 头、
            # X-Resume-Id、Last-Event-ID（带生成 ID 的 rid 游标或裸数字序号）、
            # ?resume_from=。旧实现漏掉后两者，?resume_from= 会被当全新请求
            # 重复烧上游配额（该参数 791 行已从转发 query 剥离，表明本就支持）。
            resume = (request.headers.get("x-resumable", "").lower() in ("1", "true", "yes")
                      or request.headers.get("x-resume-id")
                      or bool(request.headers.get("last-event-id", "").strip())
                      or bool(request.query_params.get("resume_from", "").strip()))
            if resume and not sse:
                return _native_error(400, "原生续传需要 streamGenerateContent?alt=sse。")
            if sse and resume and resumable_enabled():
                return await _resumable_native(request, _auth, model, action, query, raw_body, parsed)
            async def execute():
                async with asyncio.timeout(request_timeout_seconds()):
                    return await _passthrough_request(request, model, action, query, raw_body)

            def error_body(result):
                if isinstance(result, Exception):
                    code, message = extract_upstream_error(result)
                    return _native_error(code, message).body
                try:
                    if json.loads(result.body).get("error"):
                        return result.body
                except (ValueError, TypeError, AttributeError):
                    pass
                code = result.status_code if result.status_code >= 400 else 502
                return _native_error(code, f"上游返回非 SSE 响应（HTTP {result.status_code}）。").body

            if sse:
                return await prepare_sse(execute, request, error_body, stats.add_error)
            return await execute()
        except TimeoutError:
            return _native_error(504, "本轮请求超过总执行期限。")
        except GenerationCapacityError as exc:
            return _native_error(503, str(exc))
        except Exception as exc:
            code, message = extract_upstream_error(exc)
            return _native_error(code, message[:1024])

    return endpoint


for _prefix in ("/v1beta", "/v1"):
    router.add_api_route(
        f"{_prefix}/models/{{model_action:path}}",
        _passthrough_endpoint(),
        methods=["POST"],
        name=f"gemini_native_passthrough{_prefix.replace('/', '_')}",
    )


# ============================================================
# 端点：模型列表（GET /v1beta/models、GET /v1beta/models/{model}）
# ⚠️ 只注册 /v1beta：GET /v1/models 是 OpenAI 生态的既有端点（models_api），
#    注册两份会因路由顺序导致其中一份被永久遮蔽。
# ============================================================
@router.get("/v1beta/models")
async def native_list_models(fastapi_request: Request,
                             _auth: str = Depends(get_gemini_auth)):
    """把内部模型清单映射成 Gemini 原生 models.list 响应。"""
    manager = fastapi_request.app.state.express_key_manager
    has_any = manager.get_total_keys() > 0 or app_state.get_channel_mode() in ("web_proxy", "mixed")
    raw = await get_express_models() if has_any else []

    models = [_native_model_entry(mid) for mid in sorted(raw)]
    return {"models": models}


@router.get("/v1beta/models/{model_action}")
async def native_get_model(model_action: str, fastapi_request: Request,
                           _auth: str = Depends(get_gemini_auth)):
    # 与 native_list_models 一致：未配置任何通道时不返回模型详情
    # （旧实现缺这个检查，无 Key 也照常返回目录条目，与列表行为矛盾）。
    manager = fastapi_request.app.state.express_key_manager
    has_any = manager.get_total_keys() > 0 or app_state.get_channel_mode() in ("web_proxy", "mixed")
    model, action = _split_model_action(model_action)
    if action:
        return _native_error(400, f"GET 不支持 :{action}，请用 POST。")
    raw = await get_express_models() if has_any else []
    if model and model in raw:
        return _native_model_entry(model)
    return _native_error(404, f"Model {model} not found.", status_detail="NOT_FOUND")

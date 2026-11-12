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

通道选择（尊重控制台的通道模式开关）：api_key / mixed 模式下有 Express Key 时**字节级
透传**（上述保真要点全部成立）；没有 Key 但配置了 Cookie 账号时，generateContent /
streamGenerateContent 改走 Cookie 通道**协议级桥接**（upstreams/cookie_native.py：原生
JSON → batchGraphql 转码、响应重组回原生形状，countTokens / embedContent 不支持）。
web_proxy（Cookie 模式）下即使同时存在 Express Key，generateContent /
streamGenerateContent 也优先走 Cookie 桥接（旧实现只看"有没有 Key"，Cookie 模式被
Key 霸占）；Cookie 桥接不支持的操作在有 Key 时回退 Key 透传。两者皆无时返回明确错误。
"""

import asyncio
import json
from typing import Optional
from urllib.parse import parse_qs, quote

from fastapi import APIRouter, Depends, Header, Request

from admission import GenerationCapacityError
from auth import constant_time_equal, extract_candidate_key
from config import API_KEY
from logger import stats
from model_loader import get_express_models
from request_errors import MAX_GENERATION_BODY_BYTES, declared_body_exceeds
from request_tracking import configure as track_configure
from runtime_state import app_state
from streaming import prepare_sse
from upstream_errors import extract_upstream_error
from resumable import (image_bypass_enabled, request_timeout_seconds,
                       is_enabled as resumable_enabled)
router = APIRouter()

# 支持透传的 :action 后缀
_SUPPORTED_ACTIONS = {"generateContent", "streamGenerateContent", "countTokens", "embedContent"}

# 透传请求体体积上限：原始 bytes 在解析、指纹、重试全程被持有，不设限
# 等于允许认证后的内存放大。32MiB 远大于任何正常请求（含数张 4K 图的
# base64）；先查 Content-Length 头再复查实际长度（防谎报头的绕过）。
MAX_NATIVE_BODY_BYTES = MAX_GENERATION_BODY_BYTES  # QUA-P3-1：单一来源别名

# ============================================================
# 兼容 re-export（QUA-P2-9 拆分后测试 patch seam 的稳定面）
# ============================================================
# 目录 / 变体 / URL 层与透传编排引擎已分别迁至 upstreams/native_catalog.py
# 与 upstreams/native_passthrough.py；本模块保留鉴权、端点装配与 models
# 路由。经 patch.object(gemini_native, ...) 重绑模块属性、或直接调用
# gemini_native._xxx 的既有测试/脚本不受影响（引擎内部相互调用已指向
# 新模块全局——对引擎内部行为的 patch 需打到对应新模块上）。
from upstreams.native_catalog import (  # noqa: F401
    EXPRESS_API_VERSION,
    EXPRESS_BASE,
    _build_upstream_url,
    _native_catalog_ids,
    _native_model_entry,
    _resolve_upstream_model_path,
    _split_model_action,
    _strip_image_size_variant,
    _strip_search_variant,
)
from upstreams.native_passthrough import (  # noqa: F401
    NativeAPIError,
    NativeStreamError,
    _NativeStreamState,
    _close_upstream,
    _log_native_usage,
    _native_body_usage,
    _native_error,
    _native_json_chunks,
    _native_sse_frames,
    _passthrough_once,
    _passthrough_request,
    _passthrough_request_impl,
    _relay_stream,
    _resumable_native,
)
from upstreams.native_passthrough import _UPSTREAM_TIMEOUT  # noqa: F401


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
    candidate = extract_candidate_key(
        request, authorization=authorization or "",
        x_goog_api_key=x_goog_api_key or "")
    if not await _require_native_key(request, candidate):
        # 错误格式对齐 Google 风格，原生客户端能正常展示
        raise NativeAPIError(_native_error(
            401,
            "API key not valid. Please pass a valid API key.",
            status_detail="UNAUTHENTICATED",
        ))
    return candidate



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
        # 路径归一化逃逸加固（2026-11 S-1）：本端点对模型段是全路径透传，
        # httpx 会做路径归一化——模型名里的 ../ 或 ./ 分段会让持 Key 用户
        # 把请求逃逸到 aiplatform.googleapis.com 的其他资源路径。显式拒绝
        # 点分段；服务端自生成的 location 钉定路径不受影响。
        if any(segment in ("", ".", "..") for segment in model.split("/")):
            return _native_error(
                400, "模型名不能包含点分段（./ 或 ../）或空路径段。"
            )
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
        if declared_body_exceeds(declared_length, MAX_NATIVE_BODY_BYTES):
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

        # -search 变体（与 OpenAI / Claude 端点对齐；目录也列出这些变体）：
        # 剥后缀 + 注入 google_search 工具。监控按客户端原始请求名记录。
        requested_model = model
        if model.lower().endswith("-search"):
            from model_catalog import search_variant_available
            base_probe = model[: -len("-search")]
            if not search_variant_available(base_probe):
                return _native_error(
                    400, f"模型 {base_probe} 不支持 -search 变体（仅 Gemini 文本模型支持）。")
            model, rebuilt = _strip_search_variant(model, action, parsed)
            if rebuilt is not None:
                raw_body = rebuilt
                print(f"🔎 [Gemini 透传] {model} | -search 变体：已注入 google_search 工具"
                      "（body 最小重组；普通模型仍字节级透传）。")

        # 生图分辨率后缀变体（-1k/-2k/-4k/-512，目录已列出）：剥后缀 +
        # 注入 imageConfig.imageSize。与 -search 互斥（生图无 -search 变体），
        # 二者尾部形态不同不会同时命中。仅生图档案生效，文本模型名原样透传。
        from model_capabilities import image_resolution_suffix
        if image_resolution_suffix(model)[1]:
            model, rebuilt = _strip_image_size_variant(model, action, parsed)
            if rebuilt is not None:
                raw_body = rebuilt
                print(f"🔎 [Gemini 透传] {model} | 分辨率后缀变体：已注入"
                      " imageConfig.imageSize（body 最小重组；普通模型仍字节级透传）。")

        # 通道选择（尊重控制台的通道模式开关，与 chat_api 的选路语义对齐）：
        # - web_proxy（Cookie 模式）：配置了 Cookie 账号就走 Cookie 通道**协议级桥接**
        #   （cookie_native：原生 JSON → batchGraphql 转码，响应重组回原生形状），
        #   **即使同时配置了 Express Key 也不再霸占该端点**（旧实现只看"有没有 Key"，
        #   只要存在任意 Key 就字节级透传，Cookie 模式形同虚设）。Cookie 桥接不支持
        #   的操作（countTokens 等）在没有更优解时才回退 Key 透传。
        # - api_key / mixed：有 Key 优先字节级透传（最保真）；没有 Key 但配了
        #   Cookie 时回退 Cookie 桥接——旧实现在此直接 403，导致仅配 Cookie 的
        #   部署无法使用 Gemini 原生生态客户端。
        # getattr 防御：单测的 SimpleNamespace 桩可能不带 app（生产 Starlette 必有）。
        express_manager = getattr(getattr(request, "app", None), "state", None)
        manager = getattr(express_manager, "express_key_manager", None)
        from credential_manager import credential_manager
        if manager is None:
            # 凭据池不可知（测试桩 / 极早期生命周期）：保持旧行为——不做模式
            # 改道，直接落到透传路径；否则桩环境会被误判为"无凭据"而 403。
            has_keys, has_cookie = True, False
        else:
            has_keys = manager.get_total_keys() > 0
            has_cookie = credential_manager.get_cookie_record() is not None
        mode = app_state.get_channel_mode()
        cookie_bridgable = has_cookie and action in ("generateContent", "streamGenerateContent")

        if not has_keys or mode == "web_proxy":
            if cookie_bridgable and (mode == "web_proxy" or not has_keys):
                from upstreams.cookie_native import handle_native_request
                is_stream = action == "streamGenerateContent"
                track_configure(model=requested_model, protocol="gemini", channel="cookie",
                                stream=is_stream, mode="stream" if is_stream else "json")
                print(f"🔁 [Gemini 透传] {requested_model}:{action} | {'流式' if is_stream else '非流式'}"
                      f" | Cookie 通道协议级桥接（{mode} 模式）")
                return await handle_native_request(
                    request, model, action, parsed, requested_model=requested_model)
            if has_cookie and not cookie_bridgable and not has_keys:
                # Cookie 模式 / 无 Key 部署：桥接不支持的明确报错（不要静默换通道）。
                return _native_error(
                    400,
                    f"Cookie 通道暂不支持 :{action}（上游 batchGraphql 无对应操作），"
                    "请配置 Express API Key 使用该操作。",
                    "INVALID_ARGUMENT")
            if not has_keys and not has_cookie:
                return _native_error(
                    403,
                    "该端点需要至少一个 Express API Key（字节级透传）或一个启用的 Cookie 账号"
                    "（协议级桥接，仅 generateContent / streamGenerateContent）。"
                    "请在控制台「通道与凭证」页配置，或设环境变量 VERTEX_EXPRESS_API_KEY / GOOGLE_COOKIE。",
                    status_detail="PERMISSION_DENIED",
                )
            # 剩余组合：web_proxy 模式下 Cookie 桥接不支持该操作但有 Key 兜底，
            # 或 api_key/mixed 模式有 Key——都落到下方 Key 透传。

        # 透传 query（alt=sse 等），但剔除本服务鉴权用的 key 参数。
        # query_params 取出的是已解码值，重新下发前必须再编码，否则值里
        # 带 & / = / 空格会把 URL 拼坏（alt=sse 之外的参数被截断）。
        qp = [(k, v) for k, v in request.query_params.multi_items() if k not in ("key", "resume_from")]
        query = "&".join(f"{quote(k, safe='')}={quote(v, safe='')}" for k, v in qp)

        is_stream = action == "streamGenerateContent"
        track_configure(model=requested_model, protocol="gemini", channel="express", stream=is_stream,
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
            if sse and resume and resumable_enabled() and not image_bypass_enabled(model):
                # 生图默认绕过续传缓冲（512MB 容器内存治理，见
                # resumable.image_bypass_enabled）：4K base64 的全量驻留
                # 会叠加 OOMKill，直通交付（落到下方直通分支）。
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


@router.get("/v1beta/models")
async def native_list_models(fastapi_request: Request,
                             _auth: str = Depends(get_gemini_auth)):
    """把内部模型清单映射成 Gemini 原生 models.list 响应。

    与 OpenAI /v1/models、Anthropic 目录同源：裸模型 + -search 变体
    （透传侧 _strip_search_variant 会剥后缀并注入 google_search 工具，
    目录与实际能力一致）。抗截断后缀仍仅限 OpenAI 兼容端点。
    """
    manager = fastapi_request.app.state.express_key_manager
    has_any = manager.get_total_keys() > 0 or app_state.get_channel_mode() in ("web_proxy", "mixed")
    raw = await get_express_models() if has_any else []

    models = [_native_model_entry(mid) for mid in _native_catalog_ids(raw)]
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
    if model and model in _native_catalog_ids(raw):
        return _native_model_entry(model)
    return _native_error(404, f"Model {model} not found.", status_detail="NOT_FOUND")


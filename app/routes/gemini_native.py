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
cachedContent / responseSchema 等全部字段天然 100% 兼容，因为请求体
一个字节都没动，只是换了认证（客户端的本服务 Key → Express Key）。

实现要点：
- 认证兼容三种原生写法：Authorization: Bearer、x-goog-api-key、?key=
  （值必须等于本服务的 API_KEY，否则 401）。
- Key 池轮询/随机、location 钉定、出站代理全部复用现有 Express 通道逻辑。
- 上游响应（含错误）状态码与 body 原样转发；流式以原始字节流转发 SSE。
- 429/5xx 自动换下一个 Express Key 重试（最多 3 次）。
- 模型列表用内部模型清单映射成 Gemini 原生格式（models/get 也支持）。

限制：Cookie 通道（batchGraphql）是私有 GraphQL 协议，无法承载原生 REST
请求，故本透传始终走 Express 通道；未配置任何 Express Key 时返回明确错误。
"""

import asyncio
import secrets
import time
from typing import Any, Optional

import httpx
from fastapi import APIRouter, Depends, Header, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from auth import validate_api_key
from config import API_KEY
from runtime_state import app_state, get_effective_proxy_url
import config as app_config
from upstreams.express_sdk import resolve_express_model_path
from model_loader import get_express_models
from express_fingerprint import build_direct_headers

router = APIRouter()

# Express 端点（与 SDK 解析结果一致，见 express_sdk._log_resolved_endpoint）
EXPRESS_BASE = app_config.VERTEX_BASE_URL or "https://aiplatform.googleapis.com"
EXPRESS_API_VERSION = "v1"

# 支持透传的 :action 后缀
_SUPPORTED_ACTIONS = {"generateContent", "streamGenerateContent", "countTokens", "embedContent"}

# 上游透传客户端超时：流式长思考可能持续数分钟，read 放宽。
_UPSTREAM_TIMEOUT = httpx.Timeout(connect=30.0, read=600.0, write=60.0, pool=10.0)

# 429/5xx 换 Key 重试次数（不含首次）
_KEY_RETRY_MAX = 2


# ============================================================
# 认证：兼容 Bearer / x-goog-api-key / ?key= 三种原生写法
# ============================================================
async def _require_native_key(request: Request, key: Optional[str]) -> bool:
    """校验客户端凭据是否等于本服务 API_KEY（常量时间比较）。"""
    if not key:
        return False
    return bool(API_KEY) and secrets.compare_digest(str(key), API_KEY)


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
    detail = status_detail or {
        400: "INVALID_ARGUMENT", 401: "UNAUTHENTICATED", 403: "PERMISSION_DENIED",
        404: "NOT_FOUND", 429: "RESOURCE_EXHAUSTED", 500: "INTERNAL",
    }.get(status, "UNKNOWN")
    return JSONResponse(status_code=status, content={
        "error": {"code": status, "message": message, "status": detail}
    })


# ============================================================
# 模型名解析
# ============================================================
def _split_model_action(model_action: str) -> tuple[str, str]:
    """把 `gemini-3.6-pro:generateContent` 拆成 (model, action)。

    模型名本身不含冒号（Gemini 命名规范），第一个冒号即为分隔符。
    客户端若传了 `models/gemini-xxx:action` 形式，也一并剥掉前缀。
    """
    name = model_action
    for prefix in ("models/", "publishers/google/models/", "tunedModels/"):
        if name.startswith(prefix):
            name = name[len(prefix):]
    if ":" in name:
        model, _, action = name.partition(":")
        return model.strip(), action.strip()
    return name.strip(), ""


def _resolve_upstream_model_path(model: str, key_project_id: str) -> str:
    """复用 Express 通道的 location 钉定规则。"""
    settings = app_state.get_settings()
    return resolve_express_model_path(model, settings, key_project_id=key_project_id)


# ============================================================
# 上游转发核心
# ============================================================
def _pick_express_key(fastapi_request: Request) -> Optional[dict]:
    manager = fastapi_request.app.state.express_key_manager
    if manager.get_total_keys() == 0:
        return None
    return manager.get_express_record()


async def _passthrough_once(client: httpx.AsyncClient, express_key: str,
                            model_path: str, action: str, query: str,
                            payload: Any, *, stream: bool) -> httpx.Response:
    """向上游发一次请求。stream=True 时返回**未消费**的响应
    （供 aiter_raw 逐块转发）；stream=False 时返回已缓冲的响应。"""
    url = (f"{EXPRESS_BASE}/{EXPRESS_API_VERSION}/publishers/google/models/"
           f"{model_path}:{action}")
    if query:
        url = f"{url}?{query}"
    req = client.build_request(
        "POST", url, json=payload,
        headers={
            "x-goog-api-key": express_key,
            "content-type": "application/json",
            # 请求头指纹跟随控制台 express_ua_profile（sdk/node/gcloud/custom）。
            # 这里绝不能出现 python-httpx 或任何含 "vertex2openai" 的 UA——
            # 直接管自己叫代理等于在 Google 风控日志里自首。
            **build_direct_headers(),
        },
    )
    return await client.send(req, stream=stream)


async def _passthrough_request(fastapi_request: Request, model: str, action: str,
                               query: str, payload: Any):
    """选 Key → 透传 → 429/5xx 换 Key 重试。返回 fastapi Response。

    客户端生命周期：非流式在本函数内创建/关闭；流式交给 _relay_stream
    （转发生成器结束后关闭）——绝不能用 async with 包住返回值，
    否则 StreamingResponse 还没消费，上游连接就被关了。
    """
    manager = fastapi_request.app.state.express_key_manager
    if manager.get_total_keys() == 0:
        return _native_error(
            403,
            "该透传端点需要至少一个 Express API Key（控制台「通道与凭证」页添加，"
            "或设环境变量 VERTEX_EXPRESS_API_KEY）。Cookie 直连通道是私有 GraphQL 协议，"
            "无法承载 Gemini 原生 REST 请求。",
            status_detail="PERMISSION_DENIED",
        )

    tried_keys: set[str] = set()
    last_error_body = b""
    last_error_status = 502

    proxy_url = get_effective_proxy_url()
    client_kwargs: dict[str, Any] = {"timeout": _UPSTREAM_TIMEOUT, "follow_redirects": False}
    if proxy_url:
        client_kwargs["proxy"] = proxy_url

    is_stream = action == "streamGenerateContent"
    client = httpx.AsyncClient(**client_kwargs)
    try:
        for attempt in range(_KEY_RETRY_MAX + 1):
            if await fastapi_request.is_disconnected():
                await client.aclose()
                return Response(status_code=499)

            record = manager.get_express_record()
            if record is None:
                break
            express_key = record["key"]
            if express_key in tried_keys:
                # Key 池已试遍，不再空转
                break
            tried_keys.add(express_key)

            model_path = _resolve_upstream_model_path(model, record.get("project_id", ""))
            try:
                upstream = await _passthrough_once(
                    client, express_key, model_path, action, query, payload,
                    stream=is_stream)
            except httpx.TimeoutException:
                last_error_status = 504
                last_error_body = (b'{"error":{"code":504,"message":"Upstream timeout",'
                                   b'"status":"DEADLINE_EXCEEDED"}}')
                continue
            except Exception as e:
                last_error_status = 502
                last_error_body = str(e).encode()
                continue

            retryable = upstream.status_code in (429, 500, 503)
            if not retryable or attempt >= _KEY_RETRY_MAX:
                # 成功 / 不可重试错误 / 重试耗尽：原样透传（保留上游真实状态码与 body）
                if is_stream:
                    return await _relay_stream(client, upstream)
                content = upstream.content
                status = upstream.status_code
                ctype = upstream.headers.get("content-type", "application/json")
                await client.aclose()
                return Response(content=content, status_code=status, media_type=ctype)

            last_error_status = upstream.status_code
            # 流式响应必须先消费才能安全释放（否则连接悬挂在缓冲区上）
            try:
                last_error_body = await upstream.aread()
            except Exception:
                last_error_body = b""
            await upstream.aclose()
            retry_after = upstream.headers.get("retry-after")
            wait = 1.5 * (attempt + 1)
            try:
                wait = min(float(retry_after) if retry_after else wait, 8.0)
            except ValueError:
                pass
            print(f"⚠️ [Gemini 透传] 上游 HTTP {upstream.status_code}（第 {attempt + 1} 次），"
                  f"换下一个 Express Key，{wait:.1f}s 后重试。")
            await asyncio.sleep(wait)
    except Exception:
        await client.aclose()
        raise

    await client.aclose()
    return Response(
        content=last_error_body or b'{"error":{"code":502,"message":"Upstream unreachable","status":"UNAVAILABLE"}}',
        status_code=last_error_status,
        media_type="application/json",
    )


async def _relay_stream(client: httpx.AsyncClient, upstream: httpx.Response) -> StreamingResponse:
    """把上游 SSE（或 JSON array 流）以原始字节转发给客户端。

    用 aiter_raw()（不解压、不解码）保证字节级保真；
    上游响应与 HTTP 客户端都在转发生成器 finally 里释放
    （消费完最后一个 chunk 才断开上游）。客户端断开时
    Starlette 会取消生成器，finally 同样生效。
    """

    async def _relay():
        try:
            async for chunk in upstream.aiter_raw():
                if chunk:
                    yield chunk
        finally:
            await upstream.aclose()
            await client.aclose()

    media_type = upstream.headers.get("content-type", "text/event-stream")
    return StreamingResponse(_relay(), status_code=upstream.status_code,
                             media_type=media_type)


# ============================================================
# 端点：generateContent / streamGenerateContent / countTokens / embedContent
# ============================================================
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

        # body 必须是合法 JSON 对象（Gemini 请求全是 JSON 对象）
        try:
            payload = await request.json()
        except Exception:
            return _native_error(400, "请求体必须是 JSON 对象（Gemini 原生格式）。")
        if not isinstance(payload, dict):
            return _native_error(400, "请求体必须是 JSON 对象（Gemini 原生格式）。")

        # 透传 query（alt=sse 等），但剔除本服务鉴权用的 key 参数
        qp = [(k, v) for k, v in request.query_params.multi_items() if k != "key"]
        query = "&".join(f"{k}={v}" for k, v in qp)

        is_stream = action == "streamGenerateContent"
        print(f"🔁 [Gemini 透传] {model}:{action} | {'流式' if is_stream else '非流式'}"
              f" | 原生协议透传（Express 通道）")
        return await _passthrough_request(request, model, action, query, payload)

    return endpoint


for _prefix in ("/v1beta", "/v1"):
    router.add_api_route(
        f"{_prefix}/models/{{model_action}}",
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
    has_any = manager.get_total_keys() > 0 or app_state.is_web_proxy_enabled()
    raw = await get_express_models() if has_any else []

    methods = ["generateContent", "streamGenerateContent", "countTokens"]
    models = []
    for mid in sorted(raw):
        entry = {
            "name": f"models/{mid}",
            "displayName": mid,
            "description": f"Vertex Express 模型（经 vertex2openai 透传）：{mid}",
            "inputTokenLimit": 1048576,
            "outputTokenLimit": 65536,
            "supportedGenerationMethods": methods,
        }
        models.append(entry)
    return {"models": models}


@router.get("/v1beta/models/{model_action}")
async def native_get_model(model_action: str,
                           _auth: str = Depends(get_gemini_auth)):
    model, action = _split_model_action(model_action)
    if action:
        return _native_error(400, f"GET 不支持 :{action}，请用 POST。")
    raw = await get_express_models()
    if model and model in raw:
        return {
            "name": f"models/{model}",
            "displayName": model,
            "description": f"Vertex Express 模型（经 vertex2openai 透传）：{model}",
            "inputTokenLimit": 1048576,
            "outputTokenLimit": 65536,
            "supportedGenerationMethods": [
                "generateContent", "streamGenerateContent", "countTokens"],
        }
    return _native_error(404, f"Model {model} not found.", status_detail="NOT_FOUND")

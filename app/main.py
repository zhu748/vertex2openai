"""ASGI 入口：组装路由、中间件与异常处理。"""
from pathlib import Path

from config import env_str  # QUA-P2-7：环境读取集中层
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from console_assets import ConsoleStaticFiles

from express_key_manager import ExpressKeyManager
from lifecycle import lifespan
from logger import stats
from middleware import SecurityHeadersMiddleware, StatsMiddleware
from image_delivery import ImageDeliveryMiddleware
from request_connection import BodyTooLargeError
from request_errors import body_too_large_response, ingress_error_response, validation_error_response
from request_ingress import IngressError
from routes import (
    chat_api,
    claude_api,
    console,
    credentials_api,
    gemini_native,
    generated_images,
    models_api,
    settings_api,
    system_api,
)
from runtime_state import StatePersistenceError


def parse_cors_origins(raw: str) -> list[str] | None:
    """SEC-P3-1：解析 ALLOWED_ORIGINS 环境变量。

    返回 None = 默认同源（不安装 CORS 中间件，浏览器跨源一律读不到响应，
    对 SDK/服务端调用与自带控制台零影响）；返回列表 = 显式放行的来源，
    其中 "*" 恢复旧的任意来源行为（需要浏览器跨源直连 API 时自行承担）。
    逗号 / 空白分隔均可。
    """
    raw = (raw or "").strip()
    if not raw:
        return None
    if raw == "*":
        return ["*"]
    return [item for item in raw.replace(",", " ").split() if item]


CORS_ALLOW_ORIGINS = parse_cors_origins(env_str("ALLOWED_ORIGINS"))  # QUA-P2-7：环境读取走 config 集中层


app = FastAPI(title="agentplatform2api", lifespan=lifespan)
app.state.express_key_manager = ExpressKeyManager()
app.mount("/static", ConsoleStaticFiles(directory=Path(__file__).parent / "static"), name="static")

app.add_middleware(StatsMiddleware, stats=lambda: stats)
app.add_middleware(ImageDeliveryMiddleware)
# SEC-P2-5：全部响应附 nosniff/no-referrer，HTML 额外附 CSP + X-Frame-Options
# （纵深防御；CORS 预检由更外层的 CORSMiddleware 直接应答，无需这些头）。
app.add_middleware(SecurityHeadersMiddleware)
# SEC-P3-1：CORS 默认同源（不装中间件）。旧行为 allow_origins=["*"] 会让
# 任意站点无凭据探测全部端点；需要浏览器跨源直连时显式设 ALLOWED_ORIGINS
#（逗号分隔），或 ALLOWED_ORIGINS="*" 恢复任意来源。
if CORS_ALLOW_ORIGINS is not None:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=CORS_ALLOW_ORIGINS,
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["X-Resume-Id", "X-Resumable-Replay", "X-Trace-Id", "Retry-After"],
    )


@app.exception_handler(StatePersistenceError)
async def state_persistence_error(request, exc):
    return JSONResponse(status_code=503, content={"error": str(exc), "type": "state_persistence_error"})


@app.exception_handler(RequestValidationError)
async def _request_validation_error_handler(request, exc: RequestValidationError):
    return validation_error_response(request.url.path, exc.errors())


@app.exception_handler(BodyTooLargeError)
async def _body_too_large_error_handler(request, exc: BodyTooLargeError):
    # BodyTooLargeError 继承 HTTPException：内层 ExceptionMiddleware 会在
    # StatsMiddleware 之前把它转成 Starlette 默认形状（{"detail": ...}），
    # 中间件里的 except 分支永远不会命中（2026-10 修复）。在此注册显式
    # 处理器渲染错误形状，客户端不再拿到两套不一致的 413 文案。
    path = getattr(getattr(request, "url", None), "path", "") or ""
    return body_too_large_response(path, exc.limit)


@app.exception_handler(IngressError)
async def _ingress_error_handler(request, exc: IngressError):
    return ingress_error_response(request.url.path, exc.status_code, exc.detail)


@app.exception_handler(gemini_native.NativeAPIError)
async def _gemini_native_error_handler(request, exc: "gemini_native.NativeAPIError"):
    return exc.response


@app.exception_handler(claude_api.ClaudeAPIError)
async def _claude_error_handler(request, exc: "claude_api.ClaudeAPIError"):
    """Claude 端点鉴权异常：返回 Anthropic 错误形状（而非 FastAPI 默认 detail）。"""
    return exc.response


for router in (
    console.router,
    system_api.router,
    settings_api.router,
    credentials_api.router,
    models_api.router,
    chat_api.router,
    claude_api.router,
    gemini_native.router,
    generated_images.router,
):
    app.include_router(router)

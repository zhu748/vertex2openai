"""ASGI 入口：组装路由、中间件与异常处理。"""
from pathlib import Path

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from console_assets import ConsoleStaticFiles

from express_key_manager import ExpressKeyManager
from lifecycle import lifespan
from logger import stats
from middleware import StatsMiddleware
from request_connection import BodyTooLargeError
from request_errors import body_too_large_response, validation_error_response
from routes import (
    chat_api,
    claude_api,
    console,
    credentials_api,
    gemini_native,
    models_api,
    settings_api,
    system_api,
)
from runtime_state import StatePersistenceError


app = FastAPI(title="agentplatform2api", lifespan=lifespan)
app.state.express_key_manager = ExpressKeyManager()
app.mount("/static", ConsoleStaticFiles(directory=Path(__file__).parent / "static"), name="static")

app.add_middleware(StatsMiddleware, stats=lambda: stats)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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
):
    app.include_router(router)

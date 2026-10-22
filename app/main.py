"""ASGI 入口：组装路由、中间件与异常处理。"""
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from console_assets import ConsoleStaticFiles

from express_key_manager import ExpressKeyManager
from lifecycle import lifespan
from logger import stats
from middleware import StatsMiddleware
from request_connection import BodyTooLargeError
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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Resume-Id", "X-Resumable-Replay", "X-Trace-Id", "Retry-After"],
)
app.add_middleware(StatsMiddleware, stats=lambda: stats)


@app.exception_handler(StatePersistenceError)
async def state_persistence_error(request, exc):
    return JSONResponse(status_code=503, content={"error": str(exc), "type": "state_persistence_error"})


@app.exception_handler(BodyTooLargeError)
async def _body_too_large_error_handler(request, exc: BodyTooLargeError):
    # BodyTooLargeError 继承 HTTPException：内层 ExceptionMiddleware 会在
    # StatsMiddleware 之前把它转成 Starlette 默认形状（{"detail": ...}），
    # 中间件里的 except 分支永远不会命中（2026-10 修复）。在此注册显式
    # 处理器渲染错误形状，客户端不再拿到两套不一致的 413 文案。
    limit = getattr(exc, "limit", 0) or 0
    size = f"{limit // (1024 * 1024)}MiB" if limit >= 1024 * 1024 else f"{max(1, limit // 1024)}KiB"
    message = f"请求体超过 {size} 上限。"
    path = getattr(getattr(request, "url", None), "path", "") or ""
    if path == "/v1/messages" or path.startswith("/v1/messages/"):
        # Claude 生态客户端（/v1/messages）：Anthropic 错误形状
        return claude_api.claude_error(413, "invalid_request_error", message)
    return JSONResponse(
        status_code=413,
        content={"error": {"message": message,
                          "type": "invalid_request_error", "code": 413}},
    )


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

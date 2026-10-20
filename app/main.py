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
from routes import (
    chat_api,
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


@app.exception_handler(gemini_native.NativeAPIError)
async def _gemini_native_error_handler(request, exc: "gemini_native.NativeAPIError"):
    return exc.response


for router in (
    console.router,
    system_api.router,
    settings_api.router,
    credentials_api.router,
    models_api.router,
    chat_api.router,
    gemini_native.router,
):
    app.include_router(router)

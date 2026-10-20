"""ASGI middleware which does not buffer or acknowledge streaming responses."""
import asyncio
import anyio
from starlette.requests import ClientDisconnect
from starlette.responses import JSONResponse

from request_tracking import PayloadObserver, current_trace, request_registry
from request_connection import ClientConnection, ConnectionReceiver

# 请求体体积上限：与原生透传通道（gemini_native.MAX_NATIVE_BODY_BYTES）的
# 32MiB 防护对齐。/v1/chat/completions 的 body 由 FastAPI/Pydantic 全量读入
# 解析（含巨型 messages / base64 图片），不设限等于允许认证后的内存放大。
# 此处在进入 FastAPI 前只看 content-length；chunked 无长度头的场景由 native
# 通道自身的两段式检查覆盖，chat 通道极罕见（主流客户端对 JSON body 必带长度头）。
MAX_CHAT_BODY_BYTES = 32 * 1024 * 1024


class StatsMiddleware:
    def __init__(self, app, stats):
        self.app = app
        self.stats = stats

    async def __call__(self, scope, receive, send):
        stats = self.stats() if callable(self.stats) else self.stats
        path = scope.get("path", "")
        tracked = scope["type"] == "http" and scope.get("method") == "POST" and (
            path == "/v1/chat/completions" or path.startswith(("/v1/models/", "/v1beta/models/")))
        if not tracked:
            return await self.app(scope, receive, send)
        stats.increment_total()
        declared_length = ""
        for key, value in scope.get("headers", []):
            if key == b"content-length":
                declared_length = value.decode("latin-1", "")
                break
        if declared_length.isdigit() and int(declared_length) > MAX_CHAT_BODY_BYTES:
            stats.add_error()
            response = JSONResponse(
                status_code=413,
                content={"error": {"message": f"请求体超过 {MAX_CHAT_BODY_BYTES // (1024 * 1024)}MiB 上限。",
                                  "type": "invalid_request_error", "code": 413}},
            )
            await response(scope, receive, send)
            return
        trace = request_registry.create(path)
        token = current_trace.set(trace)
        started = False
        completed = False
        observer = None
        send_failed = False
        connection = ClientConnection()
        scope.setdefault("state", {})["client_connection"] = connection
        receiver = ConnectionReceiver(receive, connection, lambda: trace.end_connection(disconnected=True),
                                      lambda: completed)

        async def relay(message):
            nonlocal started, observer, completed, send_failed
            if message["type"] == "http.response.start":
                started = True
                trace.data["http_status"] = message["status"]
                headers = list(message.get("headers", []))
                headers.append((b"x-trace-id", trace.id.encode()))
                message = dict(message, headers=headers)
                content_type = dict(headers).get(b"content-type", b"").lower()
                observer = PayloadObserver(trace, sent=True, sse=b"text/event-stream" in content_type)
                trace.milestone("response_start", f"开始响应前端：HTTP {message['status']}。")
                if message["status"] >= 400:
                    stats.add_error()
                    trace.fail(f"HTTP {message['status']}", message["status"])
            # Await the real server send, including the stream's DONE frame.
            final_body = message["type"] == "http.response.body" and not message.get("more_body", False)
            if final_body:
                receiver.final_send_started = True
            try:
                await send(message)
            except BaseException:
                send_failed = True
                receiver.disconnect()
                raise
            finally:
                if final_body:
                    # completed is assigned below before this task next yields.
                    receiver.final_send_done.set()
            if message["type"] == "http.response.body":
                completed = not message.get("more_body", False)
                if observer:
                    observer.feed(message.get("body", b""), final=completed)

        try:
            await self.app(scope, receiver, relay)
        except (asyncio.CancelledError, ClientDisconnect):
            raise
        except Exception as exc:
            if send_failed:
                raise
            if not started:
                stats.add_error()
            trace.fail(str(exc))
            raise
        finally:
            connection.finish(send_failed or not completed)
            trace.end_connection(disconnected=send_failed or not completed)
            try:
                with anyio.CancelScope(shield=True):
                    await receiver.aclose()
            finally:
                current_trace.reset(token)

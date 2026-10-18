"""ASGI middleware which does not buffer or acknowledge streaming responses."""
import asyncio
import anyio
from starlette.requests import ClientDisconnect

from request_tracking import PayloadObserver, current_trace, request_registry
from request_connection import ClientConnection, ConnectionReceiver


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

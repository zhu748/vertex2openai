"""ASGI middleware which does not buffer or acknowledge streaming responses."""
import asyncio

import anyio
from request_connection import BodyTooLargeError, ClientConnection, ConnectionReceiver
from request_tracking import PayloadObserver, current_trace, request_registry
from starlette.requests import ClientDisconnect
from starlette.responses import JSONResponse

# 请求体体积上限：与原生透传通道（gemini_native.MAX_NATIVE_BODY_BYTES）的
# 32MiB 防护对齐。/v1/chat/completions 的 body 由 FastAPI/Pydantic 全量读入
# 解析（含巨型 messages / base64 图片），不设限等于允许认证后的内存放大。
# content-length 头在此预检；chunked 无长度头的场景由 ConnectionReceiver
# 在 receive 层边读边计数，超限抛 BodyTooLargeError → 此处统一回 413
# （旧实现依赖 native 通道「读完后检查」，据不住 chunked，2026-10 修复）。
MAX_CHAT_BODY_BYTES = 32 * 1024 * 1024

# 未认证端点（/api/login）的请求体上限：Pydantic 全量解析巨型 JSON 同样
# 是内存放大面，而这里无需鉴权即可触达。口令本身就是短字符串，256KB 绰绰有余。
LOGIN_BODY_BYTES = 256 * 1024


class _LoginBodyLimitReceive:
    """给 /api/login 包一层计数 receive（不建 trace、不计入大盘统计）。"""

    def __init__(self, receive, max_bytes: int):
        self._receive = receive
        self._max_bytes = max_bytes
        self._seen = 0

    async def __call__(self):
        message = await self._receive()
        if message.get("type") == "http.request":
            self._seen += len(message.get("body") or b"")
            if self._seen > self._max_bytes:
                raise BodyTooLargeError(self._max_bytes)
        return message


class StatsMiddleware:
    def __init__(self, app, stats):
        self.app = app
        self.stats = stats

    async def __call__(self, scope, receive, send):
        stats = self.stats() if callable(self.stats) else self.stats
        path = scope.get("path", "")
        method = scope.get("method", "")
        tracked = scope["type"] == "http" and method == "POST" and (
            path == "/v1/chat/completions" or path == "/v1/messages"
            or path.startswith(("/v1/messages/", "/v1/models/", "/v1beta/models/")))
        # 登录端点单独限流：不计入请求统计（避免大盘被登录轮询污染），
        # 也不建 trace；只做体积防护。
        if (not tracked and scope["type"] == "http" and method == "POST"
                and path == "/api/login"):
            limited = _LoginBodyLimitReceive(receive, LOGIN_BODY_BYTES)
            try:
                await self.app(scope, limited, send)
            except BodyTooLargeError:
                response = JSONResponse(
                    status_code=413,
                    content={"error": {"message": f"登录请求体超过 {LOGIN_BODY_BYTES // 1024}KiB 上限。",
                                      "type": "invalid_request_error", "code": 413}},
                )
                await response(scope, receive, send)
            return
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
                                      lambda: completed, max_body_bytes=MAX_CHAT_BODY_BYTES)

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
        except BodyTooLargeError as exc:
            # chunked 请求体在 receive 层超限：响应尚未开始，统一回 413。
            # 旧实现只能在应用层全量读完后检查，拦不住无长度头的流式上传。
            # 错误计数交给 relay（status >= 400 时统一计），不在此重复计。
            if started or send_failed:
                raise
            print(f"⚠️ [体积限制] {path}：{exc}")
            message = f"请求体超过 {MAX_CHAT_BODY_BYTES // (1024 * 1024)}MiB 上限。"
            if path == "/v1/messages" or path.startswith("/v1/messages/"):
                # Claude 生态客户端：Anthropic 错误形状（懒加载避免模块级
                # 依赖顺序变化；此兜底分支本身极少命中）。
                from routes import claude_api as _claude_api
                response = _claude_api.claude_error(413, "invalid_request_error", message)
            else:
                response = JSONResponse(
                    status_code=413,
                    content={"error": {"message": message,
                                      "type": "invalid_request_error", "code": 413}},
                )
            await response(scope, receive, relay)
            return
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

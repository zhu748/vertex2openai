"""One ASGI receiver owns disconnect observation after the request body ends."""
import asyncio
import time

from starlette.exceptions import HTTPException
from starlette.requests import ClientDisconnect


class BodyTooLargeError(HTTPException):
    """请求体字节数超过调用方设定的上限（在 receive 层边读边计数抛出）。

    content-length 头只能拦「谎报长度」的请求；Transfer-Encoding: chunked
    无长度头，旧实现要在应用层全量读完后才检查——超大甚至永不终止的
    chunked 流会在 413 生效前耗尽内存。此异常由 StatsMiddleware 统一转成 413。

    继承 HTTPException 的原因：FastAPI 路由层会把 body 读取抛出的普通
    Exception 吞成 400「There was an error parsing the body」，但显式
    re-raise HTTPException——继承它才能带着 413 语义穿透到中间件。
    """

    def __init__(self, limit: int):
        mib = max(1, round(limit / (1024 * 1024)))
        super().__init__(status_code=413, detail=f"请求体超过体积上限（约 {mib}MiB）。")
        self.limit = limit

class ClientConnection:
    def __init__(self):
        self.closed = False
        self.disconnected = False
        self.disconnected_at = None
        self.disconnected_monotonic = None
        self.event = asyncio.Event()
        self.listeners = []

    def listen(self, callback):
        if self.closed:
            callback(self.disconnected)
        else:
            self.listeners.append(callback)

    def finish(self, disconnected):
        if self.closed:
            return
        self.closed, self.disconnected = True, disconnected
        if disconnected:
            self.disconnected_at = time.time()
            self.disconnected_monotonic = time.monotonic()
        self.event.set()
        callbacks, self.listeners = self.listeners, []
        for callback in callbacks:
            callback(disconnected)


class ConnectionReceiver:
    def __init__(self, receive, connection, on_disconnect, response_completed,
                 max_body_bytes: int | None = None):
        self.receive = receive
        self.connection = connection
        self.on_disconnect = on_disconnect
        self.response_completed = response_completed
        self.body_done = False
        self.watcher = None
        self.final_send_started = False
        self.final_send_done = asyncio.Event()
        # chunked 请求体的流式限额：None = 调用方已用 content-length 预检。
        self._max_body_bytes = max_body_bytes
        self._body_bytes = 0

    async def __call__(self):
        if self.body_done:
            await self.connection.event.wait()
            return {"type": "http.disconnect"}
        message = await self.receive()
        if message["type"] == "http.disconnect":
            self.disconnect()
        elif message["type"] == "http.request":
            if self._max_body_bytes is not None:
                self._body_bytes += len(message.get("body") or b"")
                if self._body_bytes > self._max_body_bytes:
                    raise BodyTooLargeError(self._max_body_bytes)
            if not message.get("more_body", False):
                self.body_done = True
                self.watcher = asyncio.create_task(self.watch())
        return message

    def disconnect(self):
        self.connection.finish(True)
        self.on_disconnect()

    async def watch(self):
        while True:
            try:
                message = await self.receive()
            except (OSError, ClientDisconnect):
                self.disconnect()
                return
            if message["type"] == "http.disconnect":
                # ASGI test transports may announce disconnect while the final
                # send is completing. A successful final send is normal closure.
                if self.final_send_started:
                    await self.final_send_done.wait()
                if not self.response_completed():
                    self.disconnect()
                return
            await asyncio.sleep(0)

    async def aclose(self):
        if self.watcher:
            self.watcher.cancel()
            await asyncio.gather(self.watcher, return_exceptions=True)

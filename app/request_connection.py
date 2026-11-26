"""One ASGI receiver owns disconnect observation after the request body ends."""
import asyncio
import time

from starlette.exceptions import HTTPException
from starlette.requests import ClientDisconnect

from stream_cleanup import await_owned_close


class BodyTooLargeError(HTTPException):
    """请求体字节数超过调用方设定的上限（在 receive 层边读边计数抛出）。

    content-length 头只能拦「谎报长度」的请求；Transfer-Encoding: chunked
    无长度头，旧实现要在应用层全量读完后才检查——超大甚至永不终止的
    chunked 流会在 413 生效前耗尽内存。此异常统一转成 413（main.py 注册
    的显式处理器渲染 OpenAI 错误形状；StatsMiddleware 的 except 分支仅作
    处理器缺失时的兜底）。

    继承 HTTPException 的原因：FastAPI 路由层会把 body 读取抛出的普通
    Exception 吞成 400「There was an error parsing the body」，但显式
    re-raise HTTPException——继承它才能带着 413 语义穿透到中间件。
    """

    def __init__(self, limit: int):
        # 尺寸提示用准确单位：<1MiB 显示 KiB（登录上限 256KB 旧文案被
        # max(1, round(...)) 误标成“约 1MiB”）。此 detail 只是 HTTPException
        # 兼容文案；正式响应由 main.py 的 BodyTooLargeError 处理器渲染。
        if limit >= 1024 * 1024:
            size = f"{limit // (1024 * 1024)}MiB"
        else:
            size = f"{max(1, limit // 1024)}KiB"
        super().__init__(status_code=413, detail=f"请求体超过体积上限（{size}）。")
        self.limit = limit

class ClientConnection:
    def __init__(self):
        self.closed = False
        self.disconnected = False
        self.disconnected_at = None
        self.disconnected_monotonic = None
        self.disconnect_during_final_send = False
        self.event = asyncio.Event()
        self.listeners = []

    @classmethod
    def for_request(cls, request):
        connection = getattr(request, "scope", {}).get("state", {}).get("client_connection")
        return connection if isinstance(connection, cls) else None

    @property
    def delivery_possible(self):
        # This is a server send receipt, not proof the remote application read it.
        return not (self.disconnected or self.disconnect_during_final_send)

    def listen(self, callback):
        if self.closed:
            callback(self.disconnected)
        else:
            self.listeners.append(callback)

    async def while_connected(self, operation, *, discard=None):
        """Run direct work up to the handoff, using the middleware's only receiver.

        A successful return transfers ownership to the caller without another
        suspension point. If disconnect/caller cancellation wins, a raced stream
        result belongs to us: cancel and join setup, then dispose it exactly once.
        """
        if self.disconnected:
            raise ClientDisconnect()
        owner = asyncio.current_task()
        cancellations = owner.cancelling()
        job = asyncio.create_task(operation())

        def cancel_on_disconnect(gone):
            if gone and not job.done() and not job.cancelling():
                job.cancel()

        async def abandon():
            cancel_on_disconnect(True)
            try:
                result = await job
            except (asyncio.CancelledError, Exception):
                return
            if discard is not None:
                await discard(result)

        self.listen(cancel_on_disconnect)
        try:
            try:
                result = await asyncio.shield(job)
            except asyncio.CancelledError:
                # A cancelled waiter doesn't tell us whether the operation
                # acquired a response in this same event-loop turn.
                await await_owned_close(abandon())
                if self.disconnected and owner.cancelling() == cancellations:
                    raise ClientDisconnect() from None
                raise
            except Exception:
                # An adapter can translate cancellation into a transport error.
                if self.disconnected:
                    raise ClientDisconnect() from None
                raise
            if self.disconnected:
                if discard is not None:
                    await await_owned_close(discard(result))
                raise ClientDisconnect()
            return result
        finally:
            if cancel_on_disconnect in self.listeners:
                self.listeners.remove(cancel_on_disconnect)

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
    def __init__(self, receive, connection, on_disconnect=None, *, max_body_bytes: int | None = None):
        self.receive = receive
        self.connection = connection
        self.on_disconnect = on_disconnect
        self.completed = False
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
                self.start_watching()
        return message

    def start_watching(self):
        """Transfer raw receive ownership once; GET polls never need their body."""
        if self.watcher is None:
            self.body_done = True
            self.watcher = asyncio.create_task(self.watch())

    async def forward_send(self, send, message):
        final = message["type"] == "http.response.body" and not message.get("more_body", False)
        if final:
            self.final_send_started = True
        try:
            await send(message)
            if final:
                self.completed = True
        except BaseException:
            self.disconnect()
            raise
        finally:
            if final:
                self.final_send_done.set()

    def disconnect(self):
        self.connection.finish(True)
        if self.on_disconnect is not None:
            self.on_disconnect()

    async def watch(self):
        while True:
            try:
                message = await self.receive()
            except (OSError, ClientDisconnect):
                self.disconnect()
                return
            if message["type"] == "http.disconnect":
                if self.final_send_started and not self.final_send_done.is_set():
                    # Keep normal closure classification if send returns
                    # successfully, but don't use an ambiguous send to consume
                    # a replay. Some servers silently discard disconnected sends.
                    self.connection.disconnect_during_final_send = True
                    await self.final_send_done.wait()
                if not self.completed:
                    self.disconnect()
                return
            await asyncio.sleep(0)

    async def aclose(self):
        if self.watcher:
            self.watcher.cancel()
            await asyncio.gather(self.watcher, return_exceptions=True)

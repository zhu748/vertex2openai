"""One ASGI receiver owns disconnect observation after the request body ends."""
import asyncio
import time

from starlette.requests import ClientDisconnect


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
    def __init__(self, receive, connection, on_disconnect, response_completed):
        self.receive = receive
        self.connection = connection
        self.on_disconnect = on_disconnect
        self.response_completed = response_completed
        self.body_done = False
        self.watcher = None
        self.final_send_started = False
        self.final_send_done = asyncio.Event()

    async def __call__(self):
        if self.body_done:
            await self.connection.event.wait()
            return {"type": "http.disconnect"}
        message = await self.receive()
        if message["type"] == "http.disconnect":
            self.disconnect()
        elif message["type"] == "http.request" and not message.get("more_body", False):
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

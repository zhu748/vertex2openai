"""ASGI middleware which does not buffer or acknowledge streaming responses."""


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
        started = False

        async def relay(message):
            nonlocal started
            if message["type"] == "http.response.start":
                started = True
                if message["status"] >= 400:
                    stats.add_error()
            # Await the real server send, including the stream's DONE frame.
            await send(message)

        try:
            await self.app(scope, receive, relay)
        except Exception:
            if not started:
                stats.add_error()
            raise

"""Direct API streams cancel a backpressured sender, not just an in-progress read."""
import asyncio
import unittest
from unittest.mock import patch

from admission import ManagedStreamingResponse, generation_limiter, image_limiter
import channel_dispatch
from request_ingress import ingress_budget
from resumable import DONE_FRAME
from routes import claude_api
from test_replay_http_delivery import Peer


class DirectStreamSendTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(patch.object(channel_dispatch.app_state, "get_channel_mode", return_value="api_key"))
        self.enterContext(patch.object(channel_dispatch.app_state, "is_web_proxy_enabled", return_value=False))
        self.enterContext(patch.object(channel_dispatch, "_other_channel", return_value=None))

    async def exercise(self, dialect, spec, send_part, *, cancel=False):
        entered, closing, can_close, send_allowed, can_read = (asyncio.Event() for _ in range(5))
        closes = []
        async def upstream(*args, **kwargs):
            async def body():
                await can_read.wait()
                yield DONE_FRAME

            async def close():
                closes.append("start")
                closing.set()
                await can_close.wait()
                closes.append("end")
            return ManagedStreamingResponse(body(), media_type="text/event-stream", cleanup=close)

        model = "gemini-3-pro-image-preview"
        payload = {"model": model, "messages": [{"role": "user", "content": "hello"}], "stream": True}
        if dialect == "claude":
            payload["max_tokens"] = 16
        peer = Peer("/v1/messages" if dialect == "claude" else "/v1/chat/completions", spec=spec,
                    payload=payload, headers=[(b"x-resumable", b"false")])
        async def blocked(message):
            if message["type"] == send_part and not entered.is_set():
                entered.set()
                await send_allowed.wait()

        gen, img, ingress = generation_limiter.active, image_limiter.active, tuple(ingress_budget.used)
        with patch.object(claude_api, "_available_models", return_value=[model]), \
                patch.object(channel_dispatch.express_upstream, "chat_completions", side_effect=upstream):
            task = asyncio.create_task(peer.run(blocked))
            try:
                await asyncio.wait_for(entered.wait(), 1)
                await peer.drop()
                await asyncio.wait_for(closing.wait(), 1)
                self.assertEqual((generation_limiter.active, image_limiter.active), (gen + 1, img + 1))
                if cancel:
                    task.cancel()
                can_close.set()
                if cancel:
                    with self.assertRaises(asyncio.CancelledError):
                        await asyncio.wait_for(asyncio.shield(task), 1)
                else:
                    await asyncio.wait_for(asyncio.shield(task), 1)
            finally:
                can_close.set()
                can_read.set()
                send_allowed.set()
                await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(closes, ["start", "end"])
        self.assertEqual((generation_limiter.active, image_limiter.active, tuple(ingress_budget.used)),
                         (gen, img, ingress))
        self.assertEqual(peer.reads, 2)
        self.assertFalse(peer.discarded)

    async def test_observed_disconnect_stops_headers_and_first_body_without_another_upstream_pull(self):
        for dialect in ("openai", "claude"):
            for spec in ("2.4", "2.3"):
                for part in ("http.response.start", "http.response.body"):
                    with self.subTest(dialect=dialect, spec=spec, part=part):
                        await self.exercise(dialect, spec, part)

    async def test_external_cancel_after_disconnect_remains_cancel(self):
        for dialect in ("openai", "claude"):
            with self.subTest(dialect=dialect):
                await self.exercise(dialect, "2.4", "http.response.body", cancel=True)

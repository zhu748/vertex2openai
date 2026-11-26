"""A provider completion is not an ASGI receipt for a translated tool turn."""

import asyncio
import base64
from contextlib import ExitStack
import unittest

from starlette.requests import ClientDisconnect

import test_bounded_signature_origins as origins
from test_claude_omitted_signature_projection import new_cache
from test_parallel_tool_controls import _request
from test_replay_http_delivery import Peer
from test_schema_contract_fidelity import SchemaRouteHarness


CALL = "immutable-delivered-call"
SIG_A = b"\xfb\xffstored-first"
SIG_B = b"\xfewould-be-collision"


class SignatureDelivery(SchemaRouteHarness):
    echo = origins.BoundedOriginRoutes.assistant_echo

    async def replay(self, path, body, assistant):
        self.reply = {"text": "The result was read."}
        self.sent.clear()
        replay = await self.client.post(path, json={
            **body, "stream": False,
            "messages": [*body["messages"], assistant, *origins.results("claude", (CALL,), "ok")],
        })
        self.assertEqual(replay.status_code, 200, replay.text)
        self.assertEqual(len(self.sent), 1)
        part, = origins.wire_calls(self.sent[-1])
        self.assertEqual(part["functionCall"]["id"], CALL)
        wire = part.get("thoughtSignature")
        if wire is None:
            return None
        # A bypass placeholder is not the absence of a signature. Comparing
        # decoded wire bytes also allows for the pinned SDK's URL-safe alphabet.
        return base64.urlsafe_b64decode(wire)

    @staticmethod
    def provider_turn(signature):
        return [{"text": "checking data", "thought": True}, {"text": "Checking."},
                origins.provider_call(CALL, signature)]

    async def assert_public_delivery(self, mode, shown, stage, failure, spec):
        path, body = _request("claude", "auto", stream=mode != "json")
        body.update(model=origins.MODEL, messages=[{"role": "user", "content": "Weather?"}],
                    thinking={"type": "enabled", "budget_tokens": 1024,
                              "display": "summarized" if shown else "omitted"})
        self.settings["fake_streaming"] = mode == "fake"
        self.reply = self.provider_turn(SIG_A)
        first = await self.client.post(path, json=body)
        self.assertEqual(first.status_code, 200, first.text)
        assistant = self.echo(first, "claude", mode)
        self.assertEqual(await self.replay(path, body, assistant), SIG_A)

        peer = Peer(path, spec=spec, payload=body, headers=[(b"x-resumable", b"false")])
        self.reply = self.provider_turn(SIG_B)
        intercepted = []

        async def drop(message):
            if message["type"] != "http.response.body":
                return
            target = not message.get("more_body") if stage == "final" else b"event: message_stop" in message["body"]
            if not target:
                return
            intercepted.append(True)
            if failure == "raise":
                raise OSError("the ASGI peer closed")
            await peer.drop()

        if failure == "raise":
            with self.assertRaises((OSError, ClientDisconnect)):
                await asyncio.wait_for(peer.run(drop), 2)
        else:
            await asyncio.wait_for(peer.run(drop), 2)
        self.assertEqual(intercepted, [True])
        self.assertEqual(await self.replay(path, body, assistant), SIG_A)

        # An actually sent B has the same public history but not the same
        # original bytes. After it succeeds neither original may be borrowed.
        self.reply = self.provider_turn(SIG_B)
        accepted = await self.client.post(path, json=body)
        self.assertEqual(accepted.status_code, 200, accepted.text)
        self.assertEqual(self.echo(accepted, "claude", mode), assistant)
        self.assertIsNone(await self.replay(path, body, assistant))

    async def test_failed_or_silently_discarded_asgi_send_cannot_fence_an_earlier_delivery(self):
        self.settings["express_location"] = "global"
        self.key_manager.get_express_record = lambda **_: {"key": "test-key", "project_id": "fixture"}
        failures = (("2.4", "raise"), ("2.4", "silent"), ("2.3", "silent"))
        for self.channel in ("api_key", "web_proxy"):
            for mode in ("json", "fake", "stream"):
                for shown in (True, False):
                    for stage in (("final",) if mode == "json" else ("message_stop", "final")):
                        for spec, failure in failures:
                            with (self.subTest(channel=self.channel, mode=mode, shown=shown,
                                               stage=stage, failure=failure, spec=spec), ExitStack() as stack):
                                new_cache(stack)
                                await self.assert_public_delivery(mode, shown, stage, failure, spec)


if __name__ == "__main__":
    unittest.main()

"""An echoed Responses item ID identifies a delivered output, not a tool call."""

import asyncio
from contextlib import ExitStack
import json
from types import SimpleNamespace
from unittest.mock import patch

from starlette.requests import ClientDisconnect

import config
import signature_store
from test_claude_omitted_signature_projection import new_cache
from test_replay_http_delivery import Peer
from test_responses_api import PATH, assistant, calls_in_wire, continuation, event_list, request, signature, thin_item
from test_schema_contract_fidelity import SchemaRouteHarness


CALL_IDS = ("same-call", "call_delimited__thought__YQ==")
FIRST = (b"\xfb\xff-old-1", b"\x05-old-2")
SECOND = (b"\x00new-1", b"\x8cnew-2")


def no_item_id(item):
    return {k: v for k, v in item.items() if k != "id"}


class ResponsesItemWitnessTests(SchemaRouteHarness):
    async def issue(self, prompt, stream, signatures):
        self.reply = [assistant(call_id, sig, shade=shade) for call_id, sig, shade in
                      zip(CALL_IDS, signatures, ("blue", "red"), strict=True)]
        response = await self.client.post(PATH, json=request(input=prompt, stream=stream))
        self.assertEqual(response.status_code, 200, response.text[:220])
        if stream:
            terminal = event_list(response.text)[-1]
            self.assertEqual(terminal["type"], "response.completed")
            return terminal["response"]["output"]
        return response.json()["output"]

    async def assert_replay(self, prompt, public, expected):
        self.reply = {"text": "Fixed fixture."}
        self.sent.clear()
        result = await self.client.post(PATH, json=request(input=continuation(prompt, public, reverse=True)))
        self.assertEqual(result.status_code, 200, result.text[:220])
        wire, = self.sent
        ids = [item["call_id"] for item in public]
        calls = calls_in_wire(wire, "functionCall")
        outputs = calls_in_wire(wire, "functionResponse")
        self.assertEqual([p["functionCall"]["id"] for p in calls], ids)
        self.assertEqual([p["functionResponse"]["id"] for p in outputs], ids)
        self.assertEqual([signature(p) for p in calls], list(expected))

    async def test_exact_item_vectors_distinguish_emissions_but_never_replace_call_ids(self):
        for self.channel in ("api_key", "web_proxy"):
            for stream in (False, True):
                with self.subTest(channel=self.channel, stream=stream):
                    prompt = f"Verify witnessed output for {self.channel}:{stream}."
                    first = [thin_item(i) for i in await self.issue(prompt, stream, FIRST)]
                    await self.assert_replay(prompt, [no_item_id(i) for i in first], FIRST)
                    later_full = await self.issue(prompt, stream, SECOND)
                    later = [thin_item(i) for i in later_full]
                    self.assertEqual([no_item_id(i) for i in first], [no_item_id(i) for i in later])
                    self.assertTrue(set(i["id"] for i in first).isdisjoint(i["id"] for i in later))

                    await self.assert_replay(prompt, first, FIRST)
                    await self.assert_replay(prompt, later, SECOND)
                    await self.assert_replay(prompt, [no_item_id(i) for i in later], (None, None))
                    await self.assert_replay(prompt, [first[0], later[1]], (None, None))

                    # An edited standard item ID still cannot override a genuine extension.
                    thick = [{**later_full[0], "id": "some-client-item"}, later_full[1]]
                    await self.assert_replay(prompt, thick, SECOND)

    async def test_incomplete_unknown_or_changed_vector_cannot_use_positive_legacy_origin(self):
        for self.channel in ("api_key", "web_proxy"):
            for stream in (False, True):
                with self.subTest(channel=self.channel, stream=stream):
                    prompt = f"Preserve actual output for {self.channel}:{stream}."
                    first, second = [thin_item(i) for i in await self.issue(prompt, stream, FIRST)]
                    await self.assert_replay(prompt, [no_item_id(first), no_item_id(second)], FIRST)
                    for label, history in (
                        ("absent half", [first, no_item_id(second)]),
                        ("null half", [first, {**second, "id": None}]),
                        ("malformed hint", [{**first, "id": "some-client-item"}, second]),
                        ("unknown issued shape", [first, {**second, "id": "fc_" + "0" * 32}]),
                        ("duplicate", [first, {**second, "id": first["id"]}]),
                        ("swapped binding", [{**first, "id": second["id"]}, {**second, "id": first["id"]}]),
                        ("different call order", [second, first]),
                        ("different arguments", [{**first, "arguments": '{"shade":"cyan"}'}, second]),
                    ):
                        with self.subTest(edit=label):
                            await self.assert_replay(prompt, history, (None, None))
                    await self.assert_replay(prompt + " Edited.", [first, second], (None, None))

    async def test_expiration_withholds_and_keeps_the_whole_live_batch_including_an_unsigned_slot(self):
        for self.channel in ("api_key", "web_proxy"):
            with self.subTest(channel=self.channel), ExitStack() as stack:
                new_cache(stack)
                clock = SimpleNamespace(now=0)
                stack.enter_context(patch.object(signature_store, "time", SimpleNamespace(monotonic=lambda: clock.now)))
                prompt = f"Expired witness for {self.channel}."
                old = [thin_item(i) for i in await self.issue(prompt, False, FIRST)]
                clock.now = signature_store.DEFAULT_TTL_SECONDS + 1
                later = [thin_item(i) for i in await self.issue(prompt, False, (SECOND[0], None))]
                await self.assert_replay(prompt, old, (None, None))
                await self.assert_replay(prompt, later, (SECOND[0], None))
                # This is not a legacy miss: the visible batch has a fresh,
                # positive unanchored record whose bytes must not be borrowed.
                await self.assert_replay(prompt, [no_item_id(i) for i in later], (SECOND[0], None))
                with patch.object(config, "GEMINI_STRICT_THOUGHT_SIGNATURES", True):
                    await self.assert_replay(prompt, later, (SECOND[0], None))
                    self.sent.clear()
                    response = await self.client.post(PATH, json=request(input=continuation(prompt, old)))
                    self.assertEqual(response.status_code, 400, response.text[:200])
                    self.assertIn("signature", response.text)
                    self.assertEqual(self.sent, [])

    async def test_only_after_public_delivery_can_an_item_witness_authorize_replay(self):
        for self.channel in ("api_key", "web_proxy"):
            for stream in (False, True):
                for spec, failure in (("2.4", "raise"), ("2.4", "silent"), ("2.3", "silent")):
                    with self.subTest(channel=self.channel, stream=stream, spec=spec, failure=failure):
                        prompt = f"Sent receipt control {self.channel}:{stream}:{spec}:{failure}."
                        first = [thin_item(i) for i in await self.issue(prompt, stream, FIRST)]
                        self.reply = [assistant(call_id, sig, shade=shade) for call_id, sig, shade in
                                      zip(CALL_IDS, SECOND, ("blue", "red"), strict=True)]
                        peer = Peer(PATH, payload=request(input=prompt, stream=stream), spec=spec)
                        attempted = []

                        async def stop_at_final_body(message):
                            if message["type"] != "http.response.body" or message.get("more_body"):
                                return
                            attempted.append(message["body"])
                            if failure == "raise":
                                raise OSError("test peer not accepting final write")
                            await peer.drop()

                        if failure == "raise":
                            with self.assertRaises((OSError, ClientDisconnect)):
                                await asyncio.wait_for(peer.run(stop_at_final_body), 2)
                        else:
                            await asyncio.wait_for(peer.run(stop_at_final_body), 2)
                        body, = attempted
                        if stream:
                            raw = b"".join(m.get("body", b"") for m in peer.messages)
                            terminal = event_list(raw.decode())[-1]
                            self.assertEqual(terminal["type"], "response.completed")
                            abandoned = terminal["response"]["output"]
                        else:
                            abandoned = json.loads(body)["output"]
                        await self.assert_replay(prompt, [thin_item(i) for i in abandoned], (None, None))
                        await self.assert_replay(prompt, first, FIRST)
                        await self.assert_replay(prompt, [no_item_id(i) for i in first], FIRST)

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
    async def generate(self, history, stream, reply):
        self.reply = reply
        self.sent.clear()
        response = await self.client.post(PATH, json=request(input=history, stream=stream))
        self.assertEqual(response.status_code, 200, response.text[:220])
        if stream:
            terminal = event_list(response.text)[-1]
            self.assertEqual(terminal["type"], "response.completed")
            return terminal["response"]["output"]
        return response.json()["output"]

    async def issue(self, prompt, stream, signatures):
        return await self.generate(prompt, stream, [assistant(call_id, sig, shade=shade) for call_id, sig, shade in
                                   zip(CALL_IDS, signatures, ("blue", "red"), strict=True)])

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

    async def test_descendant_receipt_requires_the_delivered_predecessor_chain(self):
        for provider, fallback in (("api_key", False), ("web_proxy", False), ("api_key", True)):
            for stream in (False, True):
                with self.subTest(provider=provider, fallback=fallback, stream=stream):
                    prompt = f"Distinct lineages for {provider}:{fallback}:{stream}."
                    later_id = "same-later-call"
                    third, fourth = b"descendant of first", b"descendant of second"

                    def configure(producer, *, via_standby=False):
                        self.channel = "web_proxy" if via_standby else producer
                        self.settings["cross_channel_fallback"] = via_standby
                        self.status_by_provider = {"web_proxy": 503} if via_standby else {}

                    def check_wire(ids, expected, *, via_standby=fallback):
                        if via_standby:
                            self.assertEqual(len(self.sent), 2)
                            self.assertIn("variables", self.sent[0])
                            self.assertNotIn("variables", self.sent[1])
                        else:
                            self.assertEqual(len(self.sent), 1)
                        calls = calls_in_wire(self.sent[-1], "functionCall")
                        outputs = calls_in_wire(self.sent[-1], "functionResponse")
                        self.assertEqual([part["functionCall"]["id"] for part in calls], ids)
                        self.assertEqual([part["functionResponse"]["id"] for part in outputs], ids)
                        self.assertEqual([signature(part) for part in calls], list(expected))

                    async def branch(first_signatures, later_signature):
                        configure(provider)
                        first = [thin_item(item) for item in await self.issue(prompt, stream, first_signatures)]
                        configure(provider, via_standby=fallback)
                        history = continuation(prompt, first, reverse=True)
                        later, = await self.generate(history, stream, assistant(later_id, later_signature))
                        check_wire(list(CALL_IDS), first_signatures)
                        return first, later

                    async def follow(first, later, expected, *, other_channel=False):
                        via_standby = fallback and not other_channel
                        configure("web_proxy" if other_channel else provider, via_standby=via_standby)
                        history = [*continuation(prompt, first), later,
                                   {"type": "function_call_output", "call_id": later_id, "output": "ok"}]
                        await self.generate(history, stream, {"text": "Final fixture."})
                        check_wire([*CALL_IDS, later_id], expected, via_standby=via_standby)

                    first, later_full = await branch(FIRST, third)
                    later = thin_item(later_full)
                    await follow(first, later, (*FIRST, third))
                    await follow([no_item_id(item) for item in first], no_item_id(later), (*FIRST, third))
                    # Removing only the current IDs is not an entirely ID-less history.
                    with self.subTest(edit="drop only current IDs"):
                        await follow(first, no_item_id(later), (*FIRST, None))

                    second, newest_full = await branch(SECOND, fourth)
                    newest = thin_item(newest_full)
                    await follow(second, newest, (*SECOND, fourth))
                    await follow(first, later, (*FIRST, third))
                    with self.subTest(edit="splice branch A into branch B's continuation"):
                        await follow(first, newest, (*FIRST, None))
                    with self.subTest(edit="splice branch B into branch A's continuation"):
                        await follow(second, later, (*SECOND, None))
                    for edited in (
                        [no_item_id(item) for item in first],
                        [first[0], no_item_id(first[1])],
                        [first[0], {**first[1], "id": None}],
                        [first[0], {**first[1], "id": "fc_" + "0" * 32}],
                        [{**first[0], "id": first[1]["id"]}, {**first[1], "id": first[0]["id"]}],
                    ):
                        await follow(edited, later, (None, None, None))

                    # Receipt failure never overrides original bytes a client
                    # actually sends. The other channel still can't use cache.
                    await follow(second, later_full, (*SECOND, third))
                    if fallback:
                        await follow(first, later, (None, None, None), other_channel=True)

    async def test_unissued_prior_vector_cannot_anchor_a_new_exact_receipt(self):
        for self.channel in ("api_key", "web_proxy"):
            prompt = f"No prior gateway receipt ({self.channel})."
            prior = {"type": "function_call", "id": "fc_" + "0" * 32, "call_id": "external-call",
                     "name": "count_colors", "arguments": "{}"}
            history = continuation(prompt, [prior])
            later, = await self.generate(history, False, assistant("new-call", b"new developer fixture"))
            for current, expected in ((thin_item(later), None), (later, b"new developer fixture")):
                result = {"type": "function_call_output", "call_id": "new-call", "output": "done"}
                await self.generate([*history, current, result], False, {"text": "Final fixture."})
                calls = calls_in_wire(self.sent[-1], "functionCall")
                self.assertEqual([signature(part) for part in calls], [None, expected])

    async def test_expired_predecessor_does_not_anchor_a_still_live_descendant(self):
        self.channel = "api_key"
        with ExitStack() as stack:
            new_cache(stack)
            clock = SimpleNamespace(now=0)
            stack.enter_context(patch.object(signature_store, "time", SimpleNamespace(monotonic=lambda: clock.now)))
            prompt = "Bound the life of receipts nested under an older batch."
            predecessor = [thin_item(item) for item in await self.issue(prompt, False, FIRST)]
            history = continuation(prompt, predecessor)
            clock.now = 10
            body = assistant("child", b"later provider fixture")
            later, = await self.generate(history, False, body)
            result = {"type": "function_call_output", "call_id": "child", "output": "ok"}
            for now, expected in ((11, (*FIRST, b"later provider fixture")),
                                  (signature_store.DEFAULT_TTL_SECONDS + 5, (None, None, None))):
                clock.now = now
                await self.generate([*history, thin_item(later), result], False, {"text": "Okay."})
                calls = calls_in_wire(self.sent[-1], "functionCall")
                self.assertEqual([signature(part) for part in calls], list(expected))

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

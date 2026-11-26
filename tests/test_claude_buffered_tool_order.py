"""Finite fake SSE can restore source block order without exposing partial tool_use."""
import copy
import json
import unittest

from fastapi.responses import StreamingResponse

from claude_protocol import ClaudeStreamState
from routes.claude_api import _claude_stream_response
from response_validation import IncompleteResponseError
from test_schema_contract_fidelity import MESSAGES, MODEL, SchemaRouteHarness


def read_events(body):
    return [json.loads(line[6:]) for line in body.splitlines() if line.startswith("data: ")]


def reconstruct(test, events):
    result, current = [], None
    for event in events:
        kind = event["type"]
        if kind == "content_block_start":
            test.assertIsNone(current)
            test.assertEqual(event["index"], len(result))
            current = dict(event["content_block"])
            allowed = {"text": {"type", "text"}, "thinking": {"type", "thinking", "signature"},
                       "tool_use": {"type", "id", "name", "input"}}
            test.assertEqual(set(current), allowed[current["type"]])
        elif kind == "content_block_delta":
            test.assertIsNotNone(current)
            test.assertEqual(event["index"], len(result))
            delta = event["delta"]
            if delta["type"] == "input_json_delta":
                test.assertEqual(current["input"], {})
                current["input"] = json.loads(delta["partial_json"])
            else:
                key = delta["type"].removesuffix("_delta")
                current[key] += delta[key]
        elif kind == "content_block_stop":
            test.assertIsNotNone(current)
            test.assertEqual(event["index"], len(result))
            result.append(current)
            current = None
    test.assertIsNone(current)
    return result


def call(i):
    return {"functionCall": {"id": f"fixture-{i}", "name": "observe", "args": {"slot": i}}}


class PublicBufferedOrder(SchemaRouteHarness):
    async def test_fake_claude_matches_complete_wire_for_one_validated_parallel_batch(self):
        self.settings["fake_streaming"] = True
        # Ordinary thought is representable. A genuine call signature remains
        # on the internal call; never invent a public Claude signature carrier.
        self.reply = [{"text": "先"}, dict(call(1), thoughtSignature="+/8="), {"text": "中"},
                      call(2), {"text": "理由", "thought": True}, {"text": "末"}]
        body = {"model": MODEL, "messages": MESSAGES, "max_tokens": 64}
        for self.channel in ("api_key", "web_proxy"):
            for omitted in (False, True):
                with self.subTest(channel=self.channel, omitted=omitted):
                    opts = {"thinking": {"type": "enabled", "budget_tokens": 1024, "display": "omitted"}} if omitted else {}
                    whole = await self.client.post("/v1/messages", json={**body, **opts})
                    stream = await self.client.post("/v1/messages", json={**body, **opts, "stream": True})
                    self.assertEqual((whole.status_code, stream.status_code), (200, 200))
                    events = read_events(stream.text)
                    self.assertEqual((events[0]["type"], events[-1]["type"]), ("message_start", "message_stop"))
                    self.assertEqual([e["type"] for e in events].count("message_stop"), 1)
                    blocks = reconstruct(self, events)
                    self.assertEqual(blocks, whole.json()["content"])
                    self.assertEqual([b["text"] for b in blocks if b["type"] == "text"], ["先", "中", "末"])
                    self.assertEqual([b["input"] for b in blocks if b["type"] == "tool_use"], [{"slot": 1}, {"slot": 2}])
                    self.assertEqual(events[-2]["delta"]["stop_reason"], whole.json()["stop_reason"])
                    self.assertEqual(events[-2]["usage"], whole.json()["usage"])
                    self.assertNotIn("+/8=", stream.text)

    async def test_rejected_finite_batch_never_exposes_earlier_tool_use(self):
        self.settings["fake_streaming"] = True
        self.reply = [{"text": "before"}, call(1), {"text": "between"}, call(1)]
        for self.channel in ("api_key", "web_proxy"):
            with self.subTest(channel=self.channel):
                reply = await self.client.post("/v1/messages",
                    json={"model": MODEL, "messages": MESSAGES, "max_tokens": 64, "stream": True})
                events = read_events(reply.text)
                self.assertEqual(events[-1]["type"], "error")
                self.assertNotIn("tool_use", reply.text)
                self.assertNotIn("message_stop", reply.text)


def message():
    return {"role": "assistant", "content": "ab", "tool_calls": [
                {"type": "function", "id": f"id{i}", "function": {"name": "observe", "arguments": '{"ok": true}'}}
                for i in range(2)],
            "extra_content": {"google": {"ordinary_parts": [{"kind": "text", "text": c} for c in "ab"],
                "part_order": [{"type": k, "index": i} for i in range(2) for k in ("ordinary", "tool_call")]}}}


def preview(msg):
    return {"id": "owned", "choices": [{"message": msg, "finish_reason": "tool_calls"}]}


def chunk(msg, reason=None, source="owned"):
    return {"id": source, "choices": [{"delta": msg, "finish_reason": reason}]}


def indexed(msg):
    result = copy.deepcopy(msg)
    for i, tool in enumerate(result["tool_calls"]):
        tool["index"] = i
    return result


class BufferedFailureAfterData(unittest.IsolatedAsyncioTestCase):
    async def test_error_after_accepted_content_does_not_release_tools_or_false_success(self):
        msg, state = message(), ClaudeStreamState("model")
        self.assertTrue(state.select_buffered_order(preview(msg)))
        closed = []
        async def source():
            try:
                yield ": heartbeat\n\n"
                yield "data: " + json.dumps(chunk(indexed(msg))) + "\n\n"
                yield 'data: {"error":{"code":502,"message":"incomplete"}}\n\n'
            finally:
                closed.append(True)
        stream = _claude_stream_response(StreamingResponse(source()), "model", omit_thinking=False, state=state)
        records = read_events("".join([item async for item in stream.body_iterator]))
        self.assertEqual([r["type"] for r in records], ["message_start", "ping", "error"])
        self.assertEqual(closed, [True])


class AcceptedOnly(unittest.TestCase):
    def test_requires_finite_source_and_full_buffered_message(self):
        msg = message()
        live = ClaudeStreamState("model")
        # Exact same content without producer opt-in still streams ordinary text immediately.
        self.assertTrue(live.feed_openai_chunk(chunk(indexed(msg))))
        state = ClaudeStreamState("model")
        self.assertTrue(state.select_buffered_order(preview(msg)))
        self.assertEqual(state.feed_openai_chunk(chunk(indexed(msg))), [])
        with self.assertRaises(IncompleteResponseError):
            state.finish_events()
        with self.assertRaises(IncompleteResponseError):
            state.feed_openai_chunk(chunk({}, "tool_calls", source="different"))
        with self.assertRaises(IncompleteResponseError):
            state.feed_openai_chunk(chunk({}, "length"))

    def test_partial_ambiguous_or_missing_second_call_never_emits_tool(self):
        for edit in (lambda tc: tc["function"].update(arguments='{"broken":'),
                     lambda tc: tc["function"].update(arguments=""),
                     lambda tc: tc.update(id=""), lambda tc: tc.update(id="id0"),
                     lambda tc: tc.update(index=True), lambda tc: tc.update(index=0)):
            msg = message()
            state = ClaudeStreamState("model")
            self.assertTrue(state.select_buffered_order(preview(msg)))
            wire = indexed(msg)
            edit(wire["tool_calls"][1])
            with self.assertRaises(IncompleteResponseError):
                state.feed_openai_chunk(chunk(wire))

    def test_stale_topology_uses_original_visible_reply(self):
        state = ClaudeStreamState("model")
        msg = message()
        msg["extra_content"]["google"]["ordinary_parts"][1]["text"] = "stale"
        self.assertTrue(state.select_buffered_order(preview(msg)))
        first = state.feed_openai_chunk(chunk(indexed(msg)))
        self.assertIn('"text": "ab"', "".join(first))
        self.assertNotIn("stale", "".join(first))
        self.assertNotIn("tool_use", "".join(first))

    def test_signed_ordinary_media_unknown_or_oversized_stay_on_existing_path(self):
        for attr in ({"thought_signature": "+/8="}, {"kind": "image"}, {"kind": "signature_only"},
                     {"future": "field"}, {"text": "a" * 65537}):
            msg = message()
            msg["extra_content"]["google"]["ordinary_parts"][0].update(attr)
            self.assertFalse(ClaudeStreamState("model").select_buffered_order(preview(msg)))
        msg = message()
        msg["extra_content"]["google"]["part_order"][0]["index"] = True
        self.assertFalse(ClaudeStreamState("model").select_buffered_order(preview(msg)))

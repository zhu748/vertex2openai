"""Parallel function results retain positional meaning when upstream calls had no IDs."""
from google.genai import types

from message_processing import process_gemini_response_to_openai_dict
from models import OpenAIMessage
from test_assistant_refusal_history import HistoryWireHarness, MODEL
from tool_history import gemini_history_order
from test_req_round15_contracts import response
from test_tool_roundtrip_audit import wire_response
from upstreams.cookie_response import extract_events, openai_tool_call


class ParallelResultHistoryTests(HistoryWireHarness):
    async def test_idless_model_calls_receive_same_name_results_in_declared_order(self):
        raw = [{"name": "weather", "args": {"city": city}} for city in ("Paris", "Tokyo")]
        sdk_reply = response(*(types.Part(function_call=types.FunctionCall(**call)) for call in raw))
        sdk_calls = process_gemini_response_to_openai_dict(sdk_reply, MODEL)["choices"][0]["message"]["tool_calls"]
        calls = [d for kind, d in extract_events(wire_response([{"functionCall": c} for c in raw]))
                 if kind == "function_call"]
        cookie_calls = [openai_tool_call("offline", i, c) for i, c in enumerate(calls)]
        for tools in (sdk_calls, cookie_calls):
            ids = [c["id"] for c in tools]
            self.assertEqual(len(set(ids)), 2)
            messages = [
                {"role": "assistant", "tool_calls": tools},
                {"role": "tool", "tool_call_id": ids[1], "content": '{"temperature":20}'},
                {"role": "tool", "tool_call_id": ids[0], "content": '{"temperature":10}'},
                {"role": "user", "content": "compare"},
            ]
            for adapter in ("express", "cookie"):
                with self.subTest(adapter=adapter, ids=ids):
                    sent = await self.capture_history(messages, adapter)
                    self.assertEqual([m["role"] for m in sent], ["model", "user", "user"])
                    calls = [p["functionCall"] for p in sent[0]["parts"]]
                    results = [p["functionResponse"] for p in sent[1]["parts"]]
                    self.assertEqual([(c["args"]["city"], r["response"]["temperature"]) for c, r in zip(calls, results, strict=True)],
                                     [("Paris", 10), ("Tokyo", 20)])
                    self.assertEqual([r["id"] for r in results], ids)
                    self.assertEqual(sent[2]["parts"], [{"text": "compare"}])
            self.assertEqual([messages[i]["tool_call_id"] for i in (1, 2)], ids[::-1])

    def test_do_not_guess_partial_duplicate_unknown_or_interrupted_groups(self):
        def call(ident):
            return {"id": ident, "function": {"name": "weather", "arguments": "{}"}}

        def result(ident, **kwargs):
            return OpenAIMessage(role="tool", tool_call_id=ident, content="done", **kwargs)

        first = OpenAIMessage(role="assistant", tool_calls=[call("a"), call("b"), call("c")])
        a, b, c = (result(i) for i in ("a", "b", "c"))
        user = OpenAIMessage(role="user", content="new question")
        for messages in ([first, c, a], [first, c, a, a], [first, c, a, result("unknown")],
                         [first, c, b, user, a], [first, c, b, a, result("orphan")],
                         [first, c, b, result("a", name="other")],
                         [first, c, b, OpenAIMessage(role="function", name="weather", content="done")],
                         [OpenAIMessage(role="assistant", tool_calls=[call("a__thought__x"), call("a")]), b, a]):
            with self.subTest(messages=messages):
                self.assertEqual(list(gemini_history_order(messages)), messages)
        # Matching legacy IDs use the same public-call identity as the adapters;
        # a new assistant group with reused IDs can never reorder the prior run.
        second = OpenAIMessage(role="assistant", tool_calls=[call("c__thought__c2ln"), call("b")])
        self.assertEqual(list(gemini_history_order([first, c, a, second, b, c, user])),
                         [first, c, a, second, c, b, user])

        other_order = first.model_copy(update={"extra_content": {"google": {"part_order": [
            {"type": "tool_call", "index": 2}, {"type": "tool_call", "index": 0}]}}})
        self.assertEqual(list(gemini_history_order([other_order, b, a, c])), [other_order, c, a, b])
        self.assertEqual(list(gemini_history_order([other_order, b, a, c], native=False)), [other_order, a, b, c])

"""Only a genuine SDK call continuation can finish another in-flight call."""

import base64
import json
import unittest
from unittest import mock

from google.genai import types

from models import OpenAIRequest
from response_validation import ToolStreamError
from test_req_round15_contracts import Round15Harness
from test_tool_stream_deadline import call, chunk, fragment, parts
from tool_stream import ToolStreamAssembler


class ActiveCallSiblings(unittest.TestCase):
    def test_default_typed_call_coexists_with_real_ordinary_siblings(self):
        for default in ({}, {"name": "", "id": "", "args": {}, "partial_args": [], "will_continue": False}):
            for same_frame in (False, True):
                with self.subTest(default=default, same_frame=same_frame):
                    marker = types.FunctionCall(**default)
                    siblings = [
                        types.Part(text="progress ", function_call=marker),
                        types.Part(text="thinking", thought=True, thought_signature=b"thought", function_call=marker),
                        types.Part(file_data=types.FileData(file_uri="local://example", mime_type="text/plain"),
                                   thought_signature=b"file", function_call=marker),
                        types.Part(inline_data=types.Blob(data=b"\x00\xff", mime_type="image/png"),
                                   thought_signature=b"image", function_call=marker),
                    ]
                    header = call(name="lookup", id="one", will_continue=True,
                                  partial_args=[fragment("$.query", "Par", continuing=True)])
                    header.thought_signature = b"call"
                    end = call(id="one", partial_args=[fragment("$.query", "is")], will_continue=False)
                    stream = ToolStreamAssembler()
                    result = []
                    frames = ([chunk(header, *siblings, end, finish="STOP")] if same_frame else
                              [chunk(header), *(chunk(item) for item in siblings), chunk(end, finish="STOP")])
                    for frame in frames:
                        result.extend(parts(stream.restore_sdk(frame)))
                    stream.validate_end()
                    self.assertEqual(len(result), 1 + len(siblings))
                    self.assertEqual(result[0].function_call.name, "lookup")
                    self.assertEqual(result[0].function_call.id, "one")
                    self.assertEqual(result[0].function_call.args, {"query": "Paris"})
                    self.assertEqual(result[0].thought_signature, b"call")
                    self.assertEqual(result[1:], siblings)

    def test_genuine_closer_and_late_signature_keep_their_owner(self):
        for sig in (None, b"late"):
            stream = ToolStreamAssembler()
            header = call(name="lookup", id="one", will_continue=True,
                          partial_args=[fragment("$.flag", False, kind="bool_value")])
            other = types.Part(text="ordinary", thought_signature=b"separate", function_call=types.FunctionCall())
            close = call(will_continue=False)
            close.thought_signature = sig
            self.assertEqual(parts(stream.restore_sdk(chunk(header, other))), [])
            completed, adjacent = parts(stream.restore_sdk(chunk(close, finish="STOP")))
            stream.validate_end()
            self.assertEqual((completed.function_call.id, completed.function_call.args, completed.thought_signature),
                             ("one", {"flag": False}, sig))
            self.assertEqual(adjacent, other)

    def test_ordinary_sibling_does_not_mask_a_real_malformed_call_or_unclosed_arguments(self):
        for end in (types.Part(text="alongside a real malformed call", function_call=types.FunctionCall(
                        id="one", args={"not": "incremental"})),
                    types.Part(text="ordinary, not a valid stop", function_call=types.FunctionCall()),
                    types.Part(thought_signature=b"ambiguous", function_call=types.FunctionCall())):
            stream = ToolStreamAssembler()
            header = call(name="lookup", id="one", will_continue=True,
                          partial_args=[fragment("$.query", "Par", continuing=True)])
            stream.restore_sdk(chunk(header))
            with self.subTest(end=end), self.assertRaises(ToolStreamError):
                stream.restore_sdk(chunk(end, finish="STOP"))

class ActiveCallDelivery(Round15Harness):
    async def test_real_stream_exposes_only_completed_call_then_separate_signed_text_on_both_model_families(self):
        self.settings["fake_streaming"] = False
        for model in ("gemini-2.5-flash", "gemini-3-flash-preview"):
            with self.subTest(model=model):
                async def frames():
                    ordinary = types.Part(text="done", thought_signature=b"visible", function_call=types.FunctionCall())
                    if model == "gemini-2.5-flash":
                        # This family negotiates complete calls, not partialArgs.
                        complete = call(name="lookup", id="known", args={"query": "Paris"})
                        complete.thought_signature = b"call"
                        yield chunk(complete, ordinary, finish="STOP")
                        return
                    header = call(name="lookup", id="known", will_continue=True,
                                  partial_args=[fragment("$.query", "Par", continuing=True)])
                    header.thought_signature = b"call"
                    yield chunk(header)
                    yield chunk(ordinary)
                    yield chunk(call(id="known", partial_args=[fragment("$.query", "is")]), finish="STOP")

                req = OpenAIRequest(model=model, stream=True, messages=[{"role": "user", "content": "x"}],
                                    tools=[{"type": "function", "function": {"name": "lookup"}}], tool_choice="required")
                provider = mock.AsyncMock(return_value=frames())
                result = await self.express(req, stream=provider)
                self.assertEqual(result.status_code, 200)
                body = await self.body(result)
                flags = provider.await_args.kwargs["config"]["tool_config"]["function_calling_config"]
                self.assertEqual(bool(flags.get("stream_function_call_arguments")), model != "gemini-2.5-flash")
                self.assertTrue(body.rstrip().endswith("data: [DONE]"))
                fields = [json.loads(s[6:]) for s in body.splitlines() if s.startswith("data: {")]
                self.assertTrue(all("error" not in f for f in fields))
                choices = [choice for frame in fields for choice in frame.get("choices", [])]
                deltas = [c["delta"] for c in choices]
                calls = [tc for d in deltas for tc in d.get("tool_calls", [])]
                self.assertEqual([tc["id"] for tc in calls if tc.get("id")], ["known"])
                self.assertEqual([tc["function"]["name"] for tc in calls if tc["function"].get("name")], ["lookup"])
                self.assertEqual(json.loads("".join(tc["function"].get("arguments", "") for tc in calls)), {"query": "Paris"})
                self.assertEqual([base64.b64decode(tc["extra_content"]["google"]["thought_signature"])
                                  for tc in calls if tc.get("extra_content")], [b"call"])
                self.assertEqual("".join(d.get("content", "") for d in deltas), "done")
                self.assertEqual([c["finish_reason"] for c in choices if c.get("finish_reason")], ["tool_calls"])
                topology = next(d["extra_content"]["google"] for d in deltas
                                if "ordinary_parts" in d.get("extra_content", {}).get("google", {}))
                self.assertEqual(topology["part_order"], [{"type": "tool_call", "index": 0}, {"type": "ordinary", "index": 0}])
                self.assertEqual(base64.b64decode(topology["ordinary_parts"][0]["thought_signature"]), b"visible")

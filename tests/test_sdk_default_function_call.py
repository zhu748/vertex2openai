"""Only real calls and active continuations represent a nonempty SDK tool message."""
import json

from google.genai import types

from first_reply import sdk_has_reply
from message_processing import process_gemini_response_to_openai_dict
from test_parallel_tool_controls import MODES, _request
from test_schema_contract_fidelity import MODEL, SchemaRouteHarness
from test_tool_stream_deadline import call, chunk, parts
from tool_stream import ToolStreamAssembler
from upstreams.express_response import is_gemini_response_valid


class SDKDefaultCalls(SchemaRouteHarness):
    async def test_empty_call_sibling_is_not_a_tool_or_an_error(self):
        for protocol in ("openai", "claude"):
            for stream, fake in MODES:
                self.settings["fake_streaming"] = fake
                path, body = _request(protocol, "auto", stream=stream)
                for sdk, bad in (({}, False), ({"id": "", "name": "", "partialArgs": [],
                                                "willContinue": False, "args": {}}, False),
                                 ({"args": {"result": 1}}, True), ({"id": "nonempty"}, True)):
                    with self.subTest(protocol=protocol, stream=stream, fake=fake, sdk=sdk):
                        self.reply = [{"text": "true text"}, {"functionCall": sdk}]
                        self.sent.clear()
                        r = await self.client.post(path, json=body)
                        if bad:
                            self.assertTrue(r.status_code == 502 or '"error":' in r.text)
                        else:
                            self.assertEqual(r.status_code, 200)
                            if stream and protocol == "claude":
                                frames = [json.loads(line[6:]) for line in r.text.splitlines() if line.startswith("data: ")]
                                self.assertEqual("".join(f.get("delta", {}).get("text", "") for f in frames), "true text")
                            else:
                                self.assertIn("true text", r.text)
                            self.assertNotIn('"error":', r.text)
                            self.assertNotIn('"type":"tool_use"', r.text)
                        self.assertEqual(len(self.sent), 1)

    def test_empty_alone_is_not_progress_and_active_end_still_completes(self):
        empty = call()
        self.assertFalse(is_gemini_response_valid(chunk(empty)))
        self.assertFalse(sdk_has_reply(chunk(empty)))
        signed = empty.model_copy(update={"thought_signature": b"sig"})
        result = process_gemini_response_to_openai_dict(chunk(types.Part(text="ok"), signed, finish="STOP"), MODEL)
        msg = result["choices"][0]["message"]
        self.assertNotIn("tool_calls", msg)
        self.assertEqual(msg["extra_content"]["google"]["ordinary_parts"][-1]["kind"], "signature_only")
        assembler = ToolStreamAssembler()
        self.assertEqual(parts(assembler.restore_sdk(chunk(call(name="reply", id="i", will_continue=True)))), [])
        finished = parts(assembler.restore_sdk(chunk(call(will_continue=False), finish="STOP")))
        self.assertEqual([(p.function_call.id, p.function_call.name) for p in finished], [("i", "reply")])
        assembler.validate_end()

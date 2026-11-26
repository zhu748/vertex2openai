"""Rejected complete provider replies must be invisible to thin tool histories."""
from unittest.mock import patch

import pytest
from google.genai import types

from message_processing import convert_to_openai_format
from response_validation import IncompleteResponseError
from signature_store import SignatureRecord, SignatureState, signature_store
from test_parallel_tool_controls import _request
from test_schema_contract_fidelity import MODEL, SchemaRouteHarness


def call(name, ident=None, sig=None):
    return types.Part(function_call=types.FunctionCall(id=ident, name=name, args={"city": "Paris"}),
                      thought_signature=sig)


def response(*choices):
    return types.GenerateContentResponse(candidates=[
        types.Candidate(index=i, content=types.Content(role="model", parts=parts), finish_reason="STOP")
        for i, parts in enumerate(choices)
    ])


@pytest.mark.parametrize("later_candidate", [False, True])
def test_invalid_tool_batch_does_not_register_even_generated_ids(later_candidate):
    ident = "complete-rejected-candidate"
    signature_store.register_call_signature(ident, b"old")
    valid = [call("weather", ident, b"new"), call("weather")]
    invalid = [call(None, "missing-name")]
    incomplete = response(valid, invalid) if later_candidate else response(valid + invalid)
    with patch.object(signature_store, "register_call_signature",
                      wraps=signature_store.register_call_signature) as register:
        with pytest.raises(IncompleteResponseError):
            convert_to_openai_format(incomplete, MODEL)
        register.assert_not_called()
    assert signature_store.get_record(ident) == SignatureRecord(SignatureState.SIGNED, b"old")


def test_complete_batch_registers_exact_emitted_ids_and_topology_only():
    complete = response([call("weather"), call("weather")], [call("weather", "complete-signed", b"new")])
    result = convert_to_openai_format(complete, MODEL)
    first, second = (c["id"] for c in result["choices"][0]["message"]["tool_calls"])
    signed = result["choices"][1]["message"]["tool_calls"][0]["id"]
    assert first and second and first != second
    assert signed == "complete-signed"
    assert [signature_store.get_record(i) for i in (first, second, signed)] == [
        SignatureRecord(SignatureState.UNKNOWN), SignatureRecord(SignatureState.UNSIGNED_FOLLOWER),
        SignatureRecord(SignatureState.SIGNED, b"new")]
    with patch.object(signature_store, "register_call_signature") as register:
        convert_to_openai_format(response([types.Part(text="no tools")]), MODEL)
        convert_to_openai_format(response([]), MODEL)
        register.assert_not_called()


class CompletedSignatureRoutes(SchemaRouteHarness):
    async def test_malformed_provider_batch_preserves_existing_signature_on_every_delivery_path(self):
        for model in (MODEL, "gemini-3-flash-preview"):
            await self.check_delivery_paths(model)

    async def check_delivery_paths(self, model):
        self.settings["max_retries"] = 0
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for protocol in ("openai", "claude"):
                for mode in ("json", "fake", "stream"):
                    with self.subTest(model=model, channel=channel, protocol=protocol, mode=mode):
                        ident = f"retained-{model}-{channel}-{protocol}-{mode}"
                        self.settings["fake_streaming"] = mode == "fake"
                        path, body = _request(protocol, "auto", stream=mode != "json")
                        body["model"] = model
                        def wire(sig):
                            return call("weather", ident, sig).model_dump(mode="json", by_alias=True, exclude_none=True)

                        self.sent.clear()
                        self.reply = [wire(b"old")]
                        good = await self.client.post(path, json=body)
                        self.assertEqual(good.status_code, 200, good.text)
                        self.assertIn(ident, good.text)
                        previous = SignatureRecord(SignatureState.SIGNED, b"old")
                        self.assertEqual(signature_store.get_record(ident), previous)
                        self.reply = [wire(b"new"), {"functionCall": {"args": {"city": "Tokyo"}}}]
                        bad = await self.client.post(path, json=body)
                        self.assertEqual(len(self.sent), 2)
                        if mode == "json":
                            self.assertEqual(bad.status_code, 502, bad.text)
                        else:
                            self.assertIn('"error":', bad.text)
                        self.assertNotIn(ident, bad.text)
                        self.assertEqual(signature_store.get_record(ident), previous)

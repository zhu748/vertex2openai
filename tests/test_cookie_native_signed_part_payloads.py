"""The native bridge must keep a signature on its original public payload."""

from copy import deepcopy
from unittest.mock import patch

from test_req_round15_contracts import chunks
from test_schema_contract_fidelity import SchemaRouteHarness
from upstreams import cookie_native

MODEL = "gemini-3.6-flash"
ROUTE = f"/v1beta/models/{MODEL}:"
ACTIONS = ("generateContent", "streamGenerateContent?alt=sse", "streamGenerateContent")
PROMPT = {"role": "user", "parts": [{"text": "Read offline results."}]}
FUNCTION = {"functionCall": {"name": "read_status", "id": "one", "args": {"dry": True}},
            "thoughtSignature": "AgM="}
SERVER_TOOL = [
    {"toolCall": {"id": "search", "toolType": "GOOGLE_SEARCH_WEB", "args": {"query": "local fixture"}},
     "thoughtSignature": "+/8="},
    {"toolResponse": {"id": "search", "toolType": "GOOGLE_SEARCH_WEB", "response": {"matches": []}},
     "thoughtSignature": "AwQ="},
]
CODE = [
    {"executableCode": {"id": "fixture-id", "language": "PYTHON", "code": "1 + 1"},
     "thoughtSignature": "BAU="},
    {"codeExecutionResult": {"id": "fixture-id", "outcome": "OUTCOME_OK", "output": ""},
     "thoughtSignature": "BQY="},
]
FUNCTION_RESPONSE = {"functionResponse": {
    "id": "tool-result", "name": "read_status", "response": {"toolResponse": {}, "tool_response": []},
    "parts": [{"inlineData": {"data": "AQI=", "mimeType": "image/png", "displayName": "local-blob"}},
              {"fileData": {"fileUri": "gs://offline/example", "mimeType": "image/png", "displayName": "local-file"}}],
    "willContinue": False}, "thoughtSignature": "Bgc="}
MEDIA = {"inlineData": {"data": "AQI=", "mimeType": "video/webm", "displayName": "local-media"},
         "videoMetadata": {"startOffset": "1s", "endOffset": "2s", "fps": 1.5},
         "mediaResolution": {"level": "MEDIA_RESOLUTION_LOW", "numTokens": 1},
         "thoughtSignature": "Bwg="}
METADATA = {"text": "literal", "partMetadata": {"opaque_key": {"toolResponse": "literal value"}},
            "thoughtSignature": "CQo="}
EMPTY = {"text": "", "thought": True, "thoughtSignature": "Cgs="}
ABSENT = {"thoughtSignature": "Cww="}


def frames(response, action):
    if "?alt=sse" in action:
        return chunks(response.text)
    value = response.json()
    return value if isinstance(value, list) else [value]


def model_parts(response, action):
    records = frames(response, action)
    assert all("error" not in record for record in records), records
    return [p for record in records for candidate in record["candidates"]
            for p in candidate.get("content", {}).get("parts", ())]


class NativeSignedPayloads(SchemaRouteHarness):
    async def test_all_native_framings_echo_each_original_part_and_signature(self):
        self.channel = "web_proxy"
        cases = (SERVER_TOOL, CODE, [FUNCTION_RESPONSE], [MEDIA], [METADATA], [EMPTY, ABSENT])
        for action in ACTIONS:
            for original in cases:
                with self.subTest(action=action, owning_field=next(iter(original[0]))):
                    expected = [*original, FUNCTION]
                    self.reply = deepcopy(expected)
                    first = await self.client.post(ROUTE + action, json={"contents": [PROMPT]})
                    self.assertEqual(first.status_code, 200, first.text)
                    parts = model_parts(first, action)
                    self.assertEqual(parts, expected)
                    self.reply = {"text": "continued"}
                    end = await self.client.post(ROUTE + "generateContent", json={"contents": [
                        PROMPT, {"role": "model", "parts": parts},
                        {"role": "user", "parts": [{"functionResponse": {
                            "name": "read_status", "id": "one", "response": {"ok": True}}}]},
                    ]})
                    self.assertEqual(end.status_code, 200, end.text)
                    self.assertEqual(self.sent[-1]["variables"]["contents"][1]["parts"], expected)

    async def test_private_envelope_fields_defaults_and_empty_payloads_do_not_leak(self):
        self.channel = "web_proxy"
        original = deepcopy(SERVER_TOOL[0])
        original["toolCall"]["privateTrace"] = "unknown nested field"
        original["inlineData"] = {"data": "", "mimeType": ""}
        original["fileData"] = None
        original["text"] = ""
        original["videoMetadata"] = {"startOffset": {"seconds": "0", "nanos": 0}}
        original["privateTrace"] = "unknown top-level field"
        for action in ACTIONS:
            with self.subTest(action=action):
                self.reply = [original, {"toolCall": {"toolType": "TOOL_TYPE_UNSPECIFIED", "args": {}},
                                         "executableCode": {}, "functionResponse": {"id": "", "name": ""},
                                         "partMetadata": {"ignored": "no payload"}}]
                reply = await self.client.post(ROUTE + action, json={"contents": [PROMPT]})
                self.assertEqual(reply.status_code, 200, reply.text)
                self.assertEqual(model_parts(reply, action), [SERVER_TOOL[0]])
                self.assertNotIn("privateTrace", reply.text)

    async def test_malformed_public_payload_cannot_publish_an_orphan_signature(self):
        self.channel = "web_proxy"
        for action in ACTIONS:
            with self.subTest(action=action):
                self.reply = [{"toolCall": {"toolType": "GOOGLE_SEARCH_WEB", "args": []},
                               "thoughtSignature": "+/8="}, FUNCTION]
                reply = await self.client.post(ROUTE + action, json={"contents": [PROMPT]})
                errors = [error for record in frames(reply, action) if (error := record.get("error"))]
                self.assertEqual([error["code"] for error in errors], [502])
                self.assertIn("toolCall.args", errors[0]["message"])
                self.assertNotIn("thoughtSignature", reply.text)

    async def test_restored_fields_count_toward_existing_response_budget(self):
        self.channel = "web_proxy"
        part = deepcopy(SERVER_TOOL[1])
        part["toolResponse"]["response"] = {"result": "x" * 4096}
        self.reply = [part]
        for action in ACTIONS:
            with self.subTest(action=action), patch.object(cookie_native, "_bridge_response_budget", return_value=512):
                reply = await self.client.post(ROUTE + action, json={"contents": [PROMPT]})
                errors = [error for record in frames(reply, action) if (error := record.get("error"))]
                self.assertEqual([error["code"] for error in errors], [502])
                self.assertNotIn("thoughtSignature", reply.text)

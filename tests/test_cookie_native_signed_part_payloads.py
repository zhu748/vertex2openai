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
SPEECH = {"text": "Test narration", "speechMetadata": {"speaker": "fixture", "style": "calm"},
          "thoughtSignature": "EBE="}
AUDIO = {"inlineData": {"mimeType": "audio/wav", "data": "AQI="},
         "audioTranscription": {"text": "Good day", "finished": False, "languageCode": "en",
                                "speakerLabel": "speaker-1", "startOffset": "0s", "endOffset": "1s",
                                "words": [{"word": "Good", "startOffset": "0s", "endOffset": "0.5s"},
                                          {"word": "day", "startOffset": "0.5s", "endOffset": "1s"}]},
         "mediaProcessing": "STATIC", "thoughtSignature": "EhM="}
MEDIA_FILE = {"fileData": {"fileUri": "gs://offline/clip", "mimeType": "video/webm"},
              "mediaProcessing": "AGENTIC", "thoughtSignature": "FBU="}
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
        cases = (SERVER_TOOL, CODE, [FUNCTION_RESPONSE], [MEDIA], [METADATA], [EMPTY, ABSENT],
                 [SPEECH, AUDIO, MEDIA_FILE])
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

    async def test_optional_media_metadata_requires_a_typed_owner_and_meaningful_value(self):
        self.channel = "web_proxy"
        audio = deepcopy(AUDIO)
        audio["audioTranscription"]["internalTrace"] = "private"
        audio["audioTranscription"]["words"][0]["internalOffset"] = {"seconds": "0"}
        audio["speechMetadata"] = deepcopy(SPEECH["speechMetadata"])  # wrong owner
        audio_core = {key: AUDIO[key] for key in ("inlineData", "thoughtSignature")}
        audio_default = {**audio_core, "mediaProcessing": "MEDIA_PROCESSING_UNSPECIFIED",
                         "audioTranscription": {"text": "", "finished": False, "words": [],
                                                "startOffset": {"seconds": "0"}}}
        audio_malformed = {**audio_core, "mediaProcessing": ["STATIC"], "audioTranscription": 7}
        text_core = {"text": "plain", "thoughtSignature": "Fhc="}
        text = {**text_core, "speechMetadata": {"speaker": "", "style": {}},
                "audioTranscription": deepcopy(AUDIO["audioTranscription"]), "mediaProcessing": "STATIC"}
        self.reply = [audio, audio_default, audio_malformed, text,
                      {"speechMetadata": SPEECH["speechMetadata"], "mediaProcessing": "STATIC"}, FUNCTION]
        for action in ACTIONS:
            with self.subTest(action=action):
                reply = await self.client.post(ROUTE + action, json={"contents": [PROMPT]})
                self.assertEqual(reply.status_code, 200, reply.text)
                self.assertEqual(model_parts(reply, action), [AUDIO, audio_core, audio_core, text_core, FUNCTION])

    async def test_audio_transcription_protojson_normalizes_only_typed_aliases(self):
        self.channel = "web_proxy"
        part = {"inline_data": {"mime_type": "audio/wav", "data": "AQI="},
                "audio_transcription": {"text": "Word", "language_code": "en", "speaker_label": "speaker-1",
                                        "start_offset": "0s", "end_offset": "1s",
                                        "words": [{"word": "Word", "start_offset": "0s", "end_offset": "1s"}]},
                "media_processing": "STATIC", "thought_signature": "EhM=",
                "part_metadata": {"words": [{"start_offset": "leave literal"}]}}
        body = {"contents": [PROMPT, {"role": "model", "parts": [part]}, PROMPT]}
        reply = await self.client.post(ROUTE + "generateContent", json=body)
        self.assertEqual(reply.status_code, 200, reply.text)
        expected = {"inlineData": AUDIO["inlineData"], "mediaProcessing": "STATIC", "thoughtSignature": "EhM=",
                    "audioTranscription": {"text": "Word", "languageCode": "en", "speakerLabel": "speaker-1",
                                           "startOffset": "0s", "endOffset": "1s",
                                           "words": [{"word": "Word", "startOffset": "0s", "endOffset": "1s"}]},
                    "partMetadata": part["part_metadata"]}
        self.assertEqual(self.sent[-1]["variables"]["contents"][1]["parts"], [expected])
        self.sent.clear()
        part["audio_transcription"]["words"][0]["startOffset"] = "3s"
        reply = await self.client.post(ROUTE + "generateContent", json=body)
        self.assertEqual(reply.status_code, 400, reply.text)
        self.assertIn("startOffset", reply.json()["error"]["message"])
        self.assertEqual(self.sent, [])

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

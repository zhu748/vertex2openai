"""Keep each genuine empty Cookie thought in its own slot through replay."""
import base64

from test_req_round15_contracts import chunks
from test_schema_contract_fidelity import SchemaRouteHarness


MODEL = "gemini-3-flash-preview"
INPUT = {"role": "user", "content": "local fixture"}
PRIVATE = {"text": "visible thought", "thought": True, "thoughtSignature": "AQI="}
VISIBLE = {"text": "shown", "thoughtSignature": "AgM="}
EMPTY_THOUGHT = {"text": "", "thought": True, "thoughtSignature": "+/8="}
EMPTY_TEXT = {"text": "", "thoughtSignature": "AwQ="}
ABSENT_TEXT = {"thoughtSignature": "BAU="}


def signed_shape(parts):
    return [(part.get("text"), part.get("thought") is True,
             base64.urlsafe_b64decode(part["thoughtSignature"] + "==="))
            for part in parts if "thoughtSignature" in part]


def assistant(response, streamed):
    if not streamed:
        return response.json()["choices"][0]["message"]
    message = {"role": "assistant", "content": "", "reasoning_content": ""}
    for frame in chunks(response.text):
        assert "error" not in frame, frame
        for choice in frame["choices"]:
            delta = choice["delta"]
            for key in ("content", "reasoning_content"):
                message[key] += delta.get(key) or ""
            if "extra_content" in delta:
                # A harness may replace the extension with the last snapshot.
                message["extra_content"] = delta["extra_content"]
    return message


class EmptySignedCookieParts(SchemaRouteHarness):
    async def test_compat_round_trip_retains_genuine_empty_signed_thought_on_its_own_part(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for stream, fake in ((False, False), (True, False), (True, True)):
                with self.subTest(channel=channel, stream=stream, fake=fake):
                    self.settings["fake_streaming"] = fake
                    self.reply = [PRIVATE, EMPTY_THOUGHT, VISIBLE, EMPTY_TEXT]
                    start = await self.client.post("/v1/chat/completions", json={
                        "model": MODEL, "stream": stream, "messages": [INPUT]})
                    self.assertEqual(start.status_code, 200, start.text)
                    history = assistant(start, stream)
                    self.assertEqual(history["content"], VISIBLE["text"])
                    self.assertEqual(history["reasoning_content"], PRIVATE["text"])
                    self.reply = {"text": "continued"}
                    end = await self.client.post("/v1/chat/completions", json={
                        "model": MODEL, "messages": [INPUT, history, {"role": "user", "content": "continue"}]})
                    self.assertEqual(end.status_code, 200, end.text)
                    wire = self.sent[-1].get("variables", self.sent[-1])
                    parts = [part for turn in wire["contents"] if turn["role"] == "model"
                             for part in turn["parts"]]
                    self.assertEqual(signed_shape(parts), signed_shape([PRIVATE, EMPTY_THOUGHT, VISIBLE, EMPTY_TEXT]))
                    self.assertEqual(len(parts), 4)

    async def test_native_cookie_retains_explicit_empty_without_inventing_missing_text(self):
        self.channel = "web_proxy"
        for action in ("generateContent", "streamGenerateContent?alt=sse"):
            with self.subTest(action=action):
                self.reply = [VISIBLE, EMPTY_THOUGHT, EMPTY_TEXT, ABSENT_TEXT]
                route = f"/v1beta/models/{MODEL}:"
                prompt = {"role": "user", "parts": [{"text": "local fixture"}]}
                first = await self.client.post(route + action, json={"contents": [prompt]})
                self.assertEqual(first.status_code, 200, first.text)
                frames = chunks(first.text) if "alt=sse" in action else [first.json()]
                model_parts = [part for frame in frames for candidate in frame["candidates"]
                               for part in candidate.get("content", {}).get("parts", ())]
                self.assertEqual(model_parts, self.reply)
                self.reply = {"text": "continued"}
                end = await self.client.post(route + "generateContent", json={"contents": [
                    prompt, {"role": "model", "parts": model_parts},
                    {"role": "user", "parts": [{"text": "continue"}]}]})
                self.assertEqual(end.status_code, 200, end.text)
                self.assertEqual(self.sent[-1]["variables"]["contents"][1]["parts"], model_parts)

    async def test_cookie_hidden_nonempty_thought_remains_private(self):
        self.channel = "web_proxy"
        self.settings["native_thinking_mode"] = "off"
        for stream in (False, True):
            with self.subTest(stream=stream):
                self.reply = [VISIBLE, PRIVATE, EMPTY_THOUGHT]
                first = await self.client.post("/v1/chat/completions", json={
                    "model": MODEL, "stream": stream, "messages": [INPUT]})
                self.assertEqual(first.status_code, 200, first.text)
                self.assertNotIn(PRIVATE["text"], first.text)
                parts = assistant(first, stream)["extra_content"]["google"]["ordinary_parts"]
                self.assertEqual([(part["kind"], part["thought_signature"]) for part in parts],
                                 [("text", "AgM="), ("signature_only", "AQI="), ("thought", "+/8=")])

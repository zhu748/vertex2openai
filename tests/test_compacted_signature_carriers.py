"""Real outgoing Google SDK and Studio requests keep signatures on their own Parts."""
import base64
import copy

from test_responses_api import MODEL, assistant, continuation, request, signature
from test_schema_contract_fidelity import SchemaRouteHarness


class CompactedSignatureCarriers(SchemaRouteHarness):
    async def chat(self, messages, *, with_tool=False):
        body = {"model": MODEL, "messages": messages}
        if with_tool:
            body["tools"] = [{"type": "function", "function": {
                "name": "count_colors", "parameters": {"type": "object"}}}]
        response = await self.client.post("/v1/chat/completions", json=body)
        self.assertEqual(response.status_code, 200, response.text[:240])
        return response.json()["choices"][0]["message"]

    def model_parts(self):
        wire = self.sent[-1].get("variables", self.sent[-1])
        return [p for turn in wire["contents"] if turn["role"] == "model" for p in turn["parts"]]

    def assert_tool(self, ident, expected_signature):
        call, = (p for p in self.model_parts() if "functionCall" in p)
        self.assertEqual(call["functionCall"]["id"], ident)
        self.assertEqual(signature(call), expected_signature)
        wire = self.sent[-1].get("variables", self.sent[-1])
        result, = (p for turn in wire["contents"] for p in turn["parts"] if "functionResponse" in p)
        self.assertEqual(result["functionResponse"]["id"], ident)
        self.assertEqual(result["functionResponse"]["name"], call["functionCall"]["name"])
        self.assertNotIn("thoughtSignature", result)

    async def test_compacted_chat_typed_carriers_do_not_move_to_another_part(self):
        text = "provider's actual ordinary text"
        reasoning = "provider's actual thought"
        for self.channel in ("api_key", "web_proxy"):
            for with_tool in (False, True):
                for kind in ("text", "thought", "signature_only", None):
                    with self.subTest(channel=self.channel, with_tool=with_tool, kind=kind):
                        prompt = [{"role": "user", "content": f"offline {self.channel}/{with_tool}/{kind}"}]
                        badge = f"original carrier {kind}".encode()
                        encoded = base64.b64encode(badge).decode()
                        source = [{"text": reasoning, "thought": True}, {"text": text}]
                        if kind == "signature_only":
                            source.append({"thoughtSignature": encoded})
                        else:
                            source[0 if kind == "thought" else 1]["thoughtSignature"] = encoded
                        ident = f"op-{self.channel}-{kind}"
                        if with_tool:
                            source.append(assistant(ident, b"explicit function carrier"))
                        self.reply = source
                        emitted = await self.chat(prompt, with_tool=with_tool)
                        metadata = copy.deepcopy(emitted["extra_content"]["google"])
                        self.assertEqual(metadata["thought_signature"], encoded)
                        for case in ("exact", "compact", "missing", "empty"):
                            if kind in (None, "signature_only") and case not in ("exact", "compact"):
                                continue
                            with self.subTest(case=case):
                                m = copy.deepcopy(emitted)
                                google = m["extra_content"]["google"]
                                if case != "exact":
                                    google.pop("ordinary_parts")
                                    google.pop("part_order", None)
                                if kind is None:
                                    google.pop("thought_signature_part", None)  # Old untyped metadata.
                                if case in ("missing", "empty"):
                                    if case == "empty" or kind == "thought":
                                        m.pop("reasoning_content", None)
                                    if case == "empty" or kind == "text":
                                        m["content"] = None
                                history = [*prompt, m]
                                if with_tool:
                                    history.append({"role": "tool", "tool_call_id": ident, "content": "output"})
                                history.append({"role": "user", "content": "Continue this fixture."})
                                self.reply = {"text": "done"}
                                self.sent.clear()
                                await self.chat(history, with_tool=with_tool)
                                self.assertEqual(len(self.sent), 1)
                                expectation = {
                                    "text": (text, False), None: (text, False),
                                    "thought": (reasoning, True), "signature_only": ("", False),
                                }[kind]
                                observed = [(p.get("text"), bool(p.get("thought"))) for p in self.model_parts()
                                            if signature(p) == badge]
                                self.assertEqual(observed, [] if case in ("missing", "empty") else [expectation])
                                if with_tool:
                                    self.assert_tool(ident, b"explicit function carrier")

    async def test_unsigned_cookie_no_tool_history_retains_legacy_visibility(self):
        self.channel = "web_proxy"
        self.reply = [{"text": "provider's plain thought", "thought": True}, {"text": "visible text"}]
        prompt = [{"role": "user", "content": "Local question."}]
        emitted = await self.chat(prompt)
        emitted.pop("extra_content", None)
        self.reply = {"text": "done"}
        await self.chat([*prompt, emitted, {"role": "user", "content": "Next question."}])
        self.assertEqual(self.model_parts(), [{"text": "visible text"}])

    async def test_compacted_responses_text_signature_does_not_move_to_reasoning_or_blank(self):
        for self.channel in ("api_key", "web_proxy"):
            for keep_reasoning in (False, True):
                with self.subTest(channel=self.channel, keep_reasoning=keep_reasoning):
                    prompt = f"offline Responses {self.channel}/{keep_reasoning}"
                    ident = "response-call-" + self.channel
                    raw = b"actual signed ordinary text"
                    self.reply = [{"text": "distinct thought", "thought": True},
                                  {"text": "the removed original", "thoughtSignature": base64.b64encode(raw).decode()},
                                  assistant(ident, b"actual function signature")]
                    resp = await self.client.post("/v1/responses", json=request(input=prompt))
                    self.assertEqual(resp.status_code, 200, resp.text[:240])
                    reasoning, text, call = resp.json()["output"]
                    text["content"][0]["text"] = ""
                    google = text["extra_content"]["google"]
                    self.assertEqual(google["thought_signature_part"], "text")
                    google.pop("ordinary_parts")
                    google.pop("part_order")
                    items = [reasoning, text, call] if keep_reasoning else [text, call]
                    self.reply = {"text": "done"}
                    self.sent.clear()
                    resp = await self.client.post("/v1/responses", json=request(input=continuation(prompt, items)))
                    self.assertEqual(resp.status_code, 200, resp.text[:240])
                    self.assertEqual(len(self.sent), 1)
                    self.assertFalse(any(signature(p) == raw for p in self.model_parts()))
                    self.assert_tool(ident, b"actual function signature")

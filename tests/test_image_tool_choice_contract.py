"""A mandatory client's tool choice cannot turn into ordinary image generation."""
from unittest.mock import patch

from routes import chat_api, claude_api
from test_schema_contract_fidelity import MODEL as TEXT, MESSAGES, SchemaRouteHarness


IMAGE = "gemini-3.1-flash-image"


class ModelToolChoiceRoutes(SchemaRouteHarness):
    async def send(self, protocol, model, choice=None, *, stream=False, name="weather", declare=True,
                   snake=False):
        self.sent.clear()
        body = {"model": model, "messages": MESSAGES, "stream": stream}
        if protocol == "openai":
            path = "/v1/chat/completions"
            if declare:
                body["tools"] = [{"type": "function", "function": {"name": name}}]
            if choice is not None:
                body["tool_choice"] = {"type": "function", "function": {"name": name}} if choice == "named" else choice
        elif protocol == "claude":
            path = "/v1/messages"
            body["max_tokens"] = 48
            if declare:
                body["tools"] = [{"name": name, "input_schema": {"type": "object"}}]
            if choice is not None:
                body["tool_choice"] = ({"type": "tool", "name": name} if choice == "named"
                                       else {"type": "any" if choice == "required" else choice})
        else:
            action = "streamGenerateContent?alt=sse" if stream else "generateContent"
            path = f"/v1beta/models/{model}:{action}"
            body = {"contents": [{"role": "user", "parts": [{"text": "hello"}]}]}
            if declare:
                decls = "function_declarations" if snake else "functionDeclarations"
                body["tools"] = [{decls: [{"name": name}]}]
            if choice is not None:
                config = {"mode": "ANY" if choice in ("required", "named") else choice.upper()}
                if choice == "named":
                    config["allowed_function_names" if snake else "allowedFunctionNames"] = [name]
                body["tool_config" if snake else "toolConfig"] = {
                    "function_calling_config" if snake else "functionCallingConfig": config}
        return await self.client.post(path, json=body)

    def paths(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for protocol in ("openai", "claude", "native") if channel == "web_proxy" else ("openai", "claude"):
                with self.subTest(channel=channel, protocol=protocol):
                    yield protocol

    async def test_mandatory_selection_is_a_real_400_before_dispatch(self):
        for protocol in self.paths():
            for choice in ("required", "named"):
                for stream in (False, True):
                    with self.subTest(choice=choice, stream=stream):
                        # 'search' as a client function does not select the optional built-in.
                        reply = await self.send(protocol, IMAGE + "-2k", choice, stream=stream, name="search", snake=stream)
                        self.assertEqual(reply.status_code, 400, reply.text[:256])
                        self.assertTrue(reply.headers["content-type"].startswith("application/json"))
                        error = reply.json()["error"]
                        if protocol == "native":
                            self.assertEqual(error["status"], "INVALID_ARGUMENT")
                            self.assertIn("functionCallingConfig", error["message"])
                        else:
                            self.assertEqual(error["type"], "invalid_request_error")
                            self.assertIn("tool_choice", error["message"])
                        self.assertEqual(self.sent, [])

    async def test_mixed_and_resumable_do_not_start_or_mask_invalid_requests(self):
        self.channel = "mixed"
        self.settings["resumable_enabled"] = True
        self.client.headers["X-Resumable"] = "true"
        with patch.object(chat_api.credential_manager, "get_mixed_record") as select, \
             patch.object(chat_api, "_resumable_stream") as admit, patch.object(claude_api, "call_upstream") as dispatch:
            for protocol in ("openai", "claude"):
                result = await self.send(protocol, IMAGE, "required", stream=True)
                self.assertEqual(result.status_code, 400)
            select.assert_not_called()
            admit.assert_not_called()
            dispatch.assert_not_called()
            self.assertEqual(self.sent, [])

    async def test_soft_image_choices_and_text_function_selection_reach_real_wire(self):
        for protocol in self.paths():
            for model, choice, declare in ((IMAGE, None, False), (IMAGE, "auto", True), (IMAGE, "none", True),
                                           (TEXT, "required", True), (TEXT, "named", True)):
                with self.subTest(model=model, choice=choice):
                    result = await self.send(protocol, model, choice, declare=declare)
                    self.assertEqual(result.status_code, 200, result.text[:256])
                    self.assertEqual(len(self.sent), 1)
                    wire = self.sent[0].get("variables", self.sent[0])
                    if model == IMAGE:
                        self.assertNotIn("tools", wire)
                        self.assertNotIn("toolConfig", wire)
                    else:
                        self.assertEqual(wire["tools"][0]["functionDeclarations"][0]["name"], "weather")
                        config = wire["toolConfig"]["functionCallingConfig"]
                        self.assertEqual(config["mode"], "ANY")
                        if choice == "named":
                            self.assertEqual(config.get("allowedFunctionNames", config.get("allowed_function_names")), ["weather"], config)

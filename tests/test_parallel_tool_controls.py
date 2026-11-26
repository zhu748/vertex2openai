"""The gateway must not turn an explicit serial-only request into a multi-call batch."""
import json
from unittest.mock import patch

from routes import chat_api, claude_api
from test_schema_contract_fidelity import MESSAGES, MODEL, SchemaRouteHarness


MODES = ((False, False), (True, False), (True, True))
SCHEMA = {"type": "object", "properties": {"city": {"type": "string"}}}
CITIES = ("Paris", "Tokyo")
CALLS = [{"functionCall": {"id": f"tool_{city}", "name": "weather", "args": {"city": city}}} for city in CITIES]


def _request(protocol, choice, *, allow_parallel=None, tools=True, stream=False):
    body = {"model": MODEL, "messages": MESSAGES, "stream": stream}
    if protocol == "openai":
        if tools:
            body["tools"] = [{"type": "function", "function": {"name": "weather", "parameters": SCHEMA}}]
        if choice:
            body["tool_choice"] = {"type": "function", "function": {"name": "weather"}} if choice == "named" else choice
        if allow_parallel is not None:
            body["parallel_tool_calls"] = allow_parallel
        return "/v1/chat/completions", body
    body["max_tokens"] = 48
    if tools:
        body["tools"] = [{"name": "weather", "input_schema": SCHEMA}]
    if choice or allow_parallel is not None:
        body["tool_choice"] = {"type": {"required": "any", "named": "tool"}.get(choice, choice or "auto")}
        if choice == "named":
            body["tool_choice"]["name"] = "weather"
        if allow_parallel is not None:
            body["tool_choice"]["disable_parallel_tool_use"] = not allow_parallel
    return "/v1/messages", body


def _arguments(response, protocol, stream):
    if not stream:
        message = response.json()
        if protocol == "openai":
            return [json.loads(c["function"]["arguments"]) for c in message["choices"][0]["message"]["tool_calls"]]
        return [b["input"] for b in message["content"] if b["type"] == "tool_use"]
    args = {}
    for line in response.text.splitlines():
        if not line.startswith("data: {"):
            continue
        frame = json.loads(line[6:])
        if protocol == "openai":
            for choice in frame.get("choices", []):
                for call in choice["delta"].get("tool_calls", []):
                    i = call["index"]
                    args[i] = args.get(i, "") + call.get("function", {}).get("arguments", "")
        elif frame.get("delta", {}).get("type") == "input_json_delta":
            i = frame["index"]
            args[i] = args.get(i, "") + frame["delta"]["partial_json"]
    return [json.loads(args[i]) for i in sorted(args)]


class ParallelToolRoutes(SchemaRouteHarness):
    async def test_explicit_serial_is_rejected_before_emitting_even_one_call(self):
        self.reply = CALLS  # Selecting a single *name* can still produce two invocations.
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for protocol in ("openai", "claude"):
                for choice in (None, "auto", "required", "named"):
                    for stream, fake in MODES:
                        with self.subTest(channel=channel, protocol=protocol, choice=choice, mode=(stream, fake)):
                            self.settings["fake_streaming"] = fake
                            path, body = _request(protocol, choice, stream=stream, allow_parallel=False)
                            self.sent.clear()
                            reply = await self.client.post(path, json=body)
                            self.assertEqual(reply.status_code, 400, reply.text[:256])
                            self.assertTrue(reply.headers["content-type"].startswith("application/json"))
                            error = reply.json()["error"]
                            self.assertEqual(error["type"], "invalid_request_error")
                            field = "parallel_tool_calls" if protocol == "openai" else "disable_parallel_tool_use"
                            self.assertIn(field, error["message"])
                            self.assertEqual(self.sent, [])

    async def test_supported_parallel_requests_keep_both_calls_and_their_arguments(self):
        self.reply = CALLS
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for protocol in ("openai", "claude"):
                for allow in (None, True):
                    for stream, fake in MODES:
                        with self.subTest(channel=channel, protocol=protocol, allow=allow, mode=(stream, fake)):
                            self.settings["fake_streaming"] = fake
                            path, body = _request(protocol, "named", stream=stream, allow_parallel=allow)
                            self.sent.clear()
                            reply = await self.client.post(path, json=body)
                            self.assertEqual(reply.status_code, 200, reply.text[:256])
                            self.assertEqual(len(self.sent), 1)
                            self.assertEqual(_arguments(reply, protocol, stream), [{"city": city} for city in CITIES])

    async def test_no_tools_or_disabled_tools_do_not_require_a_cardinality_feature(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for protocol in ("openai", "claude"):
                for choice, declared in (("none", True), ("auto", False)):
                    with self.subTest(channel=channel, protocol=protocol, choice=choice):
                        path, body = _request(protocol, choice, tools=declared, allow_parallel=False)
                        self.sent.clear()
                        reply = await self.client.post(path, json=body)
                        self.assertEqual(reply.status_code, 200, reply.text[:256])
                        self.assertEqual(len(self.sent), 1)
                        self.assertNotIn("tools", self.sent[0].get("variables", self.sent[0]))

    async def test_builtin_only_and_image_paths_still_have_no_callable_client_functions(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for kind in ("builtin_only", "forced_builtin", "custom_named_search", "image"):
                with self.subTest(channel=channel, kind=kind):
                    path, body = _request("openai", "auto", allow_parallel=False)
                    if kind == "builtin_only":
                        body["tools"] = [{"type": "web_search"}]
                    elif kind in ("forced_builtin", "custom_named_search"):
                        body["tool_choice"] = {"type": "function", "function": {"name": "search"}}
                        if kind == "custom_named_search":
                            body["tools"][0]["function"]["name"] = "search"
                    else:
                        body["model"] = "gemini-3.1-flash-image"
                    self.sent.clear()
                    reply = await self.client.post(path, json=body)
                    if kind == "custom_named_search":
                        self.assertEqual(reply.status_code, 400)
                        self.assertEqual(self.sent, [])
                    else:
                        self.assertEqual(reply.status_code, 200, reply.text[:256])
                        self.assertEqual(len(self.sent), 1)
                        wire = self.sent[0].get("variables", self.sent[0])
                        self.assertFalse(any("functionDeclarations" in t for t in wire.get("tools", [])))
                        if kind != "image":
                            self.assertIn({"googleSearch": {}}, wire["tools"])

    async def test_control_flags_are_booleans_not_truthy_strings(self):
        for protocol, field, status in (("openai", "parallel_tool_calls", 422),
                                        ("claude", "disable_parallel_tool_use", 400)):
            with self.subTest(protocol=protocol):
                path, body = _request(protocol, "auto")
                (body if protocol == "openai" else body["tool_choice"])[field] = "false"
                reply = await self.client.post(path, json=body)
                self.assertEqual(reply.status_code, status)
                self.assertIn(field, reply.json()["error"]["message"])
                self.assertEqual(self.sent, [])

    async def test_counting_input_tokens_does_not_generate_a_tool_call(self):
        _, body = _request("claude", "named", allow_parallel=False)
        with patch.object(claude_api, "_express_exact_input_tokens", return_value=37) as count:
            result = await self.client.post("/v1/messages/count_tokens", json=body)
        self.assertEqual(result.status_code, 200, result.text[:256])
        self.assertEqual(result.json()["input_tokens"], 37)
        count.assert_awaited_once()
        self.assertEqual(self.sent, [])

    async def test_invalid_control_precedes_key_selection_and_resume_lookup(self):
        self.channel = "mixed"
        self.settings["resumable_enabled"] = True
        with patch.object(chat_api.credential_manager, "get_mixed_record", return_value=None) as select, \
             patch.object(chat_api, "resolve_buffer", wraps=chat_api.resolve_buffer) as lookup, \
             patch.object(claude_api, "call_upstream", wraps=claude_api.call_upstream) as dispatch:
            for protocol in ("openai", "claude"):
                for stream in (False, True):
                    path, body = _request(protocol, "named", allow_parallel=False, stream=stream)
                    reply = await self.client.post(path, json=body, headers={"X-Resumable": "true", "X-Resume-Id": "r5-existing"})
                    self.assertEqual(reply.status_code, 400, reply.text[:256])
            select.assert_not_called()
            lookup.assert_not_called()
            dispatch.assert_not_called()
        self.assertEqual(self.sent, [])

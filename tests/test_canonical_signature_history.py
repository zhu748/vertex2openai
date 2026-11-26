"""Wire-equivalent harness representations must preserve authentic tool signatures."""

from copy import deepcopy
import json
from unittest.mock import patch

import pytest

from models import OpenAIRequest
import signature_origin as origin
import test_bounded_signature_origins as fixtures
from test_parallel_tool_controls import MODES, _request
from test_schema_contract_fidelity import SchemaRouteHarness


def wire_parts(seen, kind):
    body = seen.get("variables", seen)
    return [part for contents in body["contents"] for part in contents["parts"] if kind in part]


class CanonicalOriginRoutes(SchemaRouteHarness):
    async def post(self, path, body, parts, *, headers=None):
        self.reply = parts
        self.sent.clear()
        response = await self.client.post(path, json=body, headers=headers)
        self.assertEqual(response.status_code, 200, response.text[:300])
        self.assertEqual(len(self.sent), 1)
        return response

    def signatures(self):
        return [(part["functionCall"]["id"], fixtures.original(part))
                for part in wire_parts(self.sent[0], "functionCall")]

    async def test_chat_completion_thin_echo_accepts_equivalent_standard_shapes(self):
        for self.channel in ("api_key", "web_proxy"):
            for stream, fake in MODES:
                with self.subTest(channel=self.channel, stream=stream, fake=fake):
                    self.settings["fake_streaming"] = fake
                    path, body = _request("openai", "auto", stream=stream)
                    prefix = "Price in 東京"
                    body.update(model=fixtures.MODEL, messages=[{"role": "user", "content": prefix}])
                    ident = f"r11-shape-{self.channel}-{stream}-{fake}"
                    sig = b"\xff\x00genuine"
                    response = await self.post(path, body, [fixtures.provider_call(ident, sig, city="東京")])
                    assistant = fixtures.BoundedOriginRoutes.assistant_echo(
                        self, response, "openai", "stream" if stream else "json")
                    tool = fixtures.results("openai", (ident,), "ok")
                    original = {**body, "stream": False, "messages": [*body["messages"], assistant, *tool]}
                    transformed = deepcopy(original)
                    user, msg, _ = transformed["messages"]
                    user["content"] = [{"type": "text", "text": prefix}]
                    msg.update(content=[], annotations=[])
                    fn = msg["tool_calls"][0]["function"]
                    fn["arguments"] = json.loads(fn["arguments"])
                    await self.post(path, original, [{"text": "done"}])
                    baseline_wire = self.sent[0].get("variables", self.sent[0])["contents"]
                    await self.post(path, transformed, [{"text": "done"}])
                    self.assertEqual(self.signatures(), [(ident, sig)])
                    self.assertEqual(self.sent[0].get("variables", self.sent[0])["contents"], baseline_wire)

                    for change in ("part-boundaries", "args", "model", "auth", "extra"):
                        with self.subTest(change=change):
                            changed = deepcopy(transformed)
                            changed_user, changed_assistant, _ = changed["messages"]
                            headers = None
                            if change == "part-boundaries":
                                changed_user["content"] = [{"type": "text", "text": p} for p in (prefix[:5], prefix[5:])]
                            elif change == "args":
                                changed_assistant["tool_calls"][0]["function"]["arguments"]["city"] = "京都"
                            elif change == "model":
                                changed["model"] = "gemini-3-pro-preview"
                            elif change == "auth":
                                headers = {"Authorization": "Bearer alternate-test-key"}
                            else:
                                changed_assistant["annotations"] = [{"type": "new_metadata"}]
                            # Change the active fixture tenant while continuing
                            # to exercise the real route authentication layer.
                            with patch("auth.API_KEY", "alternate-test-key" if headers else "test-key"):
                                await self.post(path, changed, [{"text": "done"}], headers=headers)
                            self.assertEqual(self.signatures(), [(ident, None)])

    async def test_complete_parallel_reordering_preserves_next_call_on_both_public_routes(self):
        for self.channel in ("api_key", "web_proxy"):
            for protocol in ("openai", "claude"):
                for stream, fake in MODES:
                    with self.subTest(channel=self.channel, protocol=protocol, stream=stream, fake=fake):
                        self.settings["fake_streaming"] = fake
                        path, body = _request(protocol, "auto", stream=stream)
                        prefix = "Return three fixture prices"
                        body.update(model=fixtures.MODEL, messages=[{"role": "user", "content": prefix}])
                        suffix = f"{self.channel}-{protocol}-{stream}-{fake}"
                        a, b, c = (f"r11-order-{i}-{suffix}" for i in "abc")
                        first, second = b"\xfeone", b"\xffnext"
                        mode = "stream" if stream else "json"
                        response = await self.post(path, body, [
                            fixtures.provider_call(a, first), fixtures.provider_call(b, city="Lima")])
                        prior = fixtures.BoundedOriginRoutes.assistant_echo(self, response, protocol, mode)
                        results = fixtures.results(protocol, (a, b), "ok")
                        body["messages"].extend([prior, *results])
                        response = await self.post(path, body, [fixtures.provider_call(c, second)])
                        later = fixtures.BoundedOriginRoutes.assistant_echo(self, response, protocol, mode)
                        body["messages"].extend([later, *fixtures.results(protocol, (c,), "latest")])
                        body["stream"] = False
                        await self.post(path, body, [{"text": "done"}])
                        self.assertEqual(self.signatures(), [(a, first), (b, None), (c, second)])
                        baseline_wire = self.sent[0].get("variables", self.sent[0])["contents"]

                        reversed_ = deepcopy(body)
                        reversed_["messages"][0]["content"] = [{"type": "text", "text": prefix}]
                        if protocol == "openai":
                            run = reversed_["messages"][2:4]
                            reversed_["messages"][2:4] = run[::-1]
                        else:
                            reversed_["messages"][2]["content"].reverse()
                        await self.post(path, reversed_, [{"text": "done"}])
                        self.assertEqual(self.signatures(), [(a, first), (b, None), (c, second)])
                        self.assertEqual(self.sent[0].get("variables", self.sent[0])["contents"], baseline_wire)

                        for mismatch in ("edited", "partial", "duplicate", "orphan"):
                            with self.subTest(mismatch=mismatch):
                                changed = deepcopy(reversed_)
                                messages = changed["messages"]
                                run = messages[2:4] if protocol == "openai" else messages[2]["content"]
                                if mismatch == "edited":
                                    run[0]["content"] = "changed result"
                                elif mismatch == "partial":
                                    run.pop()
                                elif mismatch == "duplicate":
                                    run[1] = deepcopy(run[0])
                                else:
                                    id_field = "tool_call_id" if protocol == "openai" else "tool_use_id"
                                    run[1][id_field] = "unmatched-result"
                                if protocol == "openai":
                                    messages[2:4] = run
                                await self.post(path, changed, [{"text": "done"}])
                                self.assertEqual(self.signatures(), [(a, first), (b, None), (c, None)])


def test_object_argument_proof_limits_before_serialization():
    class ArgumentTooLarge(dict):
        def items(self):
            pytest.fail("oversized object should be rejected before traversal")

    wide = ArgumentTooLarge({str(i): 0 for i in range(origin._MAX_ARGS_BYTES // 2 + 2)})
    deep = []
    top = deep
    for _ in range(origin._MAX_OBJECT_DEPTH + 1):
        new = []
        deep.append(new)
        deep = new
    for unsupported in (wide, {"deep": top}, {"wide-utf8": "🡑" * (origin._MAX_ARGS_BYTES // 4 + 1)}):
        message = {"role": "assistant", "tool_calls": [
            {"id": "a", "function": {"name": "one", "arguments": unsupported}}]}
        request = OpenAIRequest(model=fixtures.MODEL, messages=[message])
        origin.bind_signature_origins(request, "test-key")
        assert request._signature_output_prefix is None
        assert not origin.origin_slot(request.messages[0], 0)


def test_tool_output_list_does_not_share_plain_text_prefix():
    def prefix(result):
        request = OpenAIRequest(model=fixtures.MODEL, tools=[{"type": "function"}], messages=[
            {"role": "user", "content": "Fixture request"},
            {"role": "tool", "tool_call_id": "id", "content": result}])
        origin.bind_signature_origins(request, "test-key")
        return request._signature_output_prefix

    list_prefix = prefix([{"type": "text", "text": "ok"}])
    string_prefix = prefix("ok")
    assert list_prefix and string_prefix and list_prefix != string_prefix

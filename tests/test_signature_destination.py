"""The actual winning provider resource, not the client alias, owns a cached signature."""
import base64
import json
from contextlib import asynccontextmanager
from copy import deepcopy
from unittest.mock import patch

import httpx
from google import genai
from google.genai import types

import config
from runtime_state import app_state
from signature_destination import UNQUALIFIED, publisher_destination
from test_claude_buffered_tool_order import read_events, reconstruct
from test_parallel_tool_controls import _request
from test_schema_contract_fidelity import SchemaRouteHarness
from upstreams import cookie_delivery, express_sdk


MODEL = "gemini-3-flash-preview"
SIGNATURE = b"\x00original-fixture\xff"
MODES = ("json", "stream", "fake")


def sdk_parts(calls):
    return [{"functionCall": {"id": ident, "name": "weather", "args": {"city": city}},
             "thoughtSignature": base64.b64encode(sig).decode()}
            for ident, city, sig in calls]


def pairs(calls):
    return [(ident, sig) for ident, _, sig in calls]


def test_qualified_resource_names_are_bounded_and_not_inferred_from_aliases():
    target = "projects/p-a/locations/global/publishers/google/models/gemini-3-flash-preview"
    assert len(publisher_destination(target)) == 32
    for other in (target.replace("p-a", "p-b"), target.replace("global", "us-central1"), target+"-lite"):
        assert publisher_destination(other) != publisher_destination(target)
    for invalid in (MODEL, "publishers/google/models/" + MODEL, target + "/", target + "?id=1", target + "a"*512):
        assert publisher_destination(invalid) == UNQUALIFIED


class ProviderDestinationRoutes(SchemaRouteHarness):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.settings.update(express_location="global", retry_max=1, retry_backoff_seconds=0, retry_jitter_seconds=0)
        self.project = "p-a"
        self.fail_on = None
        self.rotate_to = None
        self.pick_unknown_endpoint = False
        self.sdk_selections = 0
        self.wire = []

        def respond(request):
            body = json.loads(request.content)
            studio = "variables" in body
            wire = body.get("variables", body)
            path = wire["model"] if studio else request.url.path
            project = next((p for p in ("p-a", "p-b") if f"/{p}/" in path), None)
            parts = [part for turn in wire["contents"] for part in turn["parts"]]
            calls = []
            for p in parts:
                if "functionCall" in p:
                    sig = p.get("thoughtSignature")
                    if sig and sig != "skip_thought_signature_validator":
                        sig = base64.urlsafe_b64decode(sig)
                    calls.append((p["functionCall"]["id"], sig))
            results = [p["functionResponse"]["id"] for p in parts if "functionResponse" in p]
            self.wire.append((project, request.url.host, calls, results))
            if self.fail_on is not None and project == "p-a":
                code, status, message = self.fail_on
                self.fail_on = None
                return httpx.Response(code, json={"error": {"code": code, "message": message, "status": status}})
            result = {"candidates": [{"content": {"role": "model", "parts": self.reply}, "finishReason": "STOP"}]}
            if studio:
                return httpx.Response(200, json={"results": [{"data": result}]})
            if path.endswith(":streamGenerateContent"):
                return httpx.Response(200, text=f"data: {json.dumps(result)}\n\n", headers={"content-type": "text/event-stream"})
            return httpx.Response(200, json=result)

        transport = httpx.MockTransport(respond)

        def make_sdk(**options):
            sdk = genai.Client(vertexai=True, api_key="fixture", http_options=types.HttpOptions(
                async_client_args={"transport": transport}, **options))
            self.addAsyncCleanup(sdk.aio.aclose)
            self.addCleanup(sdk.close)
            return sdk

        official, unverified = make_sdk(), make_sdk(base_url="https://unverified.fixture/")
        studio = httpx.AsyncClient(transport=transport)
        self.addAsyncCleanup(studio.aclose)

        @asynccontextmanager
        async def open_studio(*args, **kwargs):
            yield studio

        def select(exclude_key=None, exclude_keys=None):
            project = self.rotate_to if exclude_key and self.rotate_to else self.project
            return {"key": f"fixture-{project}", "project_id": project}

        def raw_client(*args):
            self.sdk_selections += 1
            # Prepare() sees the official client; actual invocation resolves
            # again and sees the custom endpoint. Both are mock transports.
            return unverified if self.pick_unknown_endpoint and self.sdk_selections > 1 else official

        for manager in (
            patch.object(self.key_manager, "get_express_record", side_effect=select),
            patch.object(express_sdk, "get_pooled_express_client", side_effect=raw_client),
            patch.object(app_state, "get_cookie_accounts", side_effect=lambda **_: [{
                "id": "fixture", "cookie": "SAPISID=offline", "project_id": self.project, "enabled": True}]),
            patch.object(cookie_delivery, "open_upstream_client", open_studio),
        ):
            manager.start()
            self.addCleanup(manager.stop)

    async def offer(self, label, mode, dialect, calls, *, carrier=False):
        self.wire.clear()
        self.settings["fake_streaming"] = mode == "fake"
        prompt = [{"role": "user", "content": label}]
        self.reply = sdk_parts(calls)
        path, body = _request("openai" if dialect == "openai" else "claude", "auto", stream=mode != "json")
        body.update(model=MODEL, messages=prompt)
        if dialect == "omitted":
            body.update(max_tokens=2048, thinking={"type": "enabled", "budget_tokens": 1024, "display": "omitted"})
            self.reply[:0] = [{"text": "private fixture", "thought": True}, {"text": "Checking."}]
        response = await self.client.post(path, json=body)
        self.assertEqual(response.status_code, 200, response.text[:200])
        self.assertNotIn("private fixture", response.text)
        if dialect == "openai":
            if mode == "json":
                message = response.json()["choices"][0]["message"]
            else:
                self.assertTrue(response.text.rstrip().endswith("data: [DONE]"))
                chunks = [json.loads(line[6:]) for line in response.text.splitlines() if line.startswith("data: {")]
                defined = {}
                for chunk in chunks:
                    for choice in chunk["choices"]:
                        for delta in choice["delta"].get("tool_calls", []):
                            row = defined.setdefault(delta["index"], {"type": "function", "function": {"arguments": ""}})
                            if "id" in delta:
                                row["id"] = delta["id"]
                            if "name" in delta["function"]:
                                row["function"]["name"] = delta["function"]["name"]
                            row["function"]["arguments"] += delta["function"].get("arguments", "")
                            if "extra_content" in delta:
                                row["extra_content"] = delta["extra_content"]
                message = {"role": "assistant", "content": None, "tool_calls": list(defined.values())}
            if not carrier:
                message.pop("extra_content", None)
                for call in message["tool_calls"]:
                    call.pop("extra_content", None)
            public_ids = [call["id"] for call in message["tool_calls"]]
            result = [{"role": "tool", "tool_call_id": ident, "content": "done"} for ident in public_ids]
        else:
            blocks = (response.json()["content"] if mode == "json"
                      else reconstruct(self, read_events(response.text)))
            message = {"role": "assistant", "content": blocks}
            public_ids = [block["id"] for block in blocks if block["type"] == "tool_use"]
            result = [{"role": "user", "content": [{"type": "tool_result", "tool_use_id": ident, "content": "done"}
                                                    for ident in public_ids]}]
        self.assertEqual(public_ids, [ident for ident, _, _ in calls])
        return path, {**body, "messages": [*prompt, message, *result]}

    async def replay(self, request, mode, *, code=200):
        self.wire.clear()
        self.settings["fake_streaming"] = mode == "fake"
        self.reply = [{"text": "The result was read."}]
        path, body = deepcopy(request)
        response = await self.client.post(path, json={**body, "stream": mode != "json"})
        self.assertEqual(response.status_code, code, response.text[:200])
        if code == 200:
            if mode == "json":
                self.assertIn("The result was read.", response.text)
            elif path == "/v1/messages":
                blocks = reconstruct(self, read_events(response.text))
                self.assertEqual("".join(b["text"] for b in blocks), "The result was read.")
            else:
                frames = [json.loads(line[6:]) for line in response.text.splitlines() if line.startswith("data: {")]
                text = "".join(choice["delta"].get("content") or "" for f in frames for choice in f["choices"])
                self.assertEqual(text, "The result was read.")
        return self.wire[:]

    def assert_trace(self, trace, projects, expected_calls):
        self.assertEqual([project for project, _, _, _ in trace], projects)
        self.assertEqual([calls for _, _, calls, _ in trace], expected_calls)
        for _, _, calls, ids in trace:
            self.assertEqual([ident for ident, _ in calls], ids)

    async def test_same_qualified_project_keeps_original_crossing_adapters_but_other_project_does_not(self):
        for carrier in ("openai", "claude", "omitted"):
            for mode in MODES:
                for producer in ("api_key", "web_proxy"):
                    with self.subTest(carrier=carrier, mode=mode, producer=producer):
                        ident = f"{carrier}-{producer}-{mode}"
                        calls = [(ident, "Paris", SIGNATURE), (ident+"-second", "Berlin", b"second-signature")]
                        self.channel, self.project = producer, "p-a"
                        request = await self.offer(ident, mode, carrier, calls)
                        self.assertEqual([row[0] for row in self.wire], ["p-a"])
                        self.channel = "web_proxy" if producer == "api_key" else "api_key"
                        self.assert_trace(await self.replay(request, mode), ["p-a"], [pairs(calls)])
                        self.project = "p-b"
                        self.assert_trace(await self.replay(request, mode), ["p-b"], [[(i, None) for i, _, _ in calls]])

    async def test_rotation_unpin_and_actual_winner_are_checked_each_attempt(self):
        for carrier in ("openai", "omitted"):
            for mode in MODES:
                with self.subTest(carrier=carrier, mode=mode):
                    self.project, self.rotate_to = "p-a", "p-b"
                    ident = f"{carrier}-rotated-{mode}"
                    calls = [(ident, "Paris", SIGNATURE)]
                    request = await self.offer(ident, mode, carrier, calls)
                    self.fail_on = 503, "UNAVAILABLE", "fixture temporarily unavailable"
                    self.assert_trace(await self.replay(request, mode), ["p-a", "p-b"], [pairs(calls), [(ident, None)]])
                    self.fail_on = 404, "NOT_FOUND", "Publisher model not found"
                    self.assert_trace(await self.replay(request, mode), ["p-a", None], [pairs(calls), [(ident, None)]])
                    self.fail_on = 503, "UNAVAILABLE", "fixture primary failure"
                    winner = await self.offer(ident+" winner", mode, carrier, calls)
                    self.assertEqual([row[0] for row in self.wire], ["p-a", "p-b"])
                    self.assert_trace(await self.replay(winner, mode), ["p-a"], [[(ident, None)]])
                    self.project = "p-b"
                    self.assert_trace(await self.replay(winner, mode), ["p-b"], [pairs(calls)])

    async def test_same_public_batch_from_two_projects_never_borrows_either_emission(self):
        for dialect in ("openai", "omitted"):
            calls = [(f"{dialect}-1", "Paris", SIGNATURE), (f"{dialect}-2", "Berlin", b"unchanged sibling")]
            self.project = "p-a"
            one = await self.offer(dialect, "json", dialect, calls)
            self.assert_trace(await self.replay(one, "json"), ["p-a"], [pairs(calls)])
            self.project = "p-b"
            two = await self.offer(dialect, "json", dialect, [(calls[0][0], "Paris", b"different"), calls[1]])
            self.assertEqual(one, two)
            absent = [(ident, None) for ident, _, _ in calls]
            self.assert_trace(await self.replay(one, "json"), ["p-b"], [absent])
            self.project = "p-a"
            self.assert_trace(await self.replay(one, "json"), ["p-a"], [absent])

    async def test_custom_actual_sdk_endpoint_is_not_the_prepared_official_client(self):
        calls = [("custom-route", "Paris", SIGNATURE)]
        request = await self.offer("custom", "json", "openai", calls)
        self.pick_unknown_endpoint = True
        self.sdk_selections = 0
        trace = await self.replay(request, "json")
        self.assert_trace(trace, ["p-a"], [[("custom-route", None)]])
        self.assertEqual(trace[0][1], "unverified.fixture")

    async def test_explicit_carrier_is_never_rewritten_but_unknown_producer_is_not_proof(self):
        calls = [("genuine-carrier__thought__cmF3", "Paris", SIGNATURE)]
        request = await self.offer("carried", "json", "openai", calls, carrier=True)
        self.project = "p-b"
        self.assert_trace(await self.replay(request, "json"), ["p-b"], [pairs(calls)])
        self.project = "p-a"
        calls = [("bare-fixture", "Paris", SIGNATURE)]
        self.fail_on = 404, "NOT_FOUND", "Publisher model not found"
        request = await self.offer("bare", "json", "openai", calls)
        self.assertEqual([row[0] for row in self.wire], ["p-a", None])
        self.assert_trace(await self.replay(request, "json"), ["p-a"], [[("bare-fixture", None)]])
        self.settings["express_location"] = ""
        self.assert_trace(await self.replay(request, "json"), [None], [pairs(calls)])

    async def test_strict_mode_rejects_only_the_unproven_attempt(self):
        calls = [("strict-route", "Paris", SIGNATURE)]
        request = await self.offer("strict", "json", "openai", calls)
        with patch.object(config, "GEMINI_STRICT_THOUGHT_SIGNATURES", True):
            self.assert_trace(await self.replay(request, "json"), ["p-a"], [pairs(calls)])
            self.project = "p-b"
            self.assertEqual(await self.replay(request, "json", code=400), [])

    async def test_responses_public_projection_binds_the_accepted_internal_json_attempt(self):
        for stream in (False, True):
            with self.subTest(stream=stream):
                ident = f"responses-winner-{stream}"
                calls = [(ident, "Paris", SIGNATURE)]
                body = {"model": MODEL, "input": ident, "store": False, "stream": stream,
                        "tools": [{"type": "function", "name": "weather", "parameters": {"type": "object"}}]}
                self.project, self.rotate_to = "p-a", "p-b"
                self.fail_on = 503, "UNAVAILABLE", "fixture primary failure"
                self.wire.clear()
                self.reply = sdk_parts(calls)
                response = await self.client.post("/v1/responses", json=body)
                self.assertEqual(response.status_code, 200, response.text[:200])
                self.assertEqual([row[0] for row in self.wire], ["p-a", "p-b"])
                if stream:
                    terminal = json.loads([line[6:] for line in response.text.splitlines() if line.startswith("data: {")][-1])
                    self.assertEqual(terminal["type"], "response.completed")
                    public = terminal["response"]
                else:
                    public = response.json()
                tool, = public["output"]
                self.assertEqual(tool["call_id"], ident)
                follow = [
                    {"role": "user", "content": ident},
                    {key: value for key, value in tool.items() if key != "extra_content"},
                    {"type": "function_call_output", "call_id": ident, "output": "done"},
                ]
                for project, expected in (("p-a", None), ("p-b", SIGNATURE)):
                    self.project = project
                    self.wire.clear()
                    self.reply = [{"text": "done"}]
                    fixture = await self.client.post("/v1/responses", json={**body, "stream": False, "input": follow})
                    self.assertEqual(fixture.status_code, 200, fixture.text[:200])
                    self.assert_trace(self.wire, [project], [[(ident, expected)]])

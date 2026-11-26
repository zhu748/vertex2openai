"""Provider resource scope is an address, not a model feature or default."""

from contextlib import asynccontextmanager
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
from google import genai
from google.genai import errors, types

from channel_capabilities import cookie_model_scope
from credential_manager import COOKIE_AUTH_FAILURE_THRESHOLD, COOKIE_COOLDOWN_SECONDS, credential_manager
from first_reply import timeout_seconds
from main import app
from model_capabilities import get_profile, supports_transport
from model_resource import model_id_for_policy
from routes import claude_api, gemini_native, models_api
from runtime_state import AppState, app_state
from test_req_round15_contracts import Round15Harness
from upstreams import cookie_delivery, cookie_native, cookie_proxy, express_sdk, native_passthrough


MODEL = "gemini-3.6-flash"
PROJECT = "image-audio-studio"
PROMPT = [{"role": "user", "content": "Read the temperature."}]


def resource(project=PROJECT, location="global", model=MODEL):
    return f"projects/{project}/locations/{location}/publishers/google/models/{model}"


def test_policy_recognizes_typed_models_not_opaque_endpoints():
    for name in (resource(), "publishers/google/models/" + MODEL, "models/" + MODEL):
        assert model_id_for_policy(name) == MODEL
        assert get_profile(name) == get_profile(MODEL)
        assert supports_transport(name)
    opaque = f"projects/{PROJECT}/locations/global/endpoints/fixture"
    assert model_id_for_policy(opaque) == opaque
    assert cookie_model_scope(resource()).project_id == PROJECT
    for unscoped in (MODEL, "publishers/google/models/" + MODEL, "models/" + MODEL, opaque,
                     resource().replace("/google/", "/other/")):
        assert cookie_model_scope(unscoped) is None
    image = "gemini-3.1-flash-image"
    assert get_profile(resource(model=image)) == get_profile(image)
    assert not supports_transport(resource(model=image))


class TranslatedScopedModelRoutes(Round15Harness):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.channel = "api_key"
        self.settings.update(express_location="global", fake_streaming=False, cross_channel_fallback=False,
                             model_overrides={MODEL: {"default_max_tokens": 321, "first_reply_timeout_seconds": 18}})
        self.wire = []
        self.wire_accounts = []
        self.express_error = False
        self.reply = {"text": "A fixture reply."}

        def upstream_wire(request):
            payload = json.loads(request.content)
            query = payload.get("variables", payload)
            self.wire.append((request.url.path, query))
            self.wire_accounts.append(request.headers.get("x-fixture-cookie", "express"))
            if self.express_error and "variables" not in payload:
                return httpx.Response(503, json={"error": {
                    "code": 503, "status": "UNAVAILABLE", "message": "Fixture unavailable."}})
            if request.url.path.endswith(":countTokens"):
                return httpx.Response(200, json={"totalTokens": 12})
            value = {"candidates": [{"content": {"role": "model", "parts": [self.reply]},
                                     "finishReason": "STOP"}]}
            return httpx.Response(200, json={"results": [{"data": value}]} if "variables" in payload else value)

        transport = httpx.MockTransport(upstream_wire)
        sdk = genai.Client(vertexai=True, api_key="offline-fixture", http_options=types.HttpOptions(
            async_client_args={"transport": transport}))
        self.addCleanup(sdk.close)
        self.addAsyncCleanup(sdk.aio.aclose)
        studio = await self.enterAsyncContext(httpx.AsyncClient(transport=transport))
        self.cookie_rows = [self.cookie_record("test-account", PROJECT)]
        self.express_rows = [{"key": "offline-fixture", "project_id": PROJECT}]
        self.key_manager.get_total_keys = lambda: len(self.express_rows)
        self.key_manager.get_records = lambda: self.express_rows
        self.key_manager.get_express_record = lambda **_: next(iter(self.express_rows), None)

        @asynccontextmanager
        async def open_studio(*args, **kwargs):
            yield studio

        def headers(cookie):
            return {"authorization": "offline", "x-fixture-cookie": cookie}

        for member in (
            patch.object(app_state, "get_effective_settings", side_effect=lambda model, base=None:
                AppState.get_effective_settings(app_state, model, base=self.settings if base is None else base)),
            patch.object(app_state, "get_channel_mode", side_effect=lambda: self.channel),
            patch.object(app.state, "express_key_manager", self.key_manager),
            patch.object(app_state, "get_cookie_accounts", return_value=self.cookie_rows),
            patch.object(app_state, "get_project_id", return_value=""),
            patch.object(express_sdk, "get_pooled_express_client", return_value=sdk),
            patch.object(claude_api, "get_pooled_express_client", return_value=sdk),
            patch.object(claude_api, "_available_models", AsyncMock(return_value=[MODEL])),
            patch.object(cookie_proxy, "build_headers", side_effect=headers),
            patch.object(cookie_delivery, "build_headers", side_effect=headers),
            patch.object(cookie_native, "build_headers", side_effect=headers),
            patch.object(cookie_delivery, "open_upstream_client", open_studio),
            patch.object(cookie_native, "open_upstream_client", open_studio),
            patch.object(native_passthrough.passthrough_client_pool, "get",
                         return_value=studio),
            patch.dict(credential_manager._indices, clear=True),
            patch.dict(credential_manager._cookie_health, clear=True),
        ):
            self.enterContext(member)
        self.client = await self.enterAsyncContext(httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
            base_url="http://local", headers={"Authorization": "Bearer test-key", "X-Resumable": "false"}))

    @staticmethod
    def cookie_record(name, project, *, enabled=True):
        return {"id": name, "cookie": "SAPISID=fixture-" + name, "project_id": project, "enabled": enabled}

    async def send(self, protocol, model, *, tools=False):
        schema = {"type": "object", "properties": {"station": {"type": "string"}}}
        self.reply = ({"functionCall": {"id": "fixture-call", "name": "temperature", "args": {"station": "a"}}}
                      if tools else {"text": "A fixture reply."})
        self.wire.clear()
        self.wire_accounts.clear()
        if protocol == "chat":
            path, body = "/v1/chat/completions", {"messages": PROMPT}
            if tools:
                body.update(tools=[{"type": "function", "function": {"name": "temperature", "parameters": schema}}],
                            tool_choice="required")
        elif protocol == "messages":
            path, body = "/v1/messages", {"messages": PROMPT, "max_tokens": 128}
            if tools:
                body.update(tools=[{"name": "temperature", "input_schema": schema}], tool_choice={"type": "any"})
        elif protocol == "responses":
            path, body = "/v1/responses", {"input": PROMPT, "store": False}
            if tools:
                body.update(tools=[{"type": "function", "name": "temperature", "parameters": schema}],
                            tool_choice="required")
        else:
            return await self.client.post(f"/v1beta/models/{model}:generateContent",
                                          json={"contents": [{"parts": [{"text": "Read the temperature."}]}]})
        return await self.client.post(path, json={**body, "model": model})

    def catalog_fixture(self, catalog):
        for module, name in ((models_api, "get_express_models"),
                             (gemini_native, "get_express_models"), (claude_api, "_available_models")):
            self.enterContext(patch.object(module, name, AsyncMock(side_effect=lambda: list(catalog))))
        sdk = genai.Client(api_key="test-key", http_options=types.HttpOptions(
            base_url="http://local", api_version="v1beta",
            async_client_args={"transport": httpx.ASGITransport(app=app)},
            retry_options=types.HttpRetryOptions(attempts=1)))
        self.addCleanup(sdk.close)
        self.addAsyncCleanup(sdk.aio.aclose)
        return sdk

    async def catalogs(self, sdk):
        headers = {"anthropic-version": "2023-06-01"}
        # Every protocol must be discovery-only even when rotation is enabled.
        with (patch.object(credential_manager, "_select", side_effect=AssertionError("credential reservation")),
              patch.object(self.key_manager, "get_express_record", side_effect=AssertionError("key reservation"))):
            self.wire.clear()
            openai = await self.client.get("/v1/models")
            claude = await self.client.get("/v1/models?limit=100", headers=headers)
            pager = await sdk.aio.models.list(config={"page_size": 2})
            native = {model.name.removeprefix("models/"): model async for model in pager}

            entries = {}
            for kind, response, request_headers in (("openai", openai, {}), ("claude", claude, headers)):
                self.assertEqual(response.status_code, 200, response.text)
                found = {entry["id"]: entry for entry in response.json()["data"]}
                for model_id in found:
                    retrieved = await self.client.get("/v1/models/" + model_id, headers=request_headers)
                    self.assertEqual(retrieved.status_code, 200, retrieved.text)
                    self.assertEqual(retrieved.json()["id"], model_id)
                    if kind == "claude":
                        self.assertEqual(retrieved.json()["display_name"], found[model_id]["display_name"])
                entries[kind] = found

            for model in native.values():
                retrieved = await sdk.aio.models.get(model=model.name)
                self.assertEqual(retrieved.model_dump(), model.model_dump())
            self.assertEqual(self.wire, [])
        return entries["openai"], entries["claude"], native

    async def test_discovery_tracks_exact_projection_and_native_count_only(self):
        match = resource()
        mismatch = resource(project="other")
        regional = resource(location="us-west2")
        opaque = "endpoints/configured-fixture"
        raw = [MODEL, match, mismatch, regional, opaque]
        sdk = self.catalog_fixture(raw)
        self.settings.update(roundrobin=True, cross_channel_fallback=True)
        self.channel = "web_proxy"
        # A disabled matching row or a shared project on an incomplete row
        # cannot override the populated project of the complete enabled pool.
        self.cookie_rows.extend((self.cookie_record("disabled", "other", enabled=False),
                                 self.cookie_record("incomplete", "")))
        app_state.get_project_id.return_value = "other"
        translated, claude, native = await self.catalogs(sdk)
        supported = {MODEL, match, MODEL + "-search", match + "-search"}
        self.assertEqual(set(translated), supported)
        self.assertEqual({model for model in claude if not model.startswith("claude-")}, supported)
        self.assertTrue(any(model.startswith("claude-") for model in claude))

        for name in (MODEL, match):
            self.assertEqual(native[name].supported_actions, ["generateContent", "streamGenerateContent", "countTokens"])
            self.wire.clear()
            output = await sdk.aio.models.generate_content(model=native[name].name, contents="offline")
            self.assertEqual(output.text, "A fixture reply.")
            self.assertEqual([body["model"] for _, body in self.wire], [match])
            self.wire.clear()
            chunks = await sdk.aio.models.generate_content_stream(model=native[name].name, contents="offline")
            self.assertEqual("".join([chunk.text or "" async for chunk in chunks]), "A fixture reply.")
            self.assertEqual([body["model"] for _, body in self.wire], [match])
        for bad in (mismatch, regional, opaque):
            with self.subTest(bad=bad):
                self.assertEqual(native[bad].supported_actions, ["countTokens"])
                for headers in ({}, {"anthropic-version": "2023-06-01"}):
                    missing = await self.client.get("/v1/models/" + bad, headers=headers)
                    self.assertEqual(missing.status_code, 404, missing.text)
                for protocol in ("chat", "messages", "responses", "native"):
                    denied = await self.send(protocol, bad)
                    self.assertEqual(denied.status_code, 400, denied.text)
                    self.assertEqual(self.wire, [])
                with self.assertRaises(errors.ClientError) as failed:
                    stream = await sdk.aio.models.generate_content_stream(model=native[bad].name, contents="offline")
                    await anext(stream)
                self.assertEqual(failed.exception.code, 400)
                self.assertEqual(self.wire, [])
                count = await sdk.aio.models.count_tokens(model=native[bad].name, contents="offline")
                self.assertEqual(count.total_tokens, 12)
                self.assertEqual([path for path, _ in self.wire], ["/v1beta1/" + bad + ":countTokens"])

        # Removing the Key removes the count-only row entirely, not just the method.
        self.express_rows.clear()
        _, _, native = await self.catalogs(sdk)
        self.assertEqual(set(native), supported)
        with self.assertRaises(errors.ClientError) as missing:
            await sdk.aio.models.get(model="models/" + mismatch)
        self.assertEqual((missing.exception.code, missing.exception.status), (404, "NOT_FOUND"))

    async def test_discovery_matches_existing_fallback_and_legacy_project_contracts(self):
        match = resource()
        raw = [MODEL, match]
        sdk = self.catalog_fixture(raw)
        key = dict(self.express_rows[0])
        complete = self.cookie_record("complete", PROJECT)
        incomplete = self.cookie_record("incomplete", "")
        cases = [
            ("api_key", [], [complete], False, False, True),
            ("api_key", [], [complete], True, True, True),
            ("web_proxy", [key], [], False, False, True),
            ("web_proxy", [key], [], True, True, True),
            ("web_proxy", [], [incomplete], True, False, True),
            ("mixed", [], [incomplete], True, False, True),
            ("mixed", [], [complete], False, True, True),
            ("mixed", [], [], True, False, False),
        ]
        app_state.get_project_id.return_value = PROJECT
        self.settings["roundrobin"] = True
        variants = {MODEL, match, MODEL + "-search", match + "-search"}
        for self.channel, keys, cookies, fallback, translated_ok, native_ok in cases:
            with self.subTest(channel=self.channel, keys=len(keys), cookies=len(cookies), fallback=fallback):
                self.express_rows[:] = keys
                self.cookie_rows[:] = cookies
                self.settings["cross_channel_fallback"] = fallback
                openai, claude, native = await self.catalogs(sdk)
                self.assertEqual(set(openai), variants if translated_ok else set())
                self.assertEqual({k for k in claude if not k.startswith("claude-")}, set(openai))
                self.assertEqual(any(k.startswith("claude-") for k in claude), translated_ok)
                self.assertEqual(set(native), variants if native_ok else set())
                results = [await self.send(kind, match) for kind in ("chat", "messages", "responses", "native")]
                self.assertEqual([resp.status_code == 200 for resp in results],
                                 [translated_ok, translated_ok, translated_ok, native_ok])

    async def test_claude_discovery_and_execution_choose_eligible_scoped_text_leaf(self):
        self.channel = "web_proxy"
        good = resource(model="gemini-3.6-pro")
        unsupported = resource(project="other", model="gemini-4.8-pro")
        opaque = "projects/opaque/locations/global/endpoints/gemini-999-pro"
        raw = [unsupported, good, resource(model="gemini-3.1-flash-image"), opaque]
        sdk = self.catalog_fixture(raw)
        alias = "claude-opus-4-1"
        _, claude, _ = await self.catalogs(sdk)
        self.assertEqual(claude[alias]["display_name"], f"Claude 别名 （映射 {good}）")
        response = await self.send("messages", alias)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["model"], alias)
        self.assertEqual([body["model"] for _, body in self.wire], [good])
        self.wire.clear()
        headers = {"anthropic-version": "2023-06-01"}
        counted = await self.client.post("/v1/messages/count_tokens", headers=headers,
                                         json={"model": alias, "messages": PROMPT})
        self.assertEqual((counted.status_code, counted.json()), (200, {"input_tokens": 12}))
        self.assertEqual([path for path, _ in self.wire], ["/v1beta1/" + good + ":countTokens"])

        raw.remove(good)
        _, claude, _ = await self.catalogs(sdk)
        self.assertNotIn(alias, claude)  # no hardcoded or opaque substitute for discovery
        missing = await self.client.get("/v1/models/" + alias, headers=headers)
        self.assertEqual((missing.status_code, missing.json()["error"]["type"]), (404, "not_found_error"))
        rejected = await self.send("messages", alias)
        self.assertEqual(rejected.status_code, 400, rejected.text)
        self.assertEqual(self.wire, [])

        # Express can still address an explicitly configured opaque locator;
        # do not infer that it is a Claude text target from its final segment.
        self.channel = "api_key"
        raw[:] = [opaque]
        translated, claude, _ = await self.catalogs(sdk)
        self.assertIn(opaque, translated)
        self.assertIn(opaque, claude)
        self.assertNotIn(alias, claude)
        explicit = await self.send("messages", opaque)
        self.assertEqual(explicit.status_code, 200, explicit.text)
        self.assertEqual([path for path, _ in self.wire], ["/v1beta1/" + opaque + ":generateContent"])

    async def test_native_cursor_binds_methods_even_when_ids_and_key_presence_are_unchanged(self):
        first_scope = resource(project="a-first")
        last_scope = resource(project="z-last")
        sdk = self.catalog_fixture([first_scope, last_scope])
        self.channel = "web_proxy"
        self.cookie_rows[:] = [self.cookie_record("first", "a-first")]
        first = await sdk.aio.models.list(config={"page_size": 1})
        self.assertEqual(first.page[0].supported_actions, ["generateContent", "streamGenerateContent", "countTokens"])
        token = (await self.client.get("/v1beta/models?pageSize=1")).json()["nextPageToken"]
        params = {"page_size": 1, "page_token": token}
        second = await sdk.aio.models.list(config=params)
        self.assertNotEqual(first.page[0].name, second.page[0].name)
        original = [model.name async for model in await sdk.aio.models.list(config={"page_size": 1})]
        self.cookie_rows[:] = [self.cookie_record("last", "z-last")]
        changed = [model.name async for model in await sdk.aio.models.list(config={"page_size": 1})]
        self.assertEqual(original, changed)
        self.assertEqual((await sdk.aio.models.get(model=first.page[0].name)).supported_actions, ["countTokens"])
        with self.assertRaises(errors.ClientError) as stale:
            await sdk.aio.models.list(config=params)
        self.assertEqual((stale.exception.code, stale.exception.status), (400, "INVALID_ARGUMENT"))
        self.assertEqual(self.wire, [])

    async def test_claude_alias_metadata_and_generation_keep_the_same_scoped_target(self):
        flash = resource()
        pro = resource(model="gemini-2.5-pro")
        lite = resource(model="gemini-2.5-flash-lite")
        expected = {"claude-opus-4-1": pro, "claude-sonnet-4-5": flash, "claude-haiku-4-5": lite}
        self.express_rows[0]["project_id"] = "other-key-project"
        self.enterContext(patch.object(
            claude_api, "_available_models", AsyncMock(return_value=[flash, pro, lite])))
        headers = {"anthropic-version": "2023-06-01"}

        for self.channel in ("api_key", "web_proxy"):
            self.wire.clear()
            listing = await self.client.get("/v1/models?limit=3", headers=headers)
            self.assertEqual(listing.status_code, 200, listing.text)
            self.assertTrue(listing.json()["has_more"])
            self.assertEqual([entry["id"] for entry in listing.json()["data"]], list(expected))
            entries_by_id = {}
            for entry in listing.json()["data"]:
                retrieved = await self.client.get(f"/v1/models/{entry['id']}", headers=headers)
                self.assertEqual(retrieved.status_code, 200, retrieved.text)
                entries_by_id[entry["id"]] = (entry, retrieved.json())
            self.assertEqual(self.wire, [])

            for alias, target in expected.items():
                with self.subTest(channel=self.channel, alias=alias):
                    response = await self.send("messages", alias)
                    self.assertEqual(response.status_code, 200, response.text)
                    self.assertEqual(response.json()["model"], alias)
                    self.assertEqual(len(self.wire), 1)
                    path, body = self.wire[0]
                    if self.channel == "web_proxy":
                        self.assertEqual(body["model"], target)
                    else:
                        self.assertEqual(path, "/v1beta1/" + target + ":generateContent")
                    for entry in entries_by_id[alias]:
                        self.assertEqual(entry["display_name"], f"Claude 别名 （映射 {target}）")

        self.channel = "api_key"
        self.wire.clear()
        count = await self.client.post("/v1/messages/count_tokens", headers=headers,
                                      json={"model": "claude-opus-4-1", "messages": PROMPT})
        self.assertEqual((count.status_code, count.json()), (200, {"input_tokens": 12}))
        self.assertEqual([path for path, _ in self.wire], ["/v1beta1/" + pro + ":countTokens"])

    async def test_qualified_and_bare_text_use_same_provider_url_tools_and_defaults(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for protocol in ("chat", "messages", "responses"):
                with self.subTest(channel=channel, protocol=protocol):
                    bare = await self.send(protocol, MODEL, tools=True)
                    self.assertEqual(bare.status_code, 200, bare.text[:220])
                    path, bare_body = self.wire[0]
                    scoped = await self.send(protocol, resource(), tools=True)
                    self.assertEqual(scoped.status_code, 200, scoped.text[:220])
                    self.assertEqual(len(self.wire), 1)
                    self.assertEqual(self.wire[0], (path, bare_body))
                    self.assertEqual(bare_body["toolConfig"]["functionCallingConfig"]["mode"], "ANY")
                    self.assertEqual(bare_body["generationConfig"]["maxOutputTokens"],
                                     128 if protocol == "messages" else 321)
                    if channel == "web_proxy":
                        self.assertEqual(bare_body["model"], resource())
                    else:
                        self.assertTrue(path.endswith("/" + resource() + ":generateContent"), path)
        self.assertEqual(timeout_seconds(resource()), 18)

    async def test_cookie_scope_is_checked_before_any_translated_or_native_dispatch(self):
        self.channel = "web_proxy"
        for protocol in ("chat", "messages", "responses", "native"):
            for target in (MODEL, resource(), f"models/{MODEL}", f"publishers/google/models/{MODEL}"):
                with self.subTest(protocol=protocol, target=target):
                    reply = await self.send(protocol, target)
                    self.assertEqual(reply.status_code, 200, reply.text[:220])
                    self.assertEqual(len(self.wire), 1)
                    self.assertEqual(self.wire[0][1]["model"], resource())
            for target in (resource(project="other"), resource(location="us-west2")):
                with self.subTest(protocol=protocol, target=target):
                    reply = await self.send(protocol, target)
                    self.assertEqual(reply.status_code, 400, reply.text[:220])
                    self.assertIn("本账号项目 global", reply.json()["error"]["message"])
                    self.assertEqual(self.wire, [])

    async def test_scoped_cookie_rotation_uses_only_eligible_project_before_advancing(self):
        self.channel = "web_proxy"
        self.settings["roundrobin"] = True
        project_a = "project-a"
        self.cookie_rows[:] = [self.cookie_record(name, project, enabled=enabled) for name, project, enabled in (
            ("a", project_a, True), ("b1", PROJECT, True), ("disabled", PROJECT, False),
            ("b2", PROJECT, True), ("incomplete", "", True))]
        for protocol in ("chat", "messages", "responses", "native"):
            with self.subTest(protocol=protocol):
                credential_manager._indices.clear()
                sequence = [(PROJECT, "b1"), (project_a, "a"), (PROJECT, "b2"), (project_a, "a")]
                for project, account in sequence * 2:
                    reply = await self.send(protocol, resource(project=project))
                    self.assertEqual(reply.status_code, 200, reply.text)
                    self.assertEqual(self.wire_accounts, ["SAPISID=fixture-" + account])
                    self.assertEqual([body["model"] for _, body in self.wire], [resource(project=project)])
                # Unscoped selection keeps its original rotation, independent of scoped traffic.
                for model, project, account in ((MODEL, project_a, "a"),
                        ("models/" + MODEL, PROJECT, "b1"), ("publishers/google/models/" + MODEL, PROJECT, "b2")):
                    reply = await self.send(protocol, model)
                    self.assertEqual(reply.status_code, 200, reply.text)
                    self.assertEqual(self.wire_accounts, ["SAPISID=fixture-" + account])
                    self.assertEqual([body["model"] for _, body in self.wire], [resource(project=project)])

    async def test_mixed_pool_does_not_randomly_route_valid_explicit_targets_to_wrong_cookie(self):
        self.channel = "mixed"
        self.settings.update(roundrobin=True, express_location="us-east1")
        self.express_rows[0]["project_id"] = "some-other-key-project"
        self.cookie_rows[:] = [self.cookie_record("a", "project-a"), self.cookie_record("b1", PROJECT),
                              self.cookie_record("b2", PROJECT)]
        regional = resource(location="us-west2")
        for protocol in ("chat", "messages", "responses", "native"):
            with self.subTest(protocol=protocol):
                credential_manager._indices.clear()
                for _ in range(3):
                    reply = await self.send(protocol, regional)
                    self.assertEqual(reply.status_code, 200, reply.text)
                    self.assertEqual(self.wire_accounts, ["express"])
                    self.assertEqual([path for path, _ in self.wire], ["/v1beta1/" + regional + ":generateContent"])
                sequence = (["express"] if protocol == "native" else
                            ["express", "SAPISID=fixture-b1", "SAPISID=fixture-b2"])
                for account in sequence * 2:
                    reply = await self.send(protocol, resource())
                    self.assertEqual(reply.status_code, 200, reply.text)
                    self.assertEqual(self.wire_accounts, [account])
                    path, body = self.wire[0]
                    if account == "express":
                        self.assertEqual(path, "/v1beta1/" + resource() + ":generateContent")
                    else:
                        self.assertEqual(body["model"], resource())

    async def test_mixed_scoped_errors_distinguish_missing_unsupported_and_cooling(self):
        self.channel = "mixed"
        self.settings["roundrobin"] = True
        self.express_rows.clear()
        self.cookie_rows[:] = [self.cookie_record("other", "project-a"), self.cookie_record("b1", PROJECT),
                              self.cookie_record("b2", PROJECT)]
        clock = SimpleNamespace(now=0.0)
        self.enterContext(patch("credential_manager.time", SimpleNamespace(time=lambda: clock.now)))
        for clock.now, account in zip((990.0, 1000.0, 1005.0), self.cookie_rows, strict=True):
            for _ in range(COOKIE_AUTH_FAILURE_THRESHOLD):
                credential_manager.record_cookie_auth_failure(account["id"], credential=account)
        self.cookie_rows.append(self.cookie_record("fresh-other", "project-c"))
        clock.now = 1006.0
        for protocol in ("chat", "messages", "responses"):
            with self.subTest(protocol=protocol):
                incompatible = await self.send(protocol, resource(location="us-west2"))
                self.assertEqual(incompatible.status_code, 400, incompatible.text)
                self.assertNotIn("retry-after", incompatible.headers)
                cooling = await self.send(protocol, resource())
                self.assertEqual(cooling.status_code, 503, cooling.text)
                self.assertEqual(cooling.headers["retry-after"], str(COOKIE_COOLDOWN_SECONDS - 6))
                self.assertEqual(self.wire, [])
        self.assertEqual(credential_manager._indices, {})
        clock.now = 1000.0 + COOKIE_COOLDOWN_SECONDS
        reply = await self.send("chat", resource())
        self.assertEqual(reply.status_code, 200, reply.text)
        self.assertEqual(self.wire_accounts, ["SAPISID=fixture-b1"])

        self.cookie_rows.clear()
        for protocol in ("chat", "messages", "responses", "native"):
            reply = await self.send(protocol, resource())
            self.assertEqual(reply.status_code, 403 if protocol == "native" else 401, reply.text)
            self.assertEqual(self.wire, [])

    async def test_pure_cookie_scoped_cooling_cannot_escape_the_requested_project(self):
        self.channel = "web_proxy"
        self.settings["roundrobin"] = True
        self.express_rows.clear()
        matching = self.cookie_record("matching", PROJECT)
        self.cookie_rows[:] = [self.cookie_record("healthy-other", "project-a"), matching]
        for protocol in ("chat", "native"):
            for _ in range(COOKIE_AUTH_FAILURE_THRESHOLD):
                credential_manager.record_cookie_auth_failure(matching["id"], credential=matching)
            self.assertGreater(credential_manager._cookie_cooldown_remaining(matching["id"]), 0)
            # Cookie-only mode already permits another attempt when its eligible pool is all cooling.
            reply = await self.send(protocol, resource())
            self.assertEqual(reply.status_code, 200, reply.text)
            self.assertEqual(self.wire_accounts, ["SAPISID=fixture-matching"])
            self.assertEqual([body["model"] for _, body in self.wire], [resource()])
            self.assertEqual(credential_manager._cookie_cooldown_remaining(matching["id"]), 0)

    async def test_scoped_cross_channel_fallback_never_rotates_to_an_incompatible_cookie(self):
        self.settings.update(roundrobin=True, cross_channel_fallback=True)
        self.express_error = True
        self.cookie_rows[:] = [self.cookie_record("a", "project-a"), self.cookie_record("b", PROJECT)]
        select = self.enterContext(patch.object(credential_manager, "get_cookie_record",
                                              wraps=credential_manager.get_cookie_record))
        for protocol in ("chat", "messages", "responses"):
            with self.subTest(protocol=protocol):
                before = select.call_count
                reply = await self.send(protocol, resource())
                self.assertEqual(reply.status_code, 200, reply.text)
                self.assertEqual(self.wire_accounts, ["express", "SAPISID=fixture-b"])
                self.assertEqual(self.wire[1][1]["model"], resource())
                self.assertEqual(select.call_count, before + 1)
                for target in (resource(project="missing"), resource(location="us-west2")):
                    unavailable = await self.send(protocol, target)
                    self.assertEqual(unavailable.status_code, 503, unavailable.text)
                    self.assertEqual(self.wire_accounts, ["express"])
                    self.assertEqual(select.call_count, before + 1)

    async def test_native_scoped_legacy_project_remains_available_for_incomplete_only_pool(self):
        self.channel = "web_proxy"
        self.express_rows.clear()
        self.cookie_rows[:] = [self.cookie_record("incomplete", "")]
        app_state.get_project_id.return_value = PROJECT
        reply = await self.send("native", resource())
        self.assertEqual(reply.status_code, 200, reply.text)
        self.assertEqual(self.wire_accounts, ["SAPISID=fixture-incomplete"])
        self.assertEqual([body["model"] for _, body in self.wire], [resource()])
        for target in (resource(project="other"), resource(location="us-west2")):
            reply = await self.send("native", target)
            self.assertEqual(reply.status_code, 400, reply.text)
            self.assertEqual(self.wire, [])

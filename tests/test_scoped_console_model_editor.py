"""The console saves the exact configured ID and inherits without owning it."""
import tempfile
import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch
from urllib.parse import quote

import httpx

import console_auth
import main
import runtime_state
from routes import settings_api


LEAF = "gemini-3.5-flash-lite"
PROJECT = f"projects/first/locations/global/publishers/google/models/{LEAF}"
OTHER = PROJECT.replace("/first/", "/second/")
PUBLISHER = f"publishers/google/models/{LEAF}"
RELATIVE = f"models/{LEAF}"
ALIAS = PROJECT + "-search"
OPAQUE = "projects/first/locations/global/endpoints/example"
ROOT = "/api/model-overrides"
BUILTIN = {"anti_truncation_mode": "both"}
BASE = {"first_reply_timeout_seconds": 23, "default_max_tokens": 321}


def model_url(model, *, encode=True):
    return f"{ROOT}/{quote(model, safe='') if encode else model}"


class ScopedConsoleModelEditorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        fs = self.enterContext(tempfile.TemporaryDirectory())
        ctx = self.enterContext(ExitStack())
        ctx.enter_context(patch.object(runtime_state, "STATE_FILE", fs + "/state.json"))
        self.state = runtime_state.AppState()
        self.state.set_model_override(LEAF, BASE)
        self.state.set_model_override(OTHER, {"default_max_tokens": 654})
        ctx.enter_context(patch.object(settings_api, "app_state", self.state))
        self.catalog = ctx.enter_context(patch.object(settings_api, "get_express_models", AsyncMock(
            return_value=[LEAF, PROJECT, OTHER, PUBLISHER, RELATIVE, ALIAS, OPAQUE])))
        token = console_auth.issue_session()
        self.addCleanup(console_auth.revoke_session, token)
        # ASGITransport decodes the HTTP path once, as Uvicorn does. Some
        # versions of the synchronous TestClient unquote it a second time.
        self.client = await self.enterAsyncContext(httpx.AsyncClient(
            transport=httpx.ASGITransport(main.app), base_url="http://local",
            cookies={console_auth.AUTH_COOKIE: token}, follow_redirects=False))

    async def test_scoped_save_inherit_and_reset_preserve_exact_ownership(self):
        for model in (PROJECT, PUBLISHER, RELATIVE):
            with self.subTest(model=model):
                exact_url = model_url(model)
                empty = await self.client.post(exact_url, json={})
                self.assertEqual(empty.status_code, 200, empty.text)
                self.assertEqual(empty.json()["saved_override"], {})
                absent = await self.client.delete(exact_url)
                self.assertEqual(absent.status_code, 200, absent.text)
                self.assertEqual(absent.json()["status"], "not_found")
                self.assertNotIn(model, self.state.get_saved_model_overrides())
                previous = {"default_max_tokens": 987, "default_top_p": None}
                saved = await self.client.post(exact_url, json={
                    **previous,
                    "_expected": {key: {"present": False} for key in previous},
                })
                self.assertEqual(saved.status_code, 200, saved.text)
                self.assertEqual(saved.json(), {
                    "status": "success", "model": model, "override": previous, "saved_override": previous,
                })
                reloaded = runtime_state.AppState()
                self.assertEqual(reloaded.get_effective_settings(model)["default_max_tokens"], 987)
                # Preserve runtime's exact-record precedence, not field-wise
                # merging of two independently owned override records.
                self.assertEqual(reloaded.get_effective_settings(model)["first_reply_timeout_seconds"], 0)
                self.assertEqual(reloaded.get_effective_settings(LEAF)["default_max_tokens"], 321)
                self.assertEqual(reloaded.get_effective_settings(OTHER)["default_max_tokens"], 654)
                listed = (await self.client.get(ROOT)).json()
                self.assertEqual(listed[model], previous)

                stale = await self.client.request("DELETE", exact_url, json={"expected_override": BASE})
                self.assertEqual(stale.status_code, 409, stale.text)
                self.assertEqual(stale.json()["field"], model)
                stale_post = await self.client.post(exact_url, json={
                    "_inherit": ["default_top_p"], "_expected": {"default_top_p": {"present": False}},
                })
                self.assertEqual(stale_post.status_code, 409, stale_post.text)
                inherited = await self.client.post(model_url(model, encode=False), json={
                    "_inherit": ["default_top_p"],
                    "_expected": {"default_top_p": {"present": True, "value": None}},
                })
                self.assertEqual(inherited.status_code, 200, inherited.text)
                self.assertEqual(inherited.json()["saved_override"], {"default_max_tokens": 987})
                gone = await self.client.request("DELETE", exact_url, json={
                    "expected_override": {"default_max_tokens": 987},
                })
                self.assertEqual(gone.status_code, 200, gone.text)
                self.assertEqual(gone.json()["saved_override"], {})
                self.assertEqual(gone.json()["override"], {})
                persisted = runtime_state.AppState()
                self.assertNotIn(model, persisted.get_saved_model_overrides())
                self.assertEqual(persisted.get_effective_settings(model)["first_reply_timeout_seconds"], 23)
                self.assertEqual(persisted.get_effective_settings(model)["anti_truncation_mode"], "both")

    async def test_capabilities_keep_owned_maps_exact_and_describe_runtime_fallback_order(self):
        result = await self.client.get("/api/capabilities")
        self.assertEqual(result.status_code, 200, result.text)
        caps = result.json()
        for model in (PROJECT, OTHER, RELATIVE, PUBLISHER, ALIAS):
            self.assertIn(model, caps["models"])
            self.assertIn(model, caps["capabilities"])
        self.assertEqual(caps["override_fallbacks"], {
            PROJECT: [LEAF], OTHER: [LEAF], PUBLISHER: [LEAF], RELATIVE: [LEAF],
            ALIAS: [PROJECT, LEAF + "-search", LEAF],
        })
        self.assertEqual(caps["overrides"][LEAF], {**BASE, **BUILTIN})
        self.assertEqual(caps["saved_overrides"][LEAF], BASE)
        self.assertEqual(caps["builtin_overrides"][LEAF], BUILTIN)
        self.assertNotIn(PROJECT, caps["overrides"])
        self.assertNotIn(PROJECT, caps["saved_overrides"])
        self.assertNotIn(PROJECT, caps["builtin_overrides"])
        self.assertEqual(caps["overrides"][OTHER], {"default_max_tokens": 654})

        for model, max_tokens in ((LEAF + "-search", 456), (PROJECT, 789), (ALIAS, 123)):
            self.state.set_model_override(model, {"default_max_tokens": max_tokens})
            effective = self.state.get_effective_settings(ALIAS)
            self.assertEqual(effective["default_max_tokens"], max_tokens)
            self.assertEqual(effective["first_reply_timeout_seconds"], 0)

    async def test_unlisted_and_malformed_scopes_do_not_write(self):
        before = self.state.get_saved_model_overrides()
        for model in (OPAQUE, PROJECT + "/", PROJECT.replace("/first/", "/unlisted/"),
                      quote(PROJECT, safe=""), quote(PROJECT, safe="").replace("%2F", "%2f"),
                      PROJECT.replace("/first/", "//first/")):
            with self.subTest(model=model):
                self.assertEqual((await self.client.post(model_url(model), json=BASE)).status_code, 404)
                self.assertEqual((await self.client.delete(model_url(model))).status_code, 404)
        self.assertEqual(self.state.get_saved_model_overrides(), before)

    async def test_configured_literal_percent_id_does_not_alias_a_decoded_scope(self):
        literal = quote(PROJECT, safe="")
        self.catalog.return_value += [literal]
        reply = await self.client.post(model_url(literal), json={"default_max_tokens": 990})
        self.assertEqual(reply.status_code, 200, reply.text)
        self.assertEqual(reply.json()["model"], literal)
        self.assertEqual(self.state.get_saved_model_overrides()[literal], {"default_max_tokens": 990})
        self.assertNotIn(PROJECT, self.state.get_saved_model_overrides())
        self.assertEqual(self.state.get_effective_settings(PROJECT)["default_max_tokens"], 321)
        self.assertEqual((await self.client.delete(model_url(literal))).status_code, 200)

    async def test_legacy_bare_empty_routes_and_authentication(self):
        legacy = "not-in-current-catalog"
        self.assertEqual((await self.client.post(ROOT, json=BASE)).status_code, 405)
        self.assertEqual((await self.client.delete(ROOT + "/")).status_code, 307)
        self.assertEqual((await self.client.post(model_url(legacy) + "/", json=BASE)).status_code, 307)
        reply = await self.client.post(model_url(legacy), json=BASE)
        self.assertEqual(reply.status_code, 200, reply.text)
        self.assertEqual(reply.json()["saved_override"], BASE)
        self.assertEqual((await self.client.delete(model_url(legacy))).status_code, 200)
        self.catalog.assert_not_awaited()

        self.client.cookies.clear()
        self.assertEqual((await self.client.get("/api/capabilities")).status_code, 401)
        self.assertEqual((await self.client.post(model_url(PROJECT), json=BASE)).status_code, 401)
        self.assertEqual((await self.client.delete(model_url(PROJECT))).status_code, 401)
        self.catalog.assert_not_awaited()

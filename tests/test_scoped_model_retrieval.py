"""The model catalog must be retrievable by exactly its advertised IDs."""

from unittest.mock import AsyncMock, patch
from urllib.parse import quote

from routes import claude_api, models_api
from test_schema_contract_fidelity import SchemaRouteHarness


MODEL = "gemini-3-flash-preview"
SCOPED = f"projects/fixture/locations/global/publishers/google/models/{MODEL}"
NAMES = [MODEL, f"models/{MODEL}", f"publishers/google/models/{MODEL}", SCOPED]
ANTHROPIC = {"anthropic-version": "2023-06-01"}


class ScopedModelRetrievalTests(SchemaRouteHarness):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.openai_catalog = self.enterContext(patch.object(
            models_api, "get_express_models", AsyncMock(return_value=NAMES)))
        self.anthropic_catalog = self.enterContext(patch.object(
            claude_api, "_available_models", AsyncMock(return_value=NAMES)))

    async def test_protocol_lists_every_id_that_exact_retrieval_can_get(self):
        for headers, shape_field in (({}, "object"), (ANTHROPIC, "type")):
            response = await self.client.get("/v1/models", headers=headers)
            self.assertEqual(response.status_code, 200, response.text[:240])
            listed = {entry["id"] for entry in response.json()["data"]}
            variants = [*NAMES, f"{SCOPED}-search"]
            self.assertTrue(set(variants) <= listed)
            for ident in variants:
                for path in {ident, quote(ident, safe="")}:
                    with self.subTest(headers=headers, ident=ident, path=path):
                        result = await self.client.get(f"/v1/models/{path}", headers=headers)
                        self.assertEqual(result.status_code, 200, result.text[:240])
                        self.assertEqual(result.json()["id"], ident)
                        self.assertEqual(result.json()[shape_field], "model")

    async def test_unlisted_ids_cannot_resolve_to_the_listed_leaf_or_another_scope(self):
        missing = SCOPED.replace("/fixture/", "/different-project/")
        for headers in ({}, ANTHROPIC):
            for ident in (missing, f"{SCOPED}-antitruncation-stream", quote(SCOPED, safe="")):
                with self.subTest(headers=headers, ident=ident):
                    # The last case is encoded twice; the remaining literal
                    # %2F is not an alternate notation for the listed name.
                    result = await self.client.get(f"/v1/models/{quote(ident, safe='')}", headers=headers)
                    self.assertEqual(result.status_code, 404, result.text[:240])
                    if headers:
                        self.assertEqual(result.json()["error"]["type"], "not_found_error")
                    else:
                        self.assertIn("does not exist", result.json()["detail"])
        self.assertEqual(self.sent, [])  # Retrieval never probes an upstream model.

    async def test_scoped_retrieval_auth_and_native_post_are_unchanged(self):
        unauthorized = await self.client.get(f"/v1/models/{quote(SCOPED, safe='')}", headers={
            **ANTHROPIC, "Authorization": "Bearer wrong-key",
        })
        self.assertEqual(unauthorized.status_code, 401)
        self.assertEqual(unauthorized.json()["error"]["type"], "authentication_error")
        self.openai_catalog.assert_not_awaited()
        self.anthropic_catalog.assert_not_awaited()
        self.channel = "web_proxy"
        self.reply = {"text": "Native fixture."}
        native = await self.client.post(f"/v1/models/{SCOPED}:generateContent", json={
            "contents": [{"role": "user", "parts": [{"text": "A harmless fixture."}]}],
        })
        self.assertEqual(native.status_code, 200, native.text[:240])
        self.assertEqual(native.json()["candidates"][0]["content"]["parts"], [{"text": "Native fixture."}])
        self.assertEqual(len(self.sent), 1)

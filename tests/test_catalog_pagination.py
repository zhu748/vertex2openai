"""Discover models page by page through public SDK/REST routes, with no live provider."""
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch
from urllib.parse import quote

import httpx
from google import genai
from google.genai import errors, types

from main import app
from model_catalog import native_catalog_ids
from routes import claude_api, gemini_native, models_api
from upstreams import native_passthrough


class CatalogPagination(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.raw = [f"gemini-fixture-{n:02}" for n in range(29)]
        for module in (gemini_native, claude_api, models_api):
            self.enterContext(patch.object(module, "get_express_models", AsyncMock(side_effect=lambda: self.raw)))
        self.enterContext(patch.object(app.state.express_key_manager, "get_total_keys", return_value=1))
        self.queries = []

        async def observe(scope, receive, send):
            self.queries.append(scope["query_string"])
            await app(scope, receive, send)

        transport = httpx.ASGITransport(app=observe)
        self.client = await self.enterAsyncContext(httpx.AsyncClient(
            base_url="http://local", transport=transport, headers={"Authorization": "Bearer test-key"}))
        self.sdk = genai.Client(api_key="test-key", http_options=types.HttpOptions(
            base_url="http://local", api_version="v1beta", async_client_args={"transport": transport},
            retry_options=types.HttpRetryOptions(attempts=1)))
        self.addCleanup(self.sdk.close)
        self.addAsyncCleanup(self.sdk.aio.aclose)

    async def gemini(self, **params):
        return await self.client.get("/v1beta/models", params=params)

    async def anthropic(self, **params):
        return await self.client.get("/v1/models", params=params, headers={"anthropic-version": "2023-06-01"})

    async def test_real_google_sdk_multi_page_iteration_and_retrieval(self):
        expected = ["models/" + name for name in native_catalog_ids(self.raw)]
        default = (await self.gemini()).json()
        self.assertEqual(len(default["models"]), 50)
        self.assertIn("nextPageToken", default)
        self.queries.clear()
        pager = await self.sdk.aio.models.list(config={"page_size": 7})
        self.assertEqual(len(pager.page), 7)
        models = [model async for model in pager]
        self.assertEqual([m.name for m in models], expected)
        self.assertEqual(len(self.queries), 9)
        self.assertEqual(self.queries[0], b"pageSize=7")
        self.assertTrue(all(b"pageToken=" in q and b"pageSize=7" in q for q in self.queries[1:]))
        self.assertIsNone(pager.config.get("page_token"))
        retrieved = await self.sdk.aio.models.get(model=expected[-1])
        self.assertEqual(retrieved.name, models[-1].name)
        self.assertEqual(retrieved.supported_actions, models[-1].supported_actions)

    async def test_real_google_sdk_retrieves_exact_advertised_resource_names(self):
        base = "gemini-fixture"
        scoped = f"projects/fixture/locations/global/publishers/google/models/{base}"
        self.raw[:] = [base, f"models/{base}", f"publishers/google/models/{base}", scoped]

        pager = await self.sdk.aio.models.list(config={"page_size": 3})
        self.assertEqual(len(pager.page), 3)
        models = [model async for model in pager]
        self.assertEqual(len(models), 8)
        for listed in models:
            with self.subTest(name=listed.name):
                fetched = await self.sdk.aio.models.get(model=listed.name)
                self.assertEqual(fetched.model_dump(), listed.model_dump())

        # ASGI supplies decoded path components once. Exact retrieval must not
        # strip a relative prefix, infer a leaf, or resolve a different project.
        encoded_scope = quote(scoped, safe="")
        encoded = await self.client.get(f"/v1beta/models/{encoded_scope}")
        self.assertEqual((encoded.status_code, encoded.json()["name"]), (200, f"models/{scoped}"))
        async def assert_not_found(ident):
            with self.assertRaises(errors.ClientError) as caught:
                await self.sdk.aio.models.get(model=f"models/{ident}")
            self.assertEqual((caught.exception.code, caught.exception.status), (404, "NOT_FOUND"))

        self.raw.remove(f"models/{base}")  # the bare ID alone does not authorize the full ID
        await assert_not_found(f"models/{base}")
        self.raw.remove(base)  # the surviving scoped IDs must not authorize the bare one
        for ident in (base, quote(encoded_scope, safe=""), scoped.replace("/fixture/", "/other/"), scoped + ":"):
            with self.subTest(ident=ident):
                await assert_not_found(ident)

        path = "/v1beta/models/" + scoped
        bad_key = await self.client.get(path, headers={"Authorization": "Bearer wrong"})
        self.assertEqual((bad_key.status_code, bad_key.json()["error"]["status"]), (401, "UNAUTHENTICATED"))
        action = await self.client.get(path + ":generateContent")
        self.assertEqual((action.status_code, action.json()["error"]["status"]), (400, "INVALID_ARGUMENT"))

    async def test_real_google_sdk_scoped_post_still_uses_the_full_resource(self):
        scoped = "projects/fixture/locations/global/publishers/google/models/gemini-fixture"
        self.raw[:] = [scoped]
        requests = []

        def wire(request):
            requests.append(request)
            return httpx.Response(200, json={"candidates": [{"content": {
                "role": "model", "parts": [{"text": "offline"}]}, "finishReason": "STOP"}]})

        async with httpx.AsyncClient(transport=httpx.MockTransport(wire)) as upstream:
            with (patch.object(app.state.express_key_manager, "get_express_record",
                               return_value={"key": "offline-fixture", "project_id": "other"}) as reserve,
                  patch.object(gemini_native.app_state, "get_channel_mode", return_value="api_key"),
                  patch.object(native_passthrough.passthrough_client_pool, "get", return_value=upstream)):
                pager = await self.sdk.aio.models.list(config={"page_size": 1})
                listed = pager.page[0]
                await self.sdk.aio.models.get(model=listed.name)
                reserve.assert_not_called()
                self.assertEqual(requests, [])
                response = await self.sdk.aio.models.generate_content(model=listed.name, contents="fixture")
        self.assertEqual(response.text, "offline")
        self.assertEqual([request.url.path for request in requests],
                         [f"/v1beta1/{scoped}:generateContent"])

    async def test_gemini_rejects_bad_or_stale_token_and_invalid_size_without_restarting(self):
        first = (await self.gemini(pageSize=3)).json()
        token = first["nextPageToken"]
        again = (await self.gemini(pageSize=3, pageToken=token)).json()
        self.assertNotEqual(first["models"], again["models"])
        for params in ({"pageSize": "bad"}, {"pageSize": -1}, {"pageSize": 1, "pageToken": token},
                       {"pageToken": "never-issued"}, {"pageSize": 3, "pageToken": token[:-2] + "!!"}):
            with self.subTest(params=params):
                response = await self.gemini(**params)
                self.assertEqual(response.status_code, 400)
                self.assertEqual(response.json()["error"]["status"], "INVALID_ARGUMENT")
        self.raw.insert(0, "gemini-aaa")
        self.assertEqual((await self.gemini(pageSize=3, pageToken=token)).status_code, 400)
        self.assertEqual(len((await self.gemini(pageSize=0)).json()["models"]), 50)
        self.raw[:] = [f"plain-{n:04}" for n in range(1002)]
        big = (await self.gemini(pageSize=1001)).json()
        self.assertEqual(len(big["models"]), 1000)
        last = (await self.gemini(pageSize=1001, pageToken=big["nextPageToken"])).json()
        self.assertEqual(len(last["models"]), 2)
        self.assertNotIn("nextPageToken", last)

    async def test_anthropic_forward_backward_and_empty_page_boundaries(self):
        all_items = (await self.anthropic(limit=1000)).json()["data"]
        ids = [e["id"] for e in all_items]
        self.assertGreater(len(ids), 20)
        self.assertEqual(len((await self.anthropic()).json()["data"]), 20)
        seen, params = [], {"limit": 7}
        for _ in range(len(ids) + 1):
            data = (await self.anthropic(**params)).json()
            chunk = [e["id"] for e in data["data"]]
            self.assertEqual(chunk, ids[len(seen):len(seen) + 7])
            self.assertEqual((data["first_id"], data["last_id"]), (chunk[0], chunk[-1]))
            seen.extend(chunk)
            self.assertIs(data["has_more"], len(seen) < len(ids))
            if not data["has_more"]:
                break
            params["after_id"] = data["last_id"]
        self.assertEqual(seen, ids)
        for cursor, expected, more in ((ids[9], ids[6:9], True), (ids[3], ids[:3], False),
                                       (ids[1], ids[:1], False), (ids[0], [], False)):
            page = (await self.anthropic(limit=3, before_id=cursor)).json()
            self.assertEqual([e["id"] for e in page["data"]], expected)
            self.assertEqual((page["first_id"], page["last_id"], page["has_more"]),
                             (expected[0] if expected else None, expected[-1] if expected else None, more))
        self.assertEqual((await self.anthropic(after_id=ids[-1])).json(),
                         {"data": [], "first_id": None, "last_id": None, "has_more": False})
        # A later-page ID is still available to retrieve, regardless of list defaults.
        retrieved = await self.client.get("/v1/models/" + ids[-1], headers={"anthropic-version": "2023-06-01"})
        self.assertEqual((retrieved.status_code, retrieved.json()["id"]), (200, ids[-1]))

    async def test_validation_is_protocol_specific_and_the_openai_list_is_not_paginated(self):
        for params in ({"limit": 0}, {"limit": 1001}, {"limit": "1.5"}, {"after_id": "not-listed"},
                       {"before_id": "not-listed"}, {"before_id": "one", "after_id": "two"}):
            response = await self.anthropic(**params)
            self.assertEqual(response.status_code, 400)
            self.assertEqual(response.json()["error"]["type"], "invalid_request_error")
        response = await self.client.get("/v1/models?limit=1&after_id=invalid")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(set(response.json()), {"data", "object"})
        self.assertEqual(response.json()["object"], "list")
        self.assertGreater(len(response.json()["data"]), 20)

    async def test_cursors_cannot_rename_duplicate_parameters_or_skip_authentication(self):
        for path in ("/v1beta/models?pageSize=1&page_size=2", "/v1beta/models?pageToken=t&pageToken=u"):
            self.assertEqual((await self.client.get(path)).json()["error"]["status"], "INVALID_ARGUMENT")
        anth = {"anthropic-version": "2023-06-01"}
        bad = await self.client.get("/v1/models?limit=1&limit=3", headers=anth)
        self.assertEqual((bad.status_code, bad.json()["error"]["type"]), (400, "invalid_request_error"))
        for path, headers in (("/v1/models?limit=0", anth), ("/v1beta/models?pageToken=bad", {})):
            result = await self.client.get(path, headers={**headers, "Authorization": "Bearer wrong-key"})
            self.assertEqual(result.status_code, 401)

    async def test_native_empty_directory_has_no_continuation(self):
        self.raw.clear()
        self.assertEqual((await self.gemini(pageSize=1)).json(), {"models": []})

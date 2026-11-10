"""Regressions for image replay, capability routing and concurrent settings."""
import asyncio
import base64
import io
import json
import os
from pathlib import Path
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from types import SimpleNamespace
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-optimization-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from fastapi.testclient import TestClient
from google.genai import types
from PIL import Image
from pydantic import ValidationError

import client_pool
import console_auth
import config
import impersonate
import main
from channel_capabilities import cookie_feature_error
from claude_protocol import anthropic_to_openai_request, ClaudeRequestError
from generation_config import create_generation_config
from message_processing import create_gemini_prompt, process_gemini_response_to_openai_dict
from models import OpenAIMessage, OpenAIRequest
from proxy_resolver import ProxyUsage, current_proxy_usage
from request_validation import validate_openai_request
from response_validation import IncompleteResponseError
from routes import chat_api, settings_api
from runtime_state import AppState, SettingsConflictError
from schema_validation import SchemaValidationError
from upstreams.cookie_native import native_request_to_variables
from upstreams.cookie_request import _convert_messages_to_contents, build_request_body
from upstreams.cookie_response import extract_events
from upstreams.express_response import ToolCallIndexer, _build_chunk_payload


def request(**kwargs):
    return OpenAIRequest(model=kwargs.pop("model", "gemini-2.5-flash"),
                         messages=[{"role": "user", "content": "hello"}], **kwargs)


def snapshot(values, keys):
    return {key: {"present": key in values, **({"value": values[key]} if key in values else {})}
            for key in keys}


class ImageReplayTests(unittest.TestCase):
    def setUp(self):
        image = io.BytesIO()
        Image.new("RGB", (1, 1), "red").save(image, format="PNG")
        self.png = image.getvalue()
        self.parts = [types.Part(text="before"),
                      types.Part(inline_data=types.Blob(data=self.png, mime_type="image/png"), thought_signature=b"image-signature"),
                      types.Part(text="between"),
                      types.Part(inline_data=types.Blob(data=self.png, mime_type="image/png")),
                      types.Part(text="after")]

    def response(self, parts, finish="STOP"):
        return types.GenerateContentResponse(candidates=[types.Candidate(
            content=types.Content(role="model", parts=parts), finish_reason=finish)])

    def assert_replay(self, message):
        self.assertNotIn(base64.b64encode(self.png).decode(), json.dumps(message["extra_content"]))
        message = OpenAIMessage(**message)
        sdk = create_gemini_prompt([message], "gemini-2.5-flash")[0].parts
        cookie = _convert_messages_to_contents([message], "gemini-2.5-flash")[0][0]["parts"]
        self.assertEqual([p.text if p.inline_data is None else "image" for p in sdk],
                         ["before", "image", "between", "image", "after"])
        self.assertEqual([p.get("text", "image") for p in cookie],
                         ["before", "image", "between", "image", "after"])
        self.assertEqual(sdk[1].inline_data.data, self.png)
        self.assertEqual(base64.b64decode(cookie[1]["inlineData"]["data"]), self.png)
        self.assertEqual(sdk[1].thought_signature, b"image-signature")
        self.assertEqual(base64.b64decode(cookie[1]["thoughtSignature"]), b"image-signature")

    def test_nonstream_images_keep_order_signature_and_single_payload(self):
        self.assert_replay(process_gemini_response_to_openai_dict(
            self.response(self.parts), "gemini-2.5-flash")["choices"][0]["message"])

    def test_stream_image_metadata_restores_multiple_chunks(self):
        indexer = ToolCallIndexer()
        message = {"role": "assistant", "content": ""}
        for i, part in enumerate(self.parts):
            delta = _build_chunk_payload(self.response([part], "STOP" if i == 4 else None),
                                         "gemini-2.5-flash", "response", indexer=indexer)["choices"][0]["delta"]
            message["content"] += delta.get("content", "")
            if "extra_content" in delta:
                message["extra_content"] = delta["extra_content"]
        self.assert_replay(message)

    def test_old_image_placeholder_metadata_remains_compatible(self):
        message = process_gemini_response_to_openai_dict(self.response(self.parts), "gemini-2.5-flash")["choices"][0]["message"]
        for item in message["extra_content"]["google"]["ordinary_parts"]:
            if item["kind"] == "image":
                item.update(kind="text", text="[image]")
        self.assert_replay(message)

    def test_literal_image_placeholder_without_image_remains_text(self):
        message = OpenAIMessage(role="assistant", content="[image]", extra_content={
            "google": {"ordinary_parts": [{"kind": "text", "text": "[image]"}]}})
        self.assertEqual(create_gemini_prompt([message])[0].parts[0].text, "[image]")

    def test_thought_image_preserves_reasoning_slot_and_signature(self):
        part = self.parts[1].model_copy(update={"thought": True})
        message = process_gemini_response_to_openai_dict(
            self.response([part, types.Part(text="answer")]), "gemini-2.5-flash")["choices"][0]["message"]
        self.assertIn("data:image/png", message["reasoning_content"])
        message = OpenAIMessage(**message)
        sdk = create_gemini_prompt([message], "gemini-2.5-flash")[0].parts
        cookie = _convert_messages_to_contents([message], "gemini-2.5-flash")[0][0]["parts"]
        self.assertTrue(sdk[0].thought)
        self.assertEqual(sdk[0].thought_signature, b"image-signature")
        self.assertEqual(sdk[0].inline_data.data, self.png)
        self.assertTrue(cookie[0]["thought"])
        self.assertEqual(base64.b64decode(cookie[0]["inlineData"]["data"]), self.png)

    def test_images_interleaved_with_tools_keep_topology(self):
        parts = [self.parts[0], self.parts[1], types.Part(function_call=types.FunctionCall(
            id="call-x", name="lookup", args={})), self.parts[2]]
        message = process_gemini_response_to_openai_dict(self.response(parts), "gemini-2.5-flash")["choices"][0]["message"]
        messages = [OpenAIMessage(**message), OpenAIMessage(role="tool", tool_call_id="call-x", content="done")]
        sdk = create_gemini_prompt(messages, "gemini-2.5-flash")[0].parts
        cookie = _convert_messages_to_contents(messages, "gemini-2.5-flash")[0][0]["parts"]
        self.assertEqual(sdk[0].text, "before")
        self.assertEqual(sdk[1].inline_data.data, self.png)
        self.assertEqual(sdk[2].function_call.name, "lookup")
        self.assertEqual(sdk[3].text, "between")
        self.assertIn("inlineData", cookie[1])
        self.assertEqual(cookie[2]["functionCall"]["name"], "lookup")


class CapabilityTests(unittest.TestCase):
    def test_claude_schema_reaches_gemini_config(self):
        schema = {"type": "object", "properties": {"answer": {"type": "string"}}, "required": ["answer"]}
        body = {"max_tokens": 100, "messages": [{"role": "user", "content": "hello"}],
                "output_config": {"format": {"type": "json_schema", "schema": schema}, "effort": "low"}}
        obj = anthropic_to_openai_request(body, "gemini-2.5-flash")
        validate_openai_request(obj)
        self.assertEqual(obj.response_format["json_schema"]["schema"], schema)
        config = create_generation_config(obj)
        self.assertEqual(config["response_mime_type"], "application/json")
        self.assertIn("answer", config["response_schema"]["properties"])

    def test_claude_invalid_output_format_fails_early(self):
        for output in ([], {"type": "text"}, {"type": "json_schema", "schema": []}):
            with self.subTest(output=output), self.assertRaises(ClaudeRequestError):
                anthropic_to_openai_request({"max_tokens": 10, "messages": [{"role": "user", "content": "x"}],
                                            "output_config": {"format": output}}, "gemini-2.5-flash")

    def test_cookie_features_lock_mixed_routing_and_disable_fallback(self):
        for kwargs in ({"n": 2}, {"logprobs": True}, {"top_logprobs": 0}, {"response_logprobs": True}):
            with self.subTest(kwargs=kwargs):
                obj = request(**kwargs)
                self.assertTrue(cookie_feature_error(obj, structured_output=True))
                with mock.patch.object(chat_api.app_state, "get_setting", return_value=True):
                    self.assertTrue(chat_api._structured_locks_express(obj))
                    self.assertIsNone(chat_api._other_channel(SimpleNamespace(), "express", obj))
                with self.assertRaises(SchemaValidationError):
                    build_request_body("p", obj.model, obj)

    def test_single_candidate_without_logprobs_is_supported(self):
        self.assertIsNone(cookie_feature_error(request(n=1, logprobs=False)))

    def test_unsupported_model_candidate_count_is_rejected(self):
        for n in (0, -1):
            with self.subTest(n=n), self.assertRaises(ValidationError):
                request(model="gemini-3.8-flash", n=n)
        with self.assertRaises(SchemaValidationError):
            validate_openai_request(request(model="gemini-3.8-flash", n=2))
        validate_openai_request(request(n=2))

    def test_native_cookie_rejects_features_before_upstream(self):
        for gen in ({"candidateCount": 2}, {"responseLogprobs": True}, {"logprobs": 0}):
            with self.subTest(gen=gen), self.assertRaises(SchemaValidationError):
                native_request_to_variables({"contents": [], "generationConfig": gen}, "gemini-2.5-flash", "p")

    def test_cookie_never_concatenates_different_candidates(self):
        for candidates in ([{"index": 0}, {"index": 1}], [{"index": 1}]):
            with self.subTest(candidates=candidates), self.assertRaises(IncompleteResponseError):
                list(extract_events({"results": [{"data": {"candidates": candidates}}]}))

    def test_invalid_tool_description_is_client_error(self):
        for value in ([], {}, 1, False):
            with self.subTest(value=value), self.assertRaises(SchemaValidationError):
                validate_openai_request(request(tools=[{"type": "function", "function": {"name": "lookup", "description": value}}]))

    def test_invalid_schema_metadata_is_rejected_before_sdk(self):
        with self.assertRaises(SchemaValidationError):
            validate_openai_request(request(response_format={"type": "json_schema", "json_schema": {
                "schema": {"type": "object", "description": []}}}))

    def test_invalid_tool_description_returns_http_400_without_upstream_call(self):
        from upstreams import express_sdk
        client = TestClient(main.app)
        self.addCleanup(client.close)
        with mock.patch.object(chat_api.app_state, "get_channel_mode", return_value="api_key"), \
                mock.patch.object(chat_api.app_state, "is_web_proxy_enabled", return_value=False), \
                mock.patch.object(express_sdk, "get_pooled_express_client") as upstream:
            response = client.post("/v1/chat/completions", headers={"Authorization": "Bearer " + config.API_KEY},
                                   json=request(tools=[{"type": "function", "function": {
                                       "name": "lookup", "description": []}}]).model_dump())
            self.assertEqual(response.status_code, 400)
            self.assertEqual(response.json()["error"]["type"], "invalid_request_error")
            upstream.assert_not_called()


class SettingsConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.state = AppState()
        self.state.update_settings({"retry_max": 10, "keepalive_interval": 60})
        patch = mock.patch.object(settings_api, "app_state", self.state)
        patch.start()
        self.addCleanup(patch.stop)
        self.client = TestClient(main.app)
        self.addCleanup(self.client.close)
        token = console_auth.issue_session()
        self.addCleanup(console_auth.revoke_session, token)
        self.client.cookies.set(console_auth.AUTH_COOKIE, token)

    def test_disjoint_updates_merge_and_stale_same_field_is_409(self):
        old = self.state.get_settings()
        self.state.update_settings({"retry_max": 20})
        response = self.client.post("/api/settings", json={"keepalive_interval": 90, "_expected": snapshot(old, ["keepalive_interval"])})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["retry_max"], 20)
        response = self.client.post("/api/settings", json={"retry_max": 15, "_expected": snapshot(old, ["retry_max"])})
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["field"], "retry_max")
        self.assertEqual(self.state.get_setting("retry_max"), 20)

    def test_compare_and_update_is_atomic_between_workers(self):
        baseline = snapshot(self.state.get_settings(), ["retry_max"])
        barrier = Barrier(2)
        def save(value):
            barrier.wait()
            try:
                self.state.update_settings({"retry_max": value}, expected=baseline)
                return "saved"
            except SettingsConflictError:
                return "conflict"
        with ThreadPoolExecutor(max_workers=2) as pool:
            self.assertCountEqual(pool.map(save, (12, 13)), ["saved", "conflict"])

    def test_override_conflict_distinguishes_inherited_and_null(self):
        model = "test-override"
        self.state.clear_model_override(model)
        body = {"default_temperature": None, "_expected": snapshot({}, ["default_temperature"])}
        self.assertEqual(self.client.post("/api/model-overrides/" + model, json=body).status_code, 200)
        body["default_temperature"] = 0.5
        self.assertEqual(self.client.post("/api/model-overrides/" + model, json=body).status_code, 409)
        body = {"_inherit": ["default_temperature"], "_expected": snapshot({"default_temperature": None}, ["default_temperature"])}
        self.assertEqual(self.client.post("/api/model-overrides/" + model, json=body).status_code, 200)
        self.assertNotIn(model, self.state.get_saved_model_overrides())

    def test_malformed_or_partial_preconditions_are_rejected(self):
        for expected in (None, [], {}, {"retry_max": {"present": "true"}}, {"retry_max": {"present": True}}):
            with self.subTest(expected=expected):
                response = self.client.post("/api/settings", json={"retry_max": 11, "_expected": expected})
                self.assertEqual(response.status_code, 400)
        self.assertEqual(self.state.get_setting("retry_max"), 10)

    def test_legacy_settings_clients_remain_compatible(self):
        self.assertEqual(self.client.post("/api/settings", json={"retry_max": 11}).status_code, 200)


class PoolRetirementTests(unittest.IsolatedAsyncioTestCase):
    async def test_idle_key_churn_recycles_beyond_previous_failure_threshold(self):
        closer = client_pool._GracefulCloser()
        pool = client_pool.HttpxClientPool(maxsize=6)
        made = []
        def factory():
            client = SimpleNamespace(aclose=mock.AsyncMock())
            made.append(client)
            return client
        with mock.patch.object(client_pool, "_closer", closer), mock.patch.object(client_pool, "_direct_signature", side_effect=lambda uri: (uri,)):
            for i in range(400):
                pool.get(factory, str(i))
                await asyncio.sleep(0)
            self.assertLessEqual(len(closer._pending), 256)
            self.assertTrue(any(c.aclose.await_count for c in made))
            await pool.aclose()
            await closer.aclose()
        for client in made:
            client.aclose.assert_awaited_once()

    async def test_active_retired_client_survives_timeout_until_scope_closes(self):
        closer = client_pool._GracefulCloser(max_pending=1)
        pool = client_pool.HttpxClientPool(maxsize=1)
        usage = ProxyUsage()
        token = current_proxy_usage.set(usage)
        try:
            with mock.patch.object(client_pool, "_closer", closer), mock.patch.object(client_pool, "_GRACE_CLOSE_SECONDS", 0.005), mock.patch.object(client_pool, "_direct_signature", side_effect=lambda uri: (uri,)):
                first = pool.get(lambda: SimpleNamespace(aclose=mock.AsyncMock()), "one")
                pool.get(lambda: SimpleNamespace(aclose=mock.AsyncMock()), "two")
                await asyncio.sleep(0.025)
                first.aclose.assert_not_called()
                usage.close()
                await asyncio.sleep(0.025)
                first.aclose.assert_awaited_once()
                await pool.aclose()
                await closer.aclose()
        finally:
            current_proxy_usage.reset(token)
            usage.close()
        self.assertEqual(closer._users, {})

    async def test_cookie_context_lease_can_close_from_another_stream_task(self):
        closer = client_pool._GracefulCloser()
        pool = impersonate._UpstreamClientPool()
        client = SimpleNamespace(aclose=mock.AsyncMock())
        with mock.patch.object(client_pool, "_closer", closer), mock.patch.object(impersonate, "_closer", closer), \
                mock.patch.object(impersonate, "_upstream_client_pool", pool), \
                mock.patch.object(impersonate, "_create_upstream_client", return_value=client):
            context = impersonate.open_upstream_client({})
            self.assertIs(await asyncio.create_task(context.__aenter__()), client)
            self.assertEqual(closer._users[id(client)], 1)
            await asyncio.create_task(context.__aexit__(None, None, None))
            self.assertEqual(closer._users, {})
            await pool.aclose()
            await closer.aclose()
        client.aclose.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()

"""Final audit: malformed wire data, management persistence and cancelled startups."""

import asyncio
import contextvars
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-completion-audit-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx

from models import OpenAIRequest
import proxy_resolver as pr
from response_validation import IncompleteResponseError
from routes import credentials_api
from runtime_state import AppState
from schema_validation import SchemaValidationError
from request_validation import validate_openai_request
from upstreams import cookie_proxy, cookie_response


def wire_response(part=None, *, errors=None):
    result = {"data": {"candidates": [{"content": {"parts": [part or {"text": "hello"}]},
                                       "finishReason": "STOP"}]}}
    if errors is not None:
        result["errors"] = errors
    return {"results": [result]}


class CookieWireAuditTests(unittest.IsolatedAsyncioTestCase):
    async def events(self, payload):
        def respond(_):
            return httpx.Response(200, json=payload)
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            return [event async for event in cookie_proxy._execute_stream_request_generator_once(client, {}, {})]

    async def test_empty_graphql_errors_keep_valid_model_output(self):
        for errors in ([], None):
            payload = wire_response(errors=errors)
            if errors is None:
                payload["results"][0]["errors"] = None
            events = await self.events(payload)
            self.assertEqual(events, [("text", "hello"), ("finish", "STOP")])

    async def test_bad_upstream_shapes_report_502_instead_of_output_or_internal_500(self):
        malformed = [{"results": "invalid"}, {"results": [None]}, {"results": [{"data": [1]}]},
                     {"results": [{"errors": {"message": "bad"}}]},
                     {"results": [{"data": {"candidates": [None]}}]},
                     wire_response({"text": {"unexpected": True}}),
                     wire_response({"functionCall": {"name": "lookup", "args": ["lost argument"]}}),
                     wire_response({"functionCall": {"name": 42}}),
                     wire_response({"inlineData": [1]}),
                     wire_response({"inlineData": {"mimeType": "image/png", "data": [1]}})]
        for payload in malformed:
            events = await self.events(payload)
            self.assertEqual(events[0][0], "fatal_error")
            self.assertEqual(events[0][1].status_code, 502)

    async def test_non_string_error_messages_preserve_retryable_status(self):
        for message in (None, {"reason": "busy"}, ["busy"], 42):
            events = await self.events({"error": {"code": 429, "message": message}})
            self.assertEqual(events[0][0], "retryable_error")
            self.assertEqual(events[0][1].status_code, 429)
            self.assertTrue(str(events[0][1]))

    async def test_deep_complete_response_has_the_same_502_contract_as_streams(self):
        # CPython 3.12's C decoder has a separate recursion budget; 1100 levels
        # can still decode even when sys.getrecursionlimit() is only 1000.
        text = '{"nested":' + '[' * 10000 + '0' + ']' * 10000 + '}'
        with self.assertRaises(IncompleteResponseError) as error:
            _ = [obj async for obj in cookie_response.iter_json_objects(cookie_response.TextSource(text))]
        self.assertEqual(error.exception.code, 502)

    def test_nonfinite_schema_values_are_rejected_before_upstream_preparation(self):
        for value in (float("nan"), float("inf"), -float("inf")):
            request = OpenAIRequest(model="gemini-test", messages=[], tools=[{
                "type": "function", "function": {"name": "lookup", "parameters": {
                    "type": "object", "properties": {"score": {"type": "number", "default": value}}}}}])
            with self.assertRaises(SchemaValidationError):
                validate_openai_request(request)


class CredentialAuditTests(unittest.IsolatedAsyncioTestCase):
    async def test_bad_key_metadata_does_not_reach_persistence(self):
        invalid = [{"key": {"secret": "private-key"}}, {"key": "key", "project_id": ["bad"]},
                   {"key": "key", "note": {}}, {"key": "key", "project_source": 42},
                   *({"key": "key", "detected_at": value}
                     for value in (float("nan"), float("inf"), -1, True, [], "today"))]
        with mock.patch.object(credentials_api.app_state, "set_express_keys") as save:
            for record in invalid:
                response = await credentials_api.save_express_keys(credentials_api.ExpressKeysBody(keys=[record]), None, True)
                self.assertEqual(response.status_code, 400)
                self.assertNotIn("private-key", response.body.decode())
            save.assert_not_called()

    def test_invalid_legacy_timestamp_is_readable_as_zero(self):
        for value in (float("nan"), float("inf"), -1, True, {}, "today"):
            record = AppState._normalize_express_record({"key": "fixture", "detected_at": value})
            self.assertEqual(record["detected_at"], 0)
            json.dumps(record, allow_nan=False)

    async def test_cookie_ids_are_normalized_before_duplicate_validation(self):
        accounts = [{"id": "same", "cookie": "SAPISID=one", "project_id": "project"},
                    {"id": " same ", "cookie": "SAPISID=two", "project_id": "project"}]
        with (mock.patch.object(credentials_api.app_state, "get_cookie_accounts", return_value=[]),
              mock.patch.object(credentials_api.app_state, "set_cookie_accounts") as save):
            response = await credentials_api.save_cookie_accounts(credentials_api.CookieAccountsBody(accounts=accounts), True)
            self.assertEqual(response.status_code, 400)
            save.assert_not_called()

    async def test_bad_cookie_metadata_does_not_reach_persistence(self):
        with (mock.patch.object(credentials_api.app_state, "get_cookie_accounts", return_value=[]),
              mock.patch.object(credentials_api.app_state, "set_cookie_accounts") as save):
            for field in ("project_id", "note"):
                record = {"cookie": "SAPISID=fixture", "project_id": "project", field: {"unexpected": 1}}
                response = await credentials_api.save_cookie_accounts(credentials_api.CookieAccountsBody(accounts=[record]), True)
                self.assertEqual(response.status_code, 400)
            save.assert_not_called()


class CancelledStartupTests(unittest.IsolatedAsyncioTestCase):
    uri = "sstp://user:password@vpn.example:443"

    def test_cancel_at_readiness_does_not_cache_an_unused_process(self):
        usage, manager, instance = pr.ProxyUsage(), pr.ProxyBridgeManager(), mock.Mock()

        def launch(*_):
            usage.close()
            return instance

        token = pr.current_proxy_usage.set(usage)
        try:
            with (mock.patch.object(manager, "_launch", side_effect=launch),
                  mock.patch.object(manager, "_terminate") as terminate):
                with self.assertRaises(pr.ProxyResolveError):
                    manager.get_local_socks5(self.uri)
                terminate.assert_called_once_with(instance)
                self.assertFalse(manager._instances)
        finally:
            usage.close()
            pr.current_proxy_usage.reset(token)

    async def test_cancelled_generation_stops_its_in_progress_bridge(self):
        usage, manager, launched = pr.ProxyUsage(), pr.ProxyBridgeManager(), threading.Event()
        process = mock.Mock()
        process.poll.return_value = None
        paths = []

        def launch(args, **_):
            paths.append(Path(args[-1]))
            launched.set()
            return process

        token = pr.current_proxy_usage.set(usage)
        try:
            context = contextvars.copy_context()
            with (mock.patch.object(pr, "find_bridge_binary", return_value="/fake/bridge"),
                  mock.patch.object(pr.subprocess, "Popen", side_effect=launch),
                  mock.patch.object(manager, "_terminate") as terminate,
                  ThreadPoolExecutor(max_workers=1) as pool):
                future = pool.submit(context.run, manager.get_local_socks5, self.uri)
                self.assertTrue(await asyncio.to_thread(launched.wait, 1))
                usage.close()
                with self.assertRaises(pr.ProxyResolveError):
                    await asyncio.wait_for(asyncio.wrap_future(future), 2)
                terminate.assert_called_once()
                self.assertFalse(manager._instances)
                self.assertTrue(paths and all(not path.exists() for path in paths))
        finally:
            usage.close()
            pr.current_proxy_usage.reset(token)

    async def test_cancelled_request_does_not_prevent_another_generation_using_the_node(self):
        first, second, manager = pr.ProxyUsage(), pr.ProxyUsage(), pr.ProxyBridgeManager()
        first.include(manager, self.uri)
        second.include(manager, self.uri)
        token = pr.current_proxy_usage.set(first)
        try:
            first.close()
            self.assertFalse(manager._startup_cancelled(self.uri))
            second.close()
            self.assertTrue(manager._startup_cancelled(self.uri))
            with mock.patch.object(manager, "_launch") as launch:
                with self.assertRaises(pr.ProxyResolveError):
                    manager.get_local_socks5(self.uri)
                launch.assert_not_called()
        finally:
            first.close()
            second.close()
            pr.current_proxy_usage.reset(token)


if __name__ == "__main__":
    unittest.main()

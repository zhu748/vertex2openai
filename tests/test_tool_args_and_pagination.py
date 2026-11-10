"""Page/lock semantics, tool Unicode/oneof defaults and pinned image hops."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stdout
import io
import os
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-hot-path-tests-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx
from google.genai import types

import image_processing
import request_tracking
from express_key_manager import ExpressKeyManager
from response_validation import IncompleteResponseError
from tool_stream import _Arguments


class ToolArgumentsTests(unittest.TestCase):
    def test_graphql_null_oneof_defaults_do_not_conflict_with_real_value(self):
        for field, value in (("stringValue", "text"), ("numberValue", 0), ("boolValue", False), ("nullValue", "NULL_VALUE")):
            with self.subTest(field=field):
                fragment = dict(jsonPath="$.value", stringValue=None, numberValue=None, boolValue=None,
                                nullValue=None, willContinue=False)
                fragment[field] = value
                arguments = _Arguments()
                arguments.add(types.PartialArg.model_validate(fragment))
                self.assertEqual(arguments.finish(), {"value": None if field == "nullValue" else value})

    def test_two_actual_oneof_values_still_fail(self):
        for fields in ({"string_value": "x", "null_value": "NULL_VALUE"},
                       {"string_value": "x", "number_value": 1}):
            with self.subTest(fields=fields), self.assertRaises(IncompleteResponseError):
                _Arguments().add(types.PartialArg(json_path="$.x", **fields))

    def test_nested_paths_utf16_pairs_and_ordinary_unicode_are_lossless(self):
        arguments = _Arguments()
        for path, value, continuing in (("$.rows[0].text", "中文\\\"\n😺", False),
                                        ("$['\\ud83d\\ude3a']", "\ud83d", True),
                                        ("$['\\ud83d\\ude3a']", "\ude3a", False),
                                        ("$.rows[1].text", "second", False)):
            arguments.add(types.PartialArg(json_path=path, string_value=value, will_continue=continuing))
        self.assertEqual(arguments.finish(), {"rows": [{"text": "中文\\\"\n😺"}, {"text": "second"}], "😺": "😺"})

    def test_unpaired_surrogates_nonfinite_numbers_and_bad_paths_still_fail(self):
        for field, value in (("string_value", "\ud800"), ("number_value", float("nan")), ("number_value", float("inf"))):
            arguments = _Arguments()
            arguments.add(types.PartialArg(json_path="$.x", **{field: value}))
            with self.subTest(field=field, value=repr(value)), self.assertRaises(IncompleteResponseError):
                arguments.finish()
        arguments = _Arguments()
        arguments.add(types.PartialArg(json_path="$.x", string_value="valid"))
        with self.assertRaises(IncompleteResponseError):
            arguments.add(types.PartialArg(string_value="missing path"))


class MonitorPaginationTests(unittest.TestCase):
    def fixture(self):
        registry = request_tracking.RequestRegistry()
        with redirect_stdout(io.StringIO()):
            completed = [registry.create("/chat") for _ in range(8)]
            for i, trace in enumerate(completed):
                trace.configure(model="Match" if i % 2 == 0 else "other")
                trace.end_connection()
            active = registry.create("/chat")
            active.configure(model="MATCH", generation_id="rid-filter")
            active.start_generation()
            active.end_connection(disconnected=True)
        return registry, completed, active

    def test_filtering_totals_active_first_order_and_summary_are_preserved(self):
        registry, completed, active = self.fixture()
        page = registry.list(query="match", offset=1, limit=2)
        self.assertEqual(page["total"], 5)
        self.assertEqual([row["id"] for row in page["items"]], [completed[6].id, completed[4].id])
        self.assertEqual(page["summary"], {"connections": 0, "generations": 1, "background": 1, "retry_wait": 0, "active": 1})
        self.assertEqual(registry.list(status="completed", query="match")["total"], 4)
        self.assertEqual(registry.list(query="RID-FILTER")["items"][0]["id"], active.id)
        self.assertEqual(registry.list(status="active")["items"][0]["id"], active.id)

    def test_slow_page_snapshot_does_not_block_new_request_registration(self):
        registry, _, active = self.fixture()
        entered, release = threading.Event(), threading.Event()
        original = active.snapshot
        def blocked_snapshot(*args, **kwargs):
            entered.set()
            release.wait(2)
            return original(*args, **kwargs)
        with mock.patch.object(active, "snapshot", blocked_snapshot), ThreadPoolExecutor(max_workers=2) as pool:
            listing = pool.submit(registry.list, limit=1)
            try:
                self.assertTrue(entered.wait(1))
                with redirect_stdout(io.StringIO()):
                    created = pool.submit(registry.create, "/new").result(timeout=1)
                self.assertIsNotNone(created)
            finally:
                release.set()
            self.assertEqual(listing.result(timeout=1)["items"][0]["id"], active.id)


class ImageHopTests(unittest.TestCase):
    def test_each_redirect_resolves_once_and_connects_only_to_checked_address(self):
        requests = []
        def handle(request):
            requests.append(request)
            if len(requests) == 1:
                return httpx.Response(302, headers={"location": "https://two.example/next.png"})
            return httpx.Response(200, content=b"png", headers={"content-type": "image/png"})
        client = httpx.Client(transport=httpx.MockTransport(handle))
        with mock.patch.object(image_processing, "_public_image_addresses", side_effect=[["93.184.216.34"], ["8.8.8.8"]]) as resolver, \
             mock.patch.object(image_processing.httpx, "Client", return_value=client):
            self.assertEqual(image_processing._fetch_remote_image_once("https://one.example/image.png", 5, ""), (b"png", "image/png"))
        self.assertEqual([call.args[0] for call in resolver.call_args_list], ["one.example", "two.example"])
        self.assertEqual([request.url.host for request in requests], ["93.184.216.34", "8.8.8.8"])
        self.assertEqual([request.headers["host"] for request in requests], ["one.example", "two.example"])


class KeyChoiceTests(unittest.TestCase):
    def test_random_selection_keeps_exclusions_and_only_key_fallback(self):
        manager = ExpressKeyManager()
        records = [{"key": "a"}, {"key": "b"}, {"key": "c"}]
        with mock.patch.object(manager, "get_records", return_value=records), \
             mock.patch("express_key_manager._sysrand.choice", side_effect=lambda candidates: candidates[-1]), \
             redirect_stdout(io.StringIO()):
            self.assertEqual(manager.get_random_express_record(exclude_key="c", exclude_keys={"a"})["key"], "b")
            self.assertEqual(manager.get_random_express_record(exclude_key="c", exclude_keys={"a", "b"})["key"], "c")
            self.assertIsNone(manager.get_random_express_record(exclude_keys={"a", "b", "c"}))
        self.assertEqual(records, [{"key": "a"}, {"key": "b"}, {"key": "c"}])


if __name__ == "__main__":
    unittest.main()

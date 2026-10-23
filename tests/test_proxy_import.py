"""Partial imports and bounded/cancellable probes, without external connections."""

import asyncio
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-import-test-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from fastapi.responses import JSONResponse

from proxy_config import ProxyFormatError, parse_proxy_list
from proxy_import import preview_proxy_import
import proxy_resolver as pr
from routes import system_api


class ImportPreviewTests(unittest.TestCase):
    def test_bad_middle_row_does_not_discard_good_rows_and_keeps_line_numbers(self):
        text = "\nhttp://user:secret@one.example:80\n\nsocks5://two.example:0\n192.0.2.3:1080"
        rows = preview_proxy_import(text)["items"]
        self.assertEqual([r["status"] for r in rows], ["ready", "invalid", "ready"])
        self.assertEqual([r["source"] for r in rows], ["第 2 行", "第 4 行", "第 5 行"])
        self.assertNotIn("secret", rows[0]["label"])
        with self.assertRaises(ProxyFormatError):
            parse_proxy_list(text)  # Persistence must remain strict.

    def test_candidates_can_exceed_saved_limit_but_are_bounded(self):
        text = "\n".join(f"http://node-{i}.example:8080" for i in range(200))
        self.assertEqual(len(preview_proxy_import(text)["items"]), 200)
        with self.assertRaisesRegex(ProxyFormatError, "200"):
            preview_proxy_import(text + "\nhttp://extra.example:8080")
        with self.assertRaisesRegex(ProxyFormatError, "256"):
            preview_proxy_import("汉" * 100000)

    def test_duplicate_names_are_ignored_but_credentials_are_not(self):
        existing = ["socks5://u:p@host:1080#old"]
        text = "socks5://u:p@host:1080#new\nsocks5://u:other@host:1080\nsocks5://u:other@host:1080#copy"
        rows = preview_proxy_import(text, existing=existing)["items"]
        self.assertEqual([r["status"] for r in rows], ["duplicate", "ready", "duplicate"])
        self.assertEqual(rows[0]["message"], "当前列表已存在")
        self.assertEqual(existing, ["socks5://u:p@host:1080#old"])

    def test_ice_expansion_keeps_siblings_and_marks_stun_as_skipped(self):
        text = json.dumps({"iceServers": [{"urls": ["turn:one.example", "stun:two.example",
                                                   "turn:bad.example?transport=udp", "turns:last.example"],
                                          "username": "u", "credential": "secret"}]})
        rows = preview_proxy_import(text)["items"]
        self.assertEqual([r["status"] for r in rows], ["ready", "skipped", "invalid", "ready"])
        self.assertIn("urls[4]", rows[-1]["source"])
        self.assertTrue(all("secret" not in row["label"] for row in rows))

    def test_json_array_errors_selected_protocol_and_direct(self):
        rows = preview_proxy_import(json.dumps(["relay:3478", 42, "direct", "sstp://vpn:443"]),
                                    default_scheme="turn")["items"]
        self.assertEqual([r["status"] for r in rows], ["ready", "invalid", "skipped", "invalid"])
        self.assertEqual(rows[0]["type"], "turn")
        with self.assertRaises(ProxyFormatError):
            preview_proxy_import('{"host":"one","host":"two"}')

    def test_line_based_json_can_be_mixed_with_urls(self):
        node = json.dumps({"type": "http", "host": "node.example", "port": 8080})
        rows = preview_proxy_import(node + "\nhttp://second.example:8080\nsocks5://bad:0")["items"]
        self.assertEqual([row["status"] for row in rows], ["ready", "ready", "invalid"])


class ProbeLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_server_limits_concurrency_and_releases_slots_on_error(self):
        entered, finish = asyncio.Event(), asyncio.Event()
        running = 0

        async def probe(_):
            nonlocal running
            running += 1
            if running == 4:
                entered.set()
            await finish.wait()
            return JSONResponse({"ok": True})

        with mock.patch.object(system_api, "_probe_slots", threading.BoundedSemaphore(4)), mock.patch.object(system_api, "_test_proxy_candidate", side_effect=probe):
            tasks = [asyncio.create_task(system_api.test_proxy(system_api.ProxyTestBody(proxy_url="direct"), True)) for _ in range(4)]
            await asyncio.wait_for(entered.wait(), 1)
            busy = await system_api.test_proxy(system_api.ProxyTestBody(), True)
            self.assertEqual(busy.status_code, 429)
            finish.set()
            await asyncio.gather(*tasks)
            self.assertEqual((await system_api.test_proxy(system_api.ProxyTestBody(), True)).status_code, 200)
        with mock.patch.object(system_api, "_probe_slots", threading.BoundedSemaphore(1)) as slots, mock.patch.object(system_api, "_test_proxy_candidate", side_effect=RuntimeError("fixture")):
            with self.assertRaises(RuntimeError):
                await system_api.test_proxy(system_api.ProxyTestBody(), True)
            self.assertTrue(slots.acquire(blocking=False))

    async def test_client_disconnect_cancels_the_actual_probe(self):
        cancelled = asyncio.Event()

        async def probe(_):
            try:
                await asyncio.sleep(10)
            finally:
                cancelled.set()

        request = SimpleNamespace(is_disconnected=mock.AsyncMock(return_value=True))
        with mock.patch.object(system_api, "_test_proxy_candidate", side_effect=probe):
            response = await system_api.test_proxy(system_api.ProxyTestBody(), True, request)
        self.assertEqual(response.status_code, 499)
        self.assertTrue(cancelled.is_set())
        self.assertIsNone(pr.current_proxy_usage.get())

    async def test_external_cancellation_releases_admission(self):
        entered = asyncio.Event()

        async def probe(_):
            entered.set()
            await asyncio.sleep(10)

        with mock.patch.object(system_api, "_probe_slots", threading.BoundedSemaphore(1)) as slots, mock.patch.object(system_api, "_test_proxy_candidate", side_effect=probe):
            task = asyncio.create_task(system_api.test_proxy(system_api.ProxyTestBody(), True))
            await asyncio.wait_for(entered.wait(), 1)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertTrue(slots.acquire(blocking=False))

    async def test_total_deadline_cancels_conversion_or_network_work(self):
        async def slow(_):
            await asyncio.sleep(10)

        with mock.patch.object(system_api, "PROXY_TEST_TOTAL_SECONDS", .01), mock.patch.object(system_api, "_test_proxy_candidate", side_effect=slow):
            result = await system_api.test_proxy(system_api.ProxyTestBody(), True)
        self.assertEqual(json.loads(result.body)["code"], "timeout")

    async def test_cancelled_startup_does_not_retain_probe_only_credentials(self):
        manager = pr.SingboxManager()
        usage = pr.ProxyUsage(temporary=True)
        uri = "socks4://user:password@host:1080"
        usage.include(manager, uri)
        self.assertIn(uri, manager._probe_only)
        usage.close()
        self.assertFalse(manager._probe_only)
        self.assertFalse(manager._active_uses)

    async def test_temporary_listener_waits_for_last_probe_and_is_adopted_by_traffic(self):
        manager = pr.SingboxManager()
        uri = "socks4://host:1080"
        first, second = pr.ProxyUsage(temporary=True), pr.ProxyUsage(temporary=True)
        first.include(manager, uri)
        listener = SimpleNamespace()
        manager._instances[uri] = listener
        second.include(manager, uri)
        with mock.patch.object(manager, "_terminate") as terminate:
            first.close()
            terminate.assert_not_called()
            second.close()
            terminate.assert_called_once_with(listener)
        self.assertFalse(manager._instances)
        probe, traffic = pr.ProxyUsage(temporary=True), pr.ProxyUsage()
        probe.include(manager, uri)
        manager._instances[uri] = listener
        traffic.include(manager, uri)
        with mock.patch.object(manager, "_terminate") as terminate:
            traffic.close()
            probe.close()
            terminate.assert_not_called()
        self.assertIn(uri, manager._instances)

    async def test_preview_endpoint_does_not_write_configuration(self):
        before = system_api.app_state.get_settings()
        result = await system_api.preview_import(system_api.ProxyImportBody(text="http://host:8080\nhttp://bad:0"), True)
        self.assertEqual(result.status_code, 200)
        self.assertEqual(system_api.app_state.get_settings(), before)
        self.assertEqual(result.headers["cache-control"], "no-store")


if __name__ == "__main__":
    unittest.main()

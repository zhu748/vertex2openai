"""Credential rotation, bounded probes and diagnostics under adverse inputs."""

import asyncio
from contextlib import asynccontextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import queue
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-comprehensive-")
os.environ["STATE_DIR"] = _STATE.name
os.environ["API_KEY"] = "comprehensive-test-key"
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx

import console_auth
import cookie_probe
from cookie_auth import validate_cookie
from credential_manager import CredentialManager, COOKIE_AUTH_FAILURE_THRESHOLD
import impersonate
import main
import logger
import request_tracking as tracking
from proxy_resolver import _SingboxInstance
from runtime_state import app_state
from settings_validation import validate_global_settings_patch


def account(cookie="SAPISID=old; SID=one", **values):
    return {"id": "a", "cookie": cookie, "project_id": "project", "enabled": True, **values}


def graphql(*codes):
    return {"results": [{"errors": [
        {"message": f"grpc {code}", "extensions": {"status": {"code": code}}}
        for code in codes
    ]}]}


class HealthTests(unittest.TestCase):
    def setUp(self):
        self.manager = CredentialManager()
        self.old = account()
        self.new = account("SAPISID=new; SID=two")

    def quarantine(self, record):
        for _ in range(COOKIE_AUTH_FAILURE_THRESHOLD):
            self.manager.record_cookie_auth_failure("a", credential=record)

    def test_replacing_cookie_immediately_clears_old_cooldown(self):
        with mock.patch.object(app_state, "get_cookie_accounts", return_value=[self.old]):
            self.quarantine(self.old)
            self.assertGreater(self.manager.cookie_health_snapshot()["a"]["cooldown_remaining"], 0)
        with mock.patch.object(app_state, "get_cookie_accounts", return_value=[self.new, account(id="b")]):
            self.assertEqual(self.manager.cookie_health_snapshot(), {})
            with mock.patch.object(app_state, "get_setting", return_value=True):
                self.assertEqual(self.manager.get_cookie_record()["id"], "a")

    def test_changed_project_also_discards_old_health(self):
        with mock.patch.object(app_state, "get_cookie_accounts", return_value=[self.old]):
            self.quarantine(self.old)
        with mock.patch.object(app_state, "get_cookie_accounts", return_value=[account(project_id="other")]):
            self.assertEqual(self.manager.cookie_health_snapshot(), {})

    def test_late_failure_from_replaced_cookie_is_ignored(self):
        with mock.patch.object(app_state, "get_cookie_accounts", return_value=[self.new]):
            self.quarantine(self.old)
            self.assertEqual(self.manager.cookie_health_snapshot(), {})

    def test_late_success_cannot_clear_new_credentials_cooldown(self):
        with mock.patch.object(app_state, "get_cookie_accounts", return_value=[self.new]):
            self.quarantine(self.new)
            self.manager.record_cookie_success("a", credential=self.old)
            self.assertGreater(self.manager.cookie_health_snapshot()["a"]["cooldown_remaining"], 0)

    def test_deleted_accounts_are_removed_from_health(self):
        with mock.patch.object(app_state, "get_cookie_accounts", return_value=[self.old]):
            self.quarantine(self.old)
        with mock.patch.object(app_state, "get_cookie_accounts", return_value=[]):
            self.assertEqual(self.manager.cookie_health_snapshot(), {})

    def test_health_capacity_is_bounded(self):
        with mock.patch("credential_manager.MAX_COOKIE_HEALTH_ENTRIES", 3):
            for i in range(20):
                self.manager.record_cookie_auth_failure(str(i))
        self.assertEqual(len(self.manager._cookie_health), 3)

    def test_mixed_selection_reconciles_changed_credentials(self):
        with mock.patch.object(app_state, "get_cookie_accounts", return_value=[self.old]):
            self.quarantine(self.old)
        with mock.patch.object(app_state, "get_cookie_accounts", return_value=[self.new]):
            selected = self.manager.get_mixed_record(SimpleNamespace(get_records=lambda: []))
            self.assertEqual(selected["cookie"], self.new["cookie"])


class ProbeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # The mock transport is httpx; curl-only header suppression markers
        # must be disabled just as they are in the real httpx profile.
        profile = mock.patch.object(impersonate, "get_impersonate_profile", return_value="off")
        profile.start()
        self.addCleanup(profile.stop)
        self.semaphore = mock.patch.object(cookie_probe, "_probe_semaphore", asyncio.Semaphore(4))
        self.semaphore.start()
        self.addCleanup(self.semaphore.stop)
        self.health = mock.patch.object(cookie_probe, "credential_manager")
        self.manager = self.health.start()
        self.addCleanup(self.health.stop)

    async def run_probe(self, handler):
        @asynccontextmanager
        async def open_client(options):
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                yield client
        with mock.patch.object(cookie_probe, "open_upstream_client", open_client):
            return await cookie_probe.probe_account(account())

    async def test_expected_grpc_codes_validate_session(self):
        for code in (3, 5, 8):
            result = await self.run_probe(lambda req: httpx.Response(200, json=graphql(code)))
            self.assertEqual(result["status"], "valid")

    async def test_auth_error_wins_over_later_not_found(self):
        for code in (7, 16):
            result = await self.run_probe(lambda req: httpx.Response(200, json=graphql(code, 5)))
            self.assertEqual(result["status"], "invalid")

    async def test_empty_html_malformed_and_unknown_responses_are_not_successes(self):
        for body in (b"", b"<html>login</html>", b"{}", b'{"results":[]}', b'{"results":[{}]}', b'{"results":[',
                     b'{"results":[null]}', json.dumps(graphql(13)).encode()):
            result = await self.run_probe(lambda req: httpx.Response(200, content=body))
            self.assertEqual(result["status"], "error", body)
        self.manager.record_cookie_success.assert_not_called()
        self.manager.record_cookie_auth_failure.assert_not_called()

    async def test_xssi_and_multiple_objects_are_parsed(self):
        body = ")]}'\n[" + json.dumps(graphql(5)) + "," + json.dumps(graphql()) + "]"
        result = await self.run_probe(lambda req: httpx.Response(200, text=body))
        self.assertEqual(result["status"], "valid")

    async def test_http_server_error_does_not_validate_or_quarantine(self):
        result = await self.run_probe(lambda req: httpx.Response(503, json=graphql(5)))
        self.assertEqual(result["status"], "error")
        self.manager.record_cookie_success.assert_not_called()
        self.manager.record_cookie_auth_failure.assert_not_called()

    async def test_auth_rejection_never_downloads_body(self):
        class Stream(httpx.AsyncByteStream):
            async def __aiter__(self):
                raise AssertionError("auth error body must not be read")
                yield b""
        result = await self.run_probe(lambda req: httpx.Response(401, stream=Stream()))
        self.assertEqual(result["status"], "invalid")

    async def test_unknown_length_response_is_bounded_and_closed(self):
        class Stream(httpx.AsyncByteStream):
            count = 0
            closed = False
            async def __aiter__(self):
                for _ in range(100):
                    self.count += 1
                    yield b"x" * (64 * 1024)
            async def aclose(self):
                self.closed = True
        stream = Stream()
        result = await self.run_probe(lambda req: httpx.Response(200, stream=stream))
        self.assertEqual(result["status"], "error")
        self.assertLessEqual(stream.count, 5)
        self.assertTrue(stream.closed)

    async def test_total_timeout_includes_slow_body(self):
        class Stream(httpx.AsyncByteStream):
            closed = False
            async def __aiter__(self):
                await asyncio.Event().wait()
                yield b""
            async def aclose(self):
                self.closed = True
        stream = Stream()
        with mock.patch.object(cookie_probe, "PROBE_TOTAL_TIMEOUT_SECONDS", 0.02):
            result = await self.run_probe(lambda req: httpx.Response(200, stream=stream))
        self.assertEqual(result["status"], "error")
        self.assertTrue(stream.closed)

    async def test_cancellation_closes_response_and_releases_semaphore(self):
        entered = asyncio.Event()
        class Stream(httpx.AsyncByteStream):
            closed = False
            async def __aiter__(self):
                entered.set()
                await asyncio.Event().wait()
                yield b""
            async def aclose(self):
                self.closed = True
        stream = Stream()
        task = asyncio.create_task(self.run_probe(lambda req: httpx.Response(200, stream=stream)))
        await asyncio.wait_for(entered.wait(), 1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(stream.closed)
        self.assertEqual(cookie_probe._probe_semaphore._value, 4)

    async def test_error_details_redact_credentials(self):
        def handler(req):
            raise httpx.ConnectError("https://user:secret@proxy/?key=private Bearer private-token")
        result = await self.run_probe(handler)
        for secret in ("user:secret", "key=private", "private-token"):
            self.assertNotIn(secret, result["detail"])

    async def test_parallel_batches_share_four_network_slots(self):
        active = peak = 0
        async def handler(req):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.005)
            active -= 1
            return httpx.Response(200, json=graphql(5))
        @asynccontextmanager
        async def open_client(options):
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                yield client
        records = [account(id=str(i)) for i in range(12)]
        with mock.patch.object(cookie_probe, "open_upstream_client", open_client):
            batches = await asyncio.gather(cookie_probe.probe_all(records), cookie_probe.probe_all(records))
        self.assertEqual(peak, 4)
        for results in batches:
            self.assertEqual([r["id"] for r in results], [r["id"] for r in records])
            self.assertTrue(all(r["status"] == "valid" for r in results))

    async def test_batch_isolates_exceptions_and_preserves_order(self):
        async def probe(record):
            if record["id"] == "1":
                raise RuntimeError("isolated failure")
            return {"id": record["id"], "status": "valid"}
        with mock.patch.object(cookie_probe, "probe_account", probe):
            results = await cookie_probe.probe_all([account(id=str(i)) for i in range(30)])
        self.assertEqual([r["id"] for r in results], list(map(str, range(30))))
        self.assertEqual(results[1]["status"], "error")
        self.assertEqual(sum(r["status"] == "valid" for r in results), 29)


class CurlByteTests(unittest.IsolatedAsyncioTestCase):
    async def test_early_exit_cancels_producer_without_closing_shared_session(self):
        producer = asyncio.create_task(asyncio.Event().wait())
        raw = SimpleNamespace(status_code=200, astream_task=producer)
        session = SimpleNamespace(request=mock.AsyncMock(return_value=raw), close=mock.AsyncMock())
        client = impersonate._CurlClientShim(session, 60, 1800)
        async with client.stream("POST", "https://upstream.example"):
            pass
        self.assertTrue(producer.cancelled())
        session.close.assert_not_called()

    async def test_real_curl_early_exit_stops_slow_body_and_pool_remains_usable(self):
        from curl_cffi.requests import AsyncSession

        release = threading.Event()
        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"
            def log_message(self, *args):
                pass
            def do_POST(self):
                self.rfile.read(int(self.headers.get("content-length", "0")))
                self.send_response(200)
                self.send_header("Content-Length", "4")
                self.end_headers()
                self.wfile.write(b"ok")
                self.wfile.flush()
                if self.path == "/slow":
                    release.wait(5)
                try:
                    self.wfile.write(b"!!")
                    self.wfile.flush()
                except OSError:
                    pass
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        self.addCleanup(release.set)
        async with AsyncSession(trust_env=False) as session:
            client = impersonate._CurlClientShim(session, 2, 2)
            url = f"http://127.0.0.1:{server.server_port}"
            # Aclose must return while the server is still withholding its tail.
            async with asyncio.timeout(1):
                async with client.stream("POST", url + "/slow", json={}) as response:
                    self.assertEqual(await anext(response.aiter_bytes()), b"ok")
            self.assertFalse(release.is_set())
            async with asyncio.timeout(1):
                async with client.stream("POST", url + "/fast", json={}) as response:
                    self.assertEqual(await response.aread(), b"ok!!")

    async def test_probe_byte_iterator_stops_at_limit(self):
        async def chunks():
            for _ in range(100):
                yield b"x" * 1024
        raw = SimpleNamespace(status_code=200, headers={}, aiter_content=chunks)
        response = impersonate._CurlRespShim(raw, from_stream=True)
        with self.assertRaises(ValueError):
            await cookie_probe.read_limited_bytes(response, 2048)
        self.assertLessEqual(len(response._buffered), 4096)

    async def test_bytes_then_aread_does_not_consume_curl_twice(self):
        reads = 0
        async def chunks():
            nonlocal reads
            reads += 1
            self.assertEqual(reads, 1)
            yield "你好".encode()
        raw = SimpleNamespace(status_code=200, headers={}, aiter_content=chunks)
        response = impersonate._CurlRespShim(raw, from_stream=True)
        self.assertEqual(await cookie_probe.read_limited_bytes(response, 128), "你好".encode())
        self.assertEqual(await response.aread(), "你好".encode())
        self.assertEqual("".join([text async for text in response.aiter_text()]), "你好")


class DiagnosticTests(unittest.TestCase):
    @staticmethod
    def frame(text="ok"):
        return b'data: ' + json.dumps({"choices": [{"delta": {"content": text}}]}).encode() + b'\n\n'

    def test_large_chunk_of_small_frames_is_fully_counted(self):
        trace = tracking.RequestTrace("/chat")
        observer = tracking.PayloadObserver(trace)
        payload = self.frame() * 100
        with mock.patch.object(tracking, "MAX_INSPECTION_BYTES", 128):
            observer.feed(payload)
        self.assertEqual(trace.data["generated_frames"], 100)
        self.assertEqual(trace.data["content_chars"], 200)
        self.assertEqual(trace.data["generated_bytes"], len(payload))
        self.assertEqual(trace.data["inspection_skipped_bytes"], 0)

    def test_oversized_frame_with_split_delimiter_does_not_hide_next_frame(self):
        for delimiter in (b"\n\n", b"\r\r", b"\r\n\r\n"):
            for split in range(1, len(delimiter)):
                trace = tracking.RequestTrace("/chat")
                observer = tracking.PayloadObserver(trace)
                with mock.patch.object(tracking, "MAX_INSPECTION_BYTES", 128):
                    observer.feed(b"data: " + b"x" * 200 + delimiter[:split])
                    observer.feed(delimiter[split:] + self.frame())
                self.assertEqual(trace.data["content_chars"], 2)
                self.assertLessEqual(len(observer.buffer), 128)

    def test_oversized_remainder_after_delimiter_is_also_bounded(self):
        trace = tracking.RequestTrace("/chat")
        observer = tracking.PayloadObserver(trace)
        with mock.patch.object(tracking, "MAX_INSPECTION_BYTES", 128):
            observer.feed(b"x" * 200 + b"\n\n" + b"x" * 10000)
            self.assertLessEqual(len(observer.buffer), 128)
            observer.feed(b"\n\n" + self.frame())
        self.assertEqual(trace.data["content_chars"], 2)

    def test_nonstream_inspection_budget_is_preserved(self):
        trace = tracking.RequestTrace("/chat")
        observer = tracking.PayloadObserver(trace, sse=False)
        with mock.patch.object(tracking, "MAX_INSPECTION_BYTES", 128):
            observer.feed(b"x" * 10000, final=True)
        self.assertEqual(len(observer.buffer), 0)
        self.assertEqual(trace.data["inspection_skipped_bytes"], 10000)

    def test_log_tail_uses_bounded_read_and_preserves_unicode(self):
        data = ("prefix" * 100000 + "末尾🙂日志" * 100).encode()
        class File(io.BytesIO):
            def read(self, size=-1):
                if size < 0 or size > 160:
                    raise AssertionError("unbounded log read")
                return super().read(size)
        instance = _SingboxInstance("uri", 1, None, "config", "log")
        with mock.patch("builtins.open", return_value=File(data)):
            self.assertEqual(instance.log_tail(40), data.decode()[-40:])
        with mock.patch("builtins.open") as open_file:
            self.assertEqual(instance.log_tail(0), "")
            open_file.assert_not_called()

    def test_stdout_queue_bounds_message_size_as_well_as_item_count(self):
        pending = queue.Queue(maxsize=2)
        with mock.patch.object(logger, "_STDOUT_QUEUE", pending), \
             mock.patch.object(logger, "_ensure_stdout_writer"):
            for _ in range(5):
                logger._enqueue_stdout("x" * 200000, "\n", False)
        self.assertEqual(pending.qsize(), 2)
        while not pending.empty():
            text, _, _ = pending.get_nowait()
            self.assertLess(len(text), logger._MAX_STDOUT_CHARS + 100)
            self.assertIn("原长度 200000", text)


class AuthenticationTests(unittest.IsolatedAsyncioTestCase):
    async def test_login_retry_after_keeps_fractional_final_second(self):
        with mock.patch.dict(console_auth._login_failures, {"test": (3, 100.0)}, clear=True), \
             mock.patch("console_auth.time.time", return_value=101.5):
            self.assertEqual(console_auth.login_retry_after("test"), 1)

    async def test_huge_failure_count_is_clamped_before_exponentiation(self):
        with mock.patch.dict(console_auth._login_failures, {"test": (10**9, 100.0)}, clear=True), \
             mock.patch("console_auth.time.time", return_value=100.0):
            self.assertEqual(console_auth.login_retry_after("test"), 300)

    async def test_login_rate_limit_exposes_retry_after(self):
        with mock.patch("routes.console.login_retry_after", return_value=12):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://test") as client:
                response = await client.post("/api/login", json={"password": "wrong"})
        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.headers["retry-after"], "12")

    async def test_invalid_cookie_header_is_rejected_before_persistence(self):
        for cookie in ("SAPISID=a\r\nx-injected: y", "SAPISID=中文", "SAPISID=a\x00; SID=b"):
            self.assertFalse(validate_cookie(cookie)["valid"])
        self.assertTrue(validate_cookie("SAPISID=a; SID=b")["valid"])

    async def test_cookie_authuser_rejects_unsafe_headers(self):
        for value in ("0\r\nx-injected: y", "中文", "x" * 129):
            error = validate_global_settings_patch({"cookie_authuser": value})
            self.assertEqual(error["field"], "cookie_authuser")
        self.assertIsNone(validate_global_settings_patch({"cookie_authuser": "1"}))


if __name__ == "__main__":
    unittest.main()

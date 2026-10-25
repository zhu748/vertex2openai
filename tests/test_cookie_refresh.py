"""Cookie 短期令牌自动续期：Set-Cookie 合并、定向落盘与设置钳位回归。"""

import asyncio
from contextlib import asynccontextmanager
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

import httpx

_STATE = tempfile.TemporaryDirectory(prefix="v2o-cookie-refresh-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import config  # noqa: F401  (确保 DEFAULT_SETTINGS 先加载)
import settings_validation
import cookie_refresh
import impersonate
from cookie_refresh import (
    DEFAULT_REFRESH_INTERVAL_SECONDS,
    MIN_REFRESH_INTERVAL_SECONDS,
    merge_refreshed_cookies,
    parse_set_cookie_header,
    refresh_interval_seconds,
)
from runtime_state import AppState, app_state


class ParseSetCookieTests(unittest.TestCase):
    def test_extracts_name_and_first_attribute_value(self):
        name, value = parse_set_cookie_header(
            "SIDCC=APD_SKabc; Path=/; Secure; HttpOnly; Expires=Wed, 21 Oct 2026 07:28:00 GMT")
        self.assertEqual(name, "SIDCC")
        self.assertEqual(value, "APD_SKabc")

    def test_malformed_header_returns_empty(self):
        self.assertEqual(parse_set_cookie_header(""), ("", ""))
        self.assertEqual(parse_set_cookie_header("novalue"), ("", ""))


class MergeRefreshedCookiesTests(unittest.TestCase):
    BASE = ("SID=g.a000AAA; HSID=fakefake9; SAPISID=fakefake9; "
            "__Secure-1PSIDTS=sidts-old1; SIDCC=APD_SKold; "
            "__Secure-3PSIDCC=APD_SKold3")

    def test_replaces_existing_fields_in_place_and_preserves_order(self):
        new_cookie, changed = merge_refreshed_cookies(self.BASE, [
            "SIDCC=APD_SKnew; Path=/; Secure",
            "__Secure-1PSIDTS=sidts-new1; Path=/; Secure; HttpOnly",
        ])
        self.assertEqual(changed, ["__Secure-1PSIDTS", "SIDCC"])
        # 原位替换：字段顺序与其余字段不变。
        self.assertEqual(
            new_cookie,
            "SID=g.a000AAA; HSID=fakefake9; SAPISID=fakefake9; "
            "__Secure-1PSIDTS=sidts-new1; SIDCC=APD_SKnew; "
            "__Secure-3PSIDCC=APD_SKold3")
        # 长效凭证绝不被触碰。
        self.assertIn("SAPISID=fakefake9", new_cookie)

    def test_appends_watched_fields_missing_from_cookie(self):
        cookie = "SID=g.a000AAA; SAPISID=fakefake9"
        new_cookie, changed = merge_refreshed_cookies(cookie, [
            "__Secure-3PSIDTS=sidts-brand-new; Secure",
        ])
        self.assertEqual(changed, ["__Secure-3PSIDTS"])
        self.assertTrue(new_cookie.endswith("__Secure-3PSIDTS=sidts-brand-new"))

    def test_ignores_non_whitelist_and_same_value(self):
        new_cookie, changed = merge_refreshed_cookies(self.BASE, [
            "DIVERSION_ID=AXzjpdc; Secure",              # 非白名单
            "SIDCC=APD_SKold; Path=/",                   # 与现值相同
            "NOSUCH=1",
        ])
        self.assertEqual(changed, [])
        self.assertEqual(new_cookie, self.BASE)

    def test_replaces_and_appends_in_the_same_response(self):
        new_cookie, changed = merge_refreshed_cookies("SAPISID=secret; SIDCC=old;", [
            "SIDCC=new; Secure", "__Secure-3PSIDTS=added; Secure"])
        self.assertEqual(new_cookie, "SAPISID=secret; SIDCC=new; __Secure-3PSIDTS=added;")
        self.assertEqual(changed, ["SIDCC", "__Secure-3PSIDTS"])

    def test_no_set_cookie_is_noop(self):
        self.assertEqual(merge_refreshed_cookies(self.BASE, []), (self.BASE, []))
        self.assertEqual(merge_refreshed_cookies("", ["SIDCC=x"]), ("", []))


class UpdateCookieAccountTests(unittest.TestCase):
    def test_targeted_update_touches_only_cookie_field(self):
        app_state.set_cookie_accounts([
            {"id": "acc-a", "cookie": "SAPISID=aaa; SIDCC=old", "project_id": "proj-a", "note": "A"},
            {"id": "acc-b", "cookie": "SAPISID=bbb; SIDCC=old", "project_id": "proj-b"},
        ])
        self.assertTrue(app_state.update_cookie_account_cookie("acc-a", "SAPISID=aaa; SIDCC=new"))
        records = {r["id"]: r for r in app_state.get_cookie_accounts()}
        self.assertEqual(records["acc-a"]["cookie"], "SAPISID=aaa; SIDCC=new")
        self.assertEqual(records["acc-a"]["project_id"], "proj-a")
        self.assertEqual(records["acc-a"]["note"], "A")
        self.assertEqual(records["acc-b"]["cookie"], "SAPISID=bbb; SIDCC=old")

    def test_missing_account_returns_false(self):
        self.assertFalse(app_state.update_cookie_account_cookie("no-such", "SAPISID=xxx"))
        self.assertFalse(app_state.update_cookie_account_cookie("", "SAPISID=xxx"))

    def test_updates_survive_reload(self):
        app_state.set_cookie_accounts([
            {"id": "acc-r", "cookie": "SAPISID=rrr; SIDCC=old", "project_id": "proj-r"}])
        app_state.update_cookie_account_cookie("acc-r", "SAPISID=rrr; SIDCC=new")
        reloaded = {r["id"]: r for r in AppState().get_cookie_accounts()}
        self.assertEqual(reloaded["acc-r"]["cookie"], "SAPISID=rrr; SIDCC=new")

    def test_late_refresh_cannot_replace_a_manually_changed_cookie(self):
        app_state.set_cookie_accounts([
            {"id": "acc-cas", "cookie": "SAPISID=manual; SIDCC=new", "project_id": "p"}])
        with mock.patch.object(app_state, "_save") as save:
            self.assertFalse(app_state.update_cookie_account_cookie(
                "acc-cas", "SAPISID=old; SIDCC=rotated", expected_cookie="SAPISID=old; SIDCC=old"))
        save.assert_not_called()
        self.assertEqual(AppState().get_cookie_accounts()[0]["cookie"], "SAPISID=manual; SIDCC=new")


class RotateAccountTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.record = {"id": "rotate", "cookie": "SAPISID=old; SIDCC=old", "project_id": "p",
                       "proxies": ["http://own.test:80"], "proxy_fallback": True}
        app_state.set_cookie_accounts([self.record])
        app_state.update_settings({"proxy_urls": ["http://global.test:80"]})
        profile = mock.patch.object(impersonate, "get_impersonate_profile", return_value="off")
        profile.start()
        self.addCleanup(profile.stop)

    async def rotate(self, handler):
        @asynccontextmanager
        async def open_client(options):
            self.options = options
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                yield client
        with mock.patch.object(cookie_refresh, "open_upstream_client", open_client):
            return await cookie_refresh.rotate_account(self.record)

    async def test_refresh_uses_account_fallback_chain_and_custom_ca(self):
        with mock.patch("config.SSL_CERT_FILE", "/custom/ca.pem"):
            result = await self.rotate(lambda req: httpx.Response(200, headers=[
                ("set-cookie", "SIDCC=rotated; Secure"),
                ("set-cookie", "__Secure-3PSIDTS=added; Secure")], content=b"[]"))
        self.assertEqual(result["status"], "refreshed")
        self.assertEqual(self.options["_proxy_chain"].candidates,
                         ("http://own.test:80", "http://global.test:80"))
        self.assertEqual(self.options["verify"], "/custom/ca.pem")
        self.assertIn("SIDCC=rotated", AppState().get_cookie_accounts()[0]["cookie"])

    async def test_late_network_result_cannot_overwrite_replacement(self):
        def handler(req):
            app_state.update_cookie_account_cookie("rotate", "SAPISID=manual; SIDCC=manual")
            return httpx.Response(200, headers={"set-cookie": "SIDCC=rotated; Secure"})
        result = await self.rotate(handler)
        self.assertEqual(result["status"], "error")
        self.assertEqual(app_state.get_cookie_accounts()[0]["cookie"], "SAPISID=manual; SIDCC=manual")

    async def test_refresh_keeps_metadata_changed_while_network_request_was_in_flight(self):
        def handler(req):
            app_state.set_cookie_accounts([{**self.record, "note": "edited", "enabled": False,
                                           "project_id": "new-project", "proxy_fallback": False}])
            return httpx.Response(200, headers={"set-cookie": "SIDCC=rotated; Secure"})
        result = await self.rotate(handler)
        self.assertEqual(result["status"], "refreshed")
        stored = AppState().get_cookie_accounts()[0]
        self.assertEqual(stored["note"], "edited")
        self.assertEqual(stored["project_id"], "new-project")
        self.assertFalse(stored["enabled"])
        self.assertFalse(stored["proxy_fallback"])
        self.assertIn("SIDCC=rotated", stored["cookie"])

    async def test_error_status_closes_without_reading_body(self):
        class Stream(httpx.AsyncByteStream):
            closed = False
            async def __aiter__(self):
                raise AssertionError("error body must not be downloaded")
                yield b""
            async def aclose(self):
                self.closed = True
        stream = Stream()
        result = await self.rotate(lambda req: httpx.Response(403, stream=stream))
        self.assertEqual(result["status"], "error")
        self.assertIn("403", result["detail"])
        self.assertTrue(stream.closed)

    async def test_unknown_length_body_is_bounded_and_not_persisted(self):
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
        result = await self.rotate(lambda req: httpx.Response(
            200, headers={"set-cookie": "SIDCC=rotated"}, stream=stream))
        self.assertEqual(result["status"], "error")
        self.assertLessEqual(stream.count, 2)
        self.assertTrue(stream.closed)
        self.assertEqual(app_state.get_cookie_accounts()[0]["cookie"], self.record["cookie"])

    async def test_total_timeout_and_cancellation_close_slow_body(self):
        class Stream(httpx.AsyncByteStream):
            closed = False
            def __init__(self):
                self.entered = asyncio.Event()
            async def __aiter__(self):
                self.entered.set()
                await asyncio.Event().wait()
                yield b""
            async def aclose(self):
                self.closed = True
        stream = Stream()
        with mock.patch.object(cookie_refresh, "ROTATE_TOTAL_TIMEOUT_SECONDS", 0.02):
            result = await self.rotate(lambda req: httpx.Response(200, stream=stream))
        self.assertEqual(result["status"], "error")
        self.assertTrue(stream.closed)
        stream = Stream()
        task = asyncio.create_task(self.rotate(lambda req: httpx.Response(200, stream=stream)))
        await asyncio.wait_for(stream.entered.wait(), 1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(stream.closed)


class IntervalClampTests(unittest.TestCase):
    def test_auto_refresh_defaults_on_and_preserves_explicit_off(self):
        self.assertIs(config.DEFAULT_SETTINGS["cookie_auto_refresh"], True)
        with mock.patch.object(app_state, "_state", {}):
            self.assertTrue(cookie_refresh.auto_refresh_enabled())
        with mock.patch.object(app_state, "_state", {"settings": {"cookie_auto_refresh": False}}):
            self.assertFalse(cookie_refresh.auto_refresh_enabled())

    def test_interval_clamped_to_minimum(self):
        app_state.update_settings({"cookie_refresh_interval_seconds": 10})
        self.assertEqual(refresh_interval_seconds(), float(MIN_REFRESH_INTERVAL_SECONDS))

    def test_interval_default_and_hot_change(self):
        app_state.update_settings({"cookie_refresh_interval_seconds": DEFAULT_REFRESH_INTERVAL_SECONDS})
        self.assertEqual(refresh_interval_seconds(), float(DEFAULT_REFRESH_INTERVAL_SECONDS))
        app_state.update_settings({"cookie_refresh_interval_seconds": 900})
        self.assertEqual(refresh_interval_seconds(), 900.0)

    def test_settings_validation_accepts_new_keys(self):
        self.assertIsNone(settings_validation.validate_global_settings_patch({
            "cookie_auto_refresh": True,
            "cookie_refresh_interval_seconds": 600,
        }))
        self.assertIsNotNone(settings_validation.validate_global_settings_patch({
            "cookie_auto_refresh": "yes",
        }))
        self.assertIsNotNone(settings_validation.validate_global_settings_patch({
            "cookie_refresh_interval_seconds": 30,
        }))


class RefreshLoopTests(unittest.IsolatedAsyncioTestCase):
    async def test_manual_and_automatic_rounds_share_lock_and_release_on_cancel(self):
        started = asyncio.Event()

        async def rotate(record):
            started.set()
            await asyncio.Event().wait()

        with (mock.patch.object(cookie_refresh, "_loop_lock", asyncio.Lock()),
              mock.patch.object(cookie_refresh, "rotate_account", rotate)):
            task = asyncio.create_task(cookie_refresh.refresh_once([{"id": "one"}]))
            try:
                await asyncio.wait_for(started.wait(), 1)
                with self.assertRaises(cookie_refresh.RefreshInProgressError):
                    await cookie_refresh.refresh_once([{"id": "one"}])
            finally:
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
            self.assertEqual(await cookie_refresh.refresh_once([]), [])

    async def test_batch_continues_after_individual_failure(self):
        with (mock.patch.object(cookie_refresh, "_loop_lock", asyncio.Lock()),
              mock.patch.object(cookie_refresh, "_ACCOUNT_GAP_SECONDS", 0),
              mock.patch.object(cookie_refresh, "rotate_account", mock.AsyncMock(side_effect=[
                  RuntimeError("fixture network error"), {"id": "two", "status": "unchanged", "changed": []}]))):
            results = await cookie_refresh.refresh_once([{"id": "one"}, {"id": "two"}])
        self.assertEqual([r["status"] for r in results], ["error", "unchanged"])

    async def test_settings_save_wakes_disabled_loop_without_waiting_old_interval(self):
        wake = asyncio.Event()
        entered = asyncio.Event()
        refreshed = asyncio.Event()
        enabled = False

        def is_enabled():
            entered.set()
            return enabled

        async def refresh(records):
            refreshed.set()

        with (mock.patch.object(cookie_refresh, "_settings_changed", wake),
              mock.patch.object(cookie_refresh, "_STARTUP_DELAY_SECONDS", 0),
              mock.patch.object(cookie_refresh, "auto_refresh_enabled", is_enabled),
              mock.patch.object(cookie_refresh, "refresh_interval_seconds", return_value=86400),
              mock.patch.object(app_state, "get_cookie_accounts", return_value=[{"enabled": True}]),
              mock.patch.object(cookie_refresh, "refresh_once", refresh)):
            task = asyncio.create_task(cookie_refresh.refresh_loop())
            try:
                await asyncio.wait_for(entered.wait(), 1)
                enabled = True
                cookie_refresh.notify_settings_changed()
                await asyncio.wait_for(refreshed.wait(), 1)
            finally:
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task


class ManualRefreshRouteTests(unittest.TestCase):
    def setUp(self):
        from fastapi.testclient import TestClient
        import console_auth
        import main
        from routes import credentials_api

        self.routes = credentials_api
        self.auth = console_auth
        app_state.set_cookie_accounts([
            {"id": "one", "cookie": "SAPISID=secret-one", "project_id": "p", "enabled": True},
            {"id": "off", "cookie": "SAPISID=secret-off", "project_id": "p", "enabled": False},
            {"id": "two", "cookie": "SAPISID=secret-two", "project_id": "p", "enabled": True},
        ])
        self.client = TestClient(main.app)
        self.addCleanup(self.client.close)
        self.client.cookies.set(console_auth.AUTH_COOKIE, console_auth.issue_session())
        self.addCleanup(console_auth.revoke_session, self.client.cookies.get(console_auth.AUTH_COOKIE))

    def test_manual_batch_works_with_auto_off_and_returns_only_results(self):
        app_state.update_settings({"cookie_auto_refresh": False})
        results = [{"id": "one", "label": "one", "status": "refreshed", "changed": ["SIDCC"], "detail": "updated"},
                   {"id": "two", "label": "two", "status": "error", "changed": [], "detail": "offline"}]
        with mock.patch.object(self.routes, "refresh_cookie_accounts", mock.AsyncMock(return_value=results)) as refresh:
            response = self.client.post("/api/cookie-accounts/refresh", json={})
        self.assertEqual(response.status_code, 200)
        self.assertEqual([r["id"] for r in refresh.await_args.args[0]], ["one", "two"])
        self.assertEqual([response.json()[k] for k in ("total", "refreshed", "unchanged", "errors")], [2, 1, 0, 1])
        self.assertNotIn("secret-", response.text)
        self.assertEqual(response.headers["cache-control"], "no-store")
        self.assertFalse(cookie_refresh.auto_refresh_enabled())

    def test_unrelated_runtime_save_does_not_trigger_another_rotation(self):
        app_state.update_settings({"cookie_auto_refresh": False, "cookie_refresh_interval_seconds": 600})
        with mock.patch.object(cookie_refresh, "notify_settings_changed") as notify:
            unchanged = {"cookie_auto_refresh": False, "cookie_refresh_interval_seconds": 600, "retry_max": 3}
            self.assertEqual(self.client.post("/api/settings", json=unchanged).status_code, 200)
            notify.assert_not_called()
            self.assertEqual(self.client.post("/api/settings", json={"cookie_auto_refresh": True}).status_code, 200)
            notify.assert_called_once()
            self.client.post("/api/settings", json={"cookie_auto_refresh": True, "retry_max": 4})
            notify.assert_called_once()
            self.client.post("/api/settings", json={"cookie_refresh_interval_seconds": 900})
            self.assertEqual(notify.call_count, 2)

    def test_selects_single_saved_account_and_rejects_disabled_or_unknown(self):
        with mock.patch.object(self.routes, "refresh_cookie_accounts", mock.AsyncMock(return_value=[])) as refresh:
            response = self.client.post("/api/cookie-accounts/refresh", json={"id": "two"})
            self.assertEqual(response.status_code, 200)
            self.assertEqual([r["id"] for r in refresh.await_args.args[0]], ["two"])
            refresh.reset_mock()
            for account_id in ("off", "missing"):
                self.assertEqual(self.client.post("/api/cookie-accounts/refresh", json={"id": account_id}).status_code, 404)
            refresh.assert_not_awaited()

    def test_requires_session_and_reports_existing_round_without_repeating(self):
        with mock.patch.object(self.routes, "refresh_cookie_accounts", mock.AsyncMock(
                side_effect=cookie_refresh.RefreshInProgressError("Cookie 正在刷新"))) as refresh:
            response = self.client.post("/api/cookie-accounts/refresh", json={})
            self.assertEqual(response.status_code, 409)
            self.assertIn("正在刷新", response.json()["error"])
            refresh.reset_mock()
            self.client.cookies.clear()
            self.assertEqual(self.client.post("/api/cookie-accounts/refresh", json={}).status_code, 401)
            refresh.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()

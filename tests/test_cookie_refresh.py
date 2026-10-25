"""Cookie 短期令牌自动续期：Set-Cookie 合并、定向落盘与设置钳位回归。"""

import os
from pathlib import Path
import sys
import tempfile
import unittest

_STATE = tempfile.TemporaryDirectory(prefix="v2o-cookie-refresh-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import config  # noqa: F401  (确保 DEFAULT_SETTINGS 先加载)
import settings_validation
from cookie_refresh import (
    DEFAULT_REFRESH_INTERVAL_SECONDS,
    MIN_REFRESH_INTERVAL_SECONDS,
    merge_refreshed_cookies,
    parse_set_cookie_header,
    refresh_interval_seconds,
)
from runtime_state import app_state


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
        reloaded = {r["id"]: r for r in app_state.get_cookie_accounts()}
        self.assertEqual(reloaded["acc-r"]["cookie"], "SAPISID=rrr; SIDCC=new")


class IntervalClampTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()

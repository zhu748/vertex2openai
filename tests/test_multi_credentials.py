"""Mixed credentials, environment import, per-account settings and Cookie isolation."""

import asyncio
from contextlib import asynccontextmanager
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

_TEMP = tempfile.TemporaryDirectory(prefix="v2o-multi-credentials-")
os.environ["STATE_DIR"] = _TEMP.name
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app"))

import httpx
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

import config
import credential_manager
from credential_config import parse_cookie_records, parse_key_records
from express_key_manager import ExpressKeyManager
from express_endpoint import resolve_express_model_path
import main
from models import OpenAIRequest
from routes import chat_api, gemini_native, models_api
import runtime_state
from runtime_state import AppState, app_state, get_effective_proxy_uris_for_account
from upstreams import cookie_proxy


class StateFixture:
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(dir=_TEMP.name)
        self.addCleanup(self.directory.cleanup)
        # 快照并回滚全局单例：本文件大量用 set_channel_mode("mixed") /
        # set_express_keys([]) 改写 app_state；只重置不回滚会把 mixed 模式
        # 泄漏给同进程后执行的测试文件（request_tracking / socket_recovery /
        # streaming_optimizations 的 mock upstream 不接受 credential_record
        # 关键字，mixed 路径必炸），整仓全量套件的信号因此不可信。
        self._saved_state = app_state._state
        self._saved_committed = app_state._committed
        self.addCleanup(setattr, app_state, "_state", self._saved_state)
        self.addCleanup(setattr, app_state, "_committed", self._saved_committed)
        for patch in (
            mock.patch.object(runtime_state, "STATE_FILE", str(Path(self.directory.name) / "web_state.json")),
            mock.patch.object(config, "GOOGLE_COOKIE", None),
            mock.patch.object(config, "GOOGLE_COOKIES", None),
            mock.patch.object(config, "ENV_COOKIE_ACCOUNTS", []),
            mock.patch.object(config, "GOOGLE_PROJECT_ID", ""),
            mock.patch.object(config, "UPSTREAM_MODE", "api_key"),
            mock.patch.object(config, "PROXY_URL", None),
            mock.patch.object(config, "PROXY_URLS", None),
        ):
            patch.start()
            self.addCleanup(patch.stop)
        app_state._state = {}
        app_state._committed = {}
        self.manager = credential_manager.CredentialManager()
        for module in (chat_api, cookie_proxy):
            patch = mock.patch.object(module, "credential_manager", self.manager)
            patch.start()
            self.addCleanup(patch.stop)


class EnvironmentTests(StateFixture, unittest.TestCase):
    def test_key_and_cookie_environment_formats(self):
        self.assertEqual([r["key"] for r in parse_key_records("a,b\nc")], ["a", "b", "c"])
        record = {"key": "a", "project_id": "p", "proxies": ["host:1080:user:pass"], "proxy_fallback": False}
        self.assertEqual(parse_key_records(json.dumps([record])), [record])
        cookies = parse_cookie_records("SAPISID=one; SID=a\nSAPISID=two; SID=b", project_ids="p1,p2")
        self.assertEqual([r["project_id"] for r in cookies], ["p1", "p2"])
        self.assertEqual(cookies[0]["cookie"], "SAPISID=one; SID=a")
        with self.assertRaises(ValueError):
            parse_cookie_records("SAPISID=one\nSAPISID=two", project_ids="one")
        editor = '[{"name":"SAPISID","value":"one"},{"name":"SID","value":"two"}]'
        self.assertEqual(parse_cookie_records(editor, "p"), [{"cookie": "SAPISID=one; SID=two", "project_id": "p"}])
        for field in ("name", "key"):
            fields = [{field: "SAPISID", "value": "one"}, {field: "SID", "value": "two"}]
            for value in (fields, json.dumps(fields)):
                with self.subTest(field=field, encoded=isinstance(value, str)):
                    self.assertEqual(parse_cookie_records(value, project_ids="p"),
                                     [{"cookie": "SAPISID=one; SID=two", "project_id": "p"}])

    def test_real_settings_load_plural_environment_and_infer_mixed_mode(self):
        environment = dict(os.environ, PYTHONPATH=str(ROOT / "app"),
            VERTEX_EXPRESS_API_KEY="legacy-key", GOOGLE_COOKIE="SAPISID=legacy",
            VERTEX_EXPRESS_API_KEYS=json.dumps([{"key": "new-key", "project_id": "key-project",
                                               "proxies": ["host:1080:user:pass"], "proxy_fallback": False}]),
            GOOGLE_COOKIES=json.dumps([{"cookie": "SAPISID=c1", "project_id": "p1"},
                                      {"cookie": "SAPISID=c2", "project_id": "p2"}]),
            PROXY_URLS='["socks5://first:1080", "http://second:8080"]')
        environment.pop("UPSTREAM_MODE", None)
        code = "import config; assert config.UPSTREAM_MODE == 'mixed'; assert len(config.ENV_COOKIE_ACCOUNTS) == 2; assert config.ENV_EXPRESS_KEYS[0]['key'] == 'new-key'; assert len(config.INITIAL_PROXY_URLS) == 2"
        result = subprocess.run([sys.executable, "-c", code], env=environment, cwd=self.directory.name,
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_migration_is_persistent_and_deleted_lists_do_not_resurrect(self):
        app_state._state.update(use_web_proxy=True, google_cookie="SAPISID=old", google_project_id="old-project",
                                settings={"proxy_url": "socks5://proxy:1080"})
        app_state.import_env_express_keys_once([{"key": "k1", "project_id": "p1", "proxies": ["host:1080:u:p"]}])
        app_state.import_env_cookie_accounts_once()
        self.assertEqual(app_state.get_channel_mode(), "web_proxy")
        self.assertEqual(app_state.get_cookie_accounts()[0]["project_id"], "old-project")
        self.assertEqual(app_state.get_settings()["proxy_urls"], ["socks5://proxy:1080"])
        app_state.set_cookie_accounts([])
        app_state.set_express_keys([])
        reloaded = AppState()
        with mock.patch.object(config, "ENV_COOKIE_ACCOUNTS", [{"cookie": "SAPISID=new", "project_id": "new"}]):
            self.assertEqual(reloaded.import_env_cookie_accounts_once(), 0)
            self.assertEqual(reloaded.import_env_express_keys_once(["new"]), 0)
        self.assertEqual(reloaded.get_cookie_accounts(), [])
        self.assertEqual(reloaded.get_express_keys(), [])

    def test_initial_channel_mode_is_persisted_and_legacy_choice_is_respected(self):
        with mock.patch.object(config, "UPSTREAM_MODE", "mixed"):
            app_state.initialize_channel_mode_once()
        self.assertEqual(AppState().get_channel_mode(), "mixed")
        app_state._state = {"use_web_proxy": True}
        app_state.initialize_channel_mode_once()
        self.assertEqual(app_state.get_channel_mode(), "web_proxy")

    def test_mixed_express_does_not_borrow_a_cookie_accounts_project(self):
        app_state.set_project_id("cookie-project")
        app_state.set_channel_mode("mixed")
        self.assertEqual(resolve_express_model_path("gemini-test", {"express_location": "global"}), "gemini-test")
        self.assertIn("projects/key-project/", resolve_express_model_path(
            "gemini-test", {"express_location": "global"}, "key-project"))

    def test_account_proxy_priority_and_explicit_empty_globals(self):
        app_state.update_settings({"proxy_urls": ["http://global1:80", "http://global2:80"]})
        record = {"proxies": ["socks5://own1:1080", "socks5://own2:1080"]}
        self.assertEqual(get_effective_proxy_uris_for_account(record), record["proxies"] + ["http://global1:80", "http://global2:80"])
        self.assertEqual(get_effective_proxy_uris_for_account(dict(record, proxy_fallback=False)), record["proxies"])
        self.assertEqual(get_effective_proxy_uris_for_account({"proxy": "direct"}), ["direct"])
        app_state.update_settings({"proxy_urls": []})
        with mock.patch.object(config, "PROXY_URL", "http://env:8080"):
            self.assertEqual(get_effective_proxy_uris_for_account({}), [""])
            self.assertEqual(get_effective_proxy_uris_for_account(record), record["proxies"])

    def test_corrupt_state_file_is_backed_up_before_memory_mode(self):
        """回归：损坏的状态文件先改名备份，降级后的保存不得覆盖原文件。"""
        path = Path(runtime_state.STATE_FILE)
        # 半截 JSON（进程崩溃 / 磁盘写坏）：里面还有可手工修复的 Cookie。
        path.write_text('{"settings": {"a": 1}, "google_cookie": "SAPISID=precious",',
                        encoding="utf-8")
        state = AppState()
        self.assertEqual(state._state, {})          # 降级为内存模式
        backups = list(Path(self.directory.name).glob("web_state.json.corrupt-*"))
        self.assertEqual(len(backups), 1)           # 原文件已改名保留
        self.assertIn("SAPISID=precious", backups[0].read_text(encoding="utf-8"))
        self.assertFalse(path.exists())             # 原位已让位给后续新写入
        # 内存模式下的保存生成全新文件，不再原子覆盖损坏原文件。
        state._state["settings"] = {"fresh": True}
        state._save()
        self.assertEqual(json.loads(path.read_text(encoding="utf-8")), {"settings": {"fresh": True}})
        self.assertIn("SAPISID=precious", backups[0].read_text(encoding="utf-8"))
        # 顶层非 JSON 对象同样按损坏备份处理。
        state2_dir = tempfile.TemporaryDirectory(dir=_TEMP.name)
        self.addCleanup(state2_dir.cleanup)
        with mock.patch.object(runtime_state, "STATE_FILE",
                               str(Path(state2_dir.name) / "web_state.json")):
            Path(state2_dir.name, "web_state.json").write_text('["not", "an", "object"]', encoding="utf-8")
            AppState()
            self.assertEqual(len(list(Path(state2_dir.name).glob("web_state.json.corrupt-*"))), 1)


class ManagementTests(StateFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        for patch in (mock.patch.object(config, "API_KEY", "multi-console"),
                      mock.patch("auth.API_KEY", "multi-console"),
                      mock.patch.object(gemini_native, "API_KEY", "multi-console")):
            patch.start()
            self.addCleanup(patch.stop)
        self.client = TestClient(main.app)
        self.addCleanup(self.client.close)

    def login(self):
        self.assertEqual(self.client.post("/api/login", json={"password": "multi-console"}).status_code, 200)

    def test_new_endpoints_require_authentication(self):
        for method, path in (("GET", "/api/cookie-accounts"), ("POST", "/api/cookie-accounts"),
                             ("POST", "/api/proxy/parse")):
            self.assertEqual(self.client.request(method, path, json={}).status_code, 401)

    def test_cookie_edit_preserves_secret_and_settings_without_echoing_it(self):
        self.login()
        body = {"accounts": [{"cookie": "SAPISID=private-one; SID=private-sid", "project_id": "p1",
                               "proxies": ["socks5://host:1080:user:pass"], "proxy_fallback": False},
                              {"cookie": "__Secure-1PAPISID=private-two", "project_id": "p2"}]}
        response = self.client.post("/api/cookie-accounts", json=body)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertNotIn("private", response.text)
        accounts = response.json()["accounts"]
        accounts[0].update(cookie="", note="renamed", project_id="changed-project")
        response = self.client.post("/api/cookie-accounts", json={"accounts": accounts})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(app_state.get_cookie_accounts()[0]["cookie"], body["accounts"][0]["cookie"])
        self.assertFalse(app_state.get_cookie_accounts()[0]["proxy_fallback"])
        self.assertEqual(app_state.get_cookie_accounts()[0]["proxies"], ["socks5://user:pass@host:1080"])
        for path in ("/api/cookie-accounts", "/api/settings/runtime", "/healthz"):
            self.assertNotIn("private", self.client.get(path).text)
        self.assertEqual(self.client.post("/api/cookie-accounts", json={"accounts": []}).status_code, 200)
        self.assertFalse(app_state.get_google_cookie())

    def test_invalid_account_updates_and_mode_are_rejected_atomically(self):
        self.login()
        app_state.set_cookie_accounts([{"cookie": "SAPISID=original", "project_id": "original"}])
        before = app_state.get_cookie_accounts()
        for account in ({"cookie": "SID=missing-sapisid", "project_id": "p"},
                        {"cookie": "SAPISID=test", "project_id": "p", "proxies": ["socks5://host:0"]},
                        {"cookie": "SAPISID=test", "project_id": "p", "proxy_fallback": "false"}):
            response = self.client.post("/api/cookie-accounts", json={"accounts": [account]})
            self.assertEqual(response.status_code, 400, response.text)
            self.assertEqual(app_state.get_cookie_accounts(), before)
        self.assertEqual(self.client.post("/api/settings/mode", json={"mode": "mixed"}).status_code, 200)
        self.assertEqual(self.client.get("/api/settings/runtime").json()["mode"], "mixed")
        self.assertEqual(self.client.post("/api/settings/mode", json={"mode": "unknown"}).status_code, 422)
        self.assertEqual(app_state.get_channel_mode(), "mixed")

    def test_proxy_parse_and_global_list_save(self):
        self.login()
        response = self.client.post("/api/proxy/parse", json={"text": "proxy.test:8080:user:pass", "type": "http"})
        self.assertEqual(response.json()["proxies"][0]["url"], "http://user:pass@proxy.test:8080")
        response = self.client.post("/api/proxy/parse", json={"type": "socks5", "host": "::1", "port": 1080,
                                                            "username": "u@x", "password": "p:a#s"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["proxies"][0]["url"], "socks5://u%40x:p%3Aa%23s@[::1]:1080")
        response = self.client.post("/api/settings", json={"proxy_urls": ["host:1080:u:p", "http://second:8080"]})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["proxy_url"], "socks5://u:p@host:1080")
        self.assertEqual(AppState().get_settings()["proxy_urls"], ["socks5://u:p@host:1080", "http://second:8080"])

    def test_proxy_test_reports_authentication_and_gateway_errors_as_failures(self):
        self.login()
        for status in (200, 403, 404, 407, 502, 503, 504):
            client = mock.AsyncMock()
            client.get.return_value = httpx.Response(status)
            with self.subTest(status=status), mock.patch("httpx.AsyncClient") as factory:
                factory.return_value.__aenter__.return_value = client
                response = self.client.post("/api/proxy/test", json={"proxy_url": "http://proxy.test:8080"})
            self.assertEqual(response.status_code, 200)
            result = response.json()
            self.assertEqual(result["ok"], status not in (407, 502, 503, 504))
            self.assertEqual(result["status_code"], status)
            if status == 407:
                self.assertIn("账号和密码", result["message"])

    def test_mixed_mode_cookie_only_keeps_model_catalog_available(self):
        app_state.set_express_keys([])
        app_state.set_cookie_accounts([{"cookie": "SAPISID=one", "project_id": "p1"}])
        app_state.set_channel_mode("mixed")
        headers = {"Authorization": "Bearer multi-console", "x-goog-api-key": "multi-console"}
        with mock.patch.object(models_api, "get_express_models", return_value=["gemini-test"]), \
             mock.patch.object(gemini_native, "get_express_models", return_value=["gemini-test"]):
            response = self.client.get("/v1/models", headers=headers)
            self.assertEqual(response.status_code, 200)
            self.assertIn("gemini-test", [r["id"] for r in response.json()["data"]])
            response = self.client.get("/v1beta/models", headers=headers)
            self.assertEqual(response.json()["models"][0]["name"], "models/gemini-test")
            self.assertEqual(self.client.get("/v1beta/models/gemini-test", headers=headers).status_code, 200)


class RoutingTests(StateFixture, unittest.IsolatedAsyncioTestCase):
    async def test_mixed_roundrobin_uses_all_enabled_credentials_and_structured_output_uses_keys(self):
        app_state.set_express_keys([{"key": "k1", "project_id": "kp1"}, {"key": "k2", "project_id": "kp2"},
                                    {"key": "disabled", "enabled": False}])
        app_state.set_cookie_accounts([{"cookie": "SAPISID=c1", "project_id": "cp1"},
                                       {"cookie": "SAPISID=c2", "project_id": "cp2"},
                                       {"cookie": "SAPISID=disabled", "project_id": "no", "enabled": False}])
        app_state.set_channel_mode("mixed")
        app_state.update_settings({"roundrobin": True})
        called = []
        async def response(obj, request, *, credential_record):
            called.append(credential_record)
            return JSONResponse({"ok": True})
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(express_key_manager=ExpressKeyManager())),
                                  is_disconnected=mock.AsyncMock(return_value=False))
        with mock.patch.object(chat_api.express_upstream, "chat_completions", side_effect=response), \
             mock.patch.object(chat_api.cookie_upstream, "chat_completions", side_effect=response):
            for _ in range(4):
                await chat_api._call_upstream(OpenAIRequest(model="gemini-test", messages=[]), request)
            self.assertEqual([r["project_id"] for r in called], ["kp1", "kp2", "cp1", "cp2"])
            called.clear()
            for _ in range(3):
                await chat_api._call_upstream(OpenAIRequest(model="gemini-test", messages=[],
                                                            response_format={"type": "json_object"}), request)
            self.assertTrue(all(r["channel"] == "express" for r in called))

    async def test_cookie_project_headers_and_proxy_chain_do_not_cross_between_concurrent_requests(self):
        app_state.set_cookie_accounts([{"cookie": "SAPISID=one; SID=s1", "project_id": "p1", "proxies": ["http://one:80"]},
                                       {"cookie": "SAPISID=two; SID=s2", "project_id": "p2", "proxies": ["http://two:80"]}])
        app_state.update_settings({"roundrobin": True, "prefill_mode": "off", "retry_max": 0,
                                   "first_reply_timeout_seconds": 0, "cookie_impersonate": "off"})
        calls = []
        @asynccontextmanager
        async def open_client(options):
            async def post(url, *, headers, json):
                await asyncio.sleep(0)
                calls.append((headers["cookie"], json["requestContext"]["projectId"], options["_proxy_chain"].current))
                return httpx.Response(200, json={"results": [{"data": {"candidates": [{"content": {"parts": [{"text": "answer"}]}, "finishReason": "STOP"}]}}]})
            yield SimpleNamespace(post=post)
        request = SimpleNamespace(is_disconnected=mock.AsyncMock(return_value=False))
        with mock.patch.object(cookie_proxy, "open_upstream_client", open_client):
            results = await asyncio.gather(*(cookie_proxy.CookieProxyUpstream().chat_completions(
                OpenAIRequest(model="gemini-3.6-flash", messages=[]), request) for _ in range(2)))
        self.assertTrue(all(r.status_code == 200 for r in results))
        self.assertEqual(set(calls), {("SAPISID=one; SID=s1", "p1", "http://one:80"),
                                      ("SAPISID=two; SID=s2", "p2", "http://two:80")})


if __name__ == "__main__":
    unittest.main()

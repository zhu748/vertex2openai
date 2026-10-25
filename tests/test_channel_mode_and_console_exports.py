# -*- coding: utf-8 -*-
"""两处用户报告缺陷的回归测试。

Bug A（Gemini 原生格式通道选择无视模式）：
  旧版 _passthrough_endpoint 只在 get_total_keys()==0 时才走 Cookie 桥接。
  用户以 Cookie 模式（web_proxy）部署、同时留着 Express Key 时，
  /v1beta/models/{m}:generateContent 永远被 Key 通道霸占，Cookie 模式形同虚设。
  修复后：通道选择尊重 channel_mode——
    web_proxy：有 Cookie 账号即走 Cookie 桥接（generateContent/streamGenerateContent）；
               桥接不支持的操作（countTokens 等）有 Key 时回退 Key 透传；
    api_key / mixed：有 Key 优先透传，无 Key 回退 Cookie 桥接（旧行为保留）。

Bug B（控制台 Cookie 列表按钮静默失效）：
  行内/工具栏「测试会话」按钮 onclick 引用 testCookieAccount，但该函数从未
  导出到 window（channel-config.js 的 Object.assign 清单漏项）→ ReferenceError，
  按钮点了没反应。回归测试静态审计：模板内联事件处理器引用的每个函数
  都必须能在全局作用域（dashboard.js 顶层 / channel-config.js 导出 /
  console-forms.js 导出 / 模板内联脚本）找到定义。

运行：python tests/test_channel_mode_and_console_exports.py
"""
import asyncio
import os
import re
import sys
import tempfile
import types
import unittest
import unittest.mock as mock
from pathlib import Path

_TEST_STATE_DIR = tempfile.TemporaryDirectory(prefix="v2o-mode-test-")
os.environ["STATE_DIR"] = _TEST_STATE_DIR.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx

import config
import credential_manager
from runtime_state import app_state
from routes import gemini_native
from upstreams import cookie_native

COOKIE = "SAPISID=abc; SID=def; __Secure-1PSIDTS=t1"
BRIDGE_SENTINEL = object()


def _request_stub(manager):
    return types.SimpleNamespace(
        headers={},
        query_params=httpx.QueryParams(),
        body=mock.AsyncMock(return_value=b'{"contents":[]}'),
        app=types.SimpleNamespace(state=types.SimpleNamespace(express_key_manager=manager)),
    )


def _bridge_patches():
    bridge = mock.AsyncMock(return_value=BRIDGE_SENTINEL)
    passthrough = mock.AsyncMock(return_value="PASSTHROUGH")
    return (
        (mock.patch.object(cookie_native, "handle_native_request", bridge), bridge),
        (mock.patch.object(gemini_native, "_passthrough_request", passthrough), passthrough),
    )


class GeminiNativeChannelModeTests(unittest.TestCase):
    """Bug A：通道模式必须决定 Gemini 原生格式的通道归属。"""

    def setUp(self):
        # 全局单例必须逐项还原：本套件运行前后的 cookie 账号与通道模式
        # 都不能泄漏给同进程的其它测试（测试隔离硬性要求）。
        self._saved_accounts = app_state.get_cookie_accounts()
        self._saved_mode = app_state.get_channel_mode()
        self.addCleanup(self._restore)

    def _restore(self):
        app_state.set_cookie_accounts(self._saved_accounts)
        app_state.set_channel_mode(self._saved_mode)

    def _run(self, with_cookie, with_keys, mode, action="generateContent"):
        app_state.set_cookie_accounts(
            [{"cookie": COOKIE, "project_id": "proj", "note": "n", "enabled": True}] if with_cookie else [])
        manager = types.SimpleNamespace(get_total_keys=lambda: 1 if with_keys else 0)
        (bridge_patch, bridge), (pt_patch, passthrough) = _bridge_patches()
        with mock.patch.object(app_state, "get_channel_mode", return_value=mode), \
             mock.patch.object(credential_manager.credential_manager, "get_cookie_record",
                               return_value={"cookie": COOKIE, "project_id": "proj", "id": "a1"}
                               if with_cookie else None), \
             bridge_patch, pt_patch:
            result = asyncio.run(gemini_native._passthrough_endpoint()(
                f"gemini-test:{action}", _request_stub(manager), config.API_KEY))
        return result, bridge, passthrough

    def _assert_result(self, result, expected, desc=""):
        body = getattr(result, "body", b"")
        self.assertEqual(result, expected,
                         f"{desc}：得到 {type(result).__name__} body={body!r}")

    def test_web_proxy_mode_prefers_cookie_bridge_even_with_keys(self):
        """Cookie 模式 + 同时有 Key：generateContent 必须走 Cookie 桥接（本次用户报告的场景）。"""
        result, bridge, passthrough = self._run(True, True, "web_proxy")
        self._assert_result(result, BRIDGE_SENTINEL)
        self.assertEqual(bridge.await_count, 1)
        self.assertEqual(passthrough.await_count, 0)

    def test_api_key_mode_still_prefers_passthrough_with_keys(self):
        """Key 模式 + 双通道并存：字节级透传优先（旧行为不回归）。"""
        result, bridge, passthrough = self._run(True, True, "api_key")
        self._assert_result(result, "PASSTHROUGH")
        self.assertEqual(passthrough.await_count, 1)
        self.assertEqual(bridge.await_count, 0)

    def test_mixed_mode_prefers_passthrough_with_keys(self):
        result, bridge, _passthrough = self._run(True, True, "mixed")
        self._assert_result(result, "PASSTHROUGH")
        self.assertEqual(bridge.await_count, 0)

    def test_no_keys_falls_back_to_cookie_bridge(self):
        """无 Key 仅有 Cookie：回退 Cookie 桥接（旧行为保留）。"""
        result, bridge, passthrough = self._run(True, False, "api_key")
        self._assert_result(result, BRIDGE_SENTINEL)
        self.assertEqual(passthrough.await_count, 0)

    def test_web_proxy_unsupported_action_falls_back_to_key(self):
        """Cookie 模式下桥接不支持 countTokens：有 Key 时回退 Key 透传而不是报错。"""
        result, bridge, passthrough = self._run(True, True, "web_proxy", action="countTokens")
        self._assert_result(result, "PASSTHROUGH")
        self.assertEqual(bridge.await_count, 0)

    def test_web_proxy_unsupported_action_without_keys_is_explicit_400(self):
        """Cookie 模式无 Key + 桥接不支持的操作：明确 400（不静默换通道）。"""
        result, bridge, passthrough = self._run(True, False, "web_proxy", action="countTokens")
        self.assertEqual(result.status_code, 400, result.body)
        self.assertIn("不支持", json_loads(result.body)["error"]["message"])
        self.assertEqual(bridge.await_count, 0)

    def test_no_credentials_returns_403(self):
        result, bridge, passthrough = self._run(False, False, "api_key")
        self.assertEqual(result.status_code, 403, result.body)
        self.assertEqual(bridge.await_count, 0)


def json_loads(body):
    import json
    return json.loads(body)


class ConsoleInlineHandlerExportTests(unittest.TestCase):
    """Bug B：模板与脚本内联事件引用的函数必须全部存在于全局作用域。"""

    APP = Path(__file__).resolve().parents[1] / "app"

    def _inline_referenced_functions(self):
        refs = set()
        reserved = {"async", "await", "function", "return", "typeof", "new", "if", "for",
                    "while", "catch", "switch", "do", "else", "case", "void", "delete",
                    "in", "of", "instanceof", "this", "super", "yield", "let", "const",
                    "var", "class", "try", "finally", "throw", "break", "continue"}
        sources = {
            "dashboard.html": (self.APP / "templates" / "dashboard.html").read_text(encoding="utf-8"),
            "channel-config.js": (self.APP / "static" / "channel-config.js").read_text(encoding="utf-8"),
        }
        for name, src in sources.items():
            for m in re.finditer(r'on[a-z]+="([^"]+)"', src):
                code = m.group(1)
                for fm in re.finditer(r'(?<![\w.$])([a-zA-Z_$][\w$]*)\s*\(', code):
                    if fm.group(1) not in reserved:
                        refs.add((fm.group(1), name))
        return refs

    def _global_functions(self):
        dashboard = (self.APP / "static" / "dashboard.js").read_text(encoding="utf-8")
        channel = (self.APP / "static" / "channel-config.js").read_text(encoding="utf-8")
        forms = (self.APP / "static" / "console-forms.js").read_text(encoding="utf-8")
        html = (self.APP / "templates" / "dashboard.html").read_text(encoding="utf-8")
        known = set(re.findall(r'^(?:async\s+)?function\s+([a-zA-Z_$][\w$]*)', dashboard, re.M))
        known |= set(re.findall(r'^(?:async\s+)?function\s+([a-zA-Z_$][\w$]*)', forms, re.M))
        for script in re.findall(r'<script>(.*?)</script>', html, re.S):
            known |= set(re.findall(r'(?:async\s+)?function\s+([a-zA-Z_$][\w$]*)', script))
        # channel-config.js 的 IIFE 通过两条途径导出：consoleChannels 对象属性 + Object.assign(window,{...})
        known |= set(re.findall(r'([a-zA-Z_$][\w$]*)', channel[channel.rfind("window.consoleChannels="):]))
        known |= {"runAction", "confirm", "alert", "event", "window", "document",
                  "navigator", "history", "location", "setTimeout", "parseFloat", "String",
                  "Number", "Boolean", "encodeURIComponent", "decodeURIComponent", "fetchStats"}
        return known

    def test_every_inline_handler_function_is_globally_reachable(self):
        missing = sorted({f for f, _ in self._inline_referenced_functions()
                          if f not in self._global_functions()})
        self.assertEqual(missing, [],
                         "内联事件处理器引用了未导出到全局的函数（点击即 ReferenceError 静默失效）：%s" % missing)

    def test_test_cookie_account_is_exported(self):
        """用户此前「找不到测试 Cookie 功能」的直接根因：按钮存在但函数未导出。"""
        channel = (self.APP / "static" / "channel-config.js").read_text(encoding="utf-8")
        export_block = channel[channel.rfind("Object.assign(window,{"):]
        self.assertIn("testCookieAccount", export_block)
        self.assertIn("refreshCookies", export_block)


if __name__ == "__main__":
    unittest.main(verbosity=2)

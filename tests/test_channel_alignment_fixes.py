"""2026-10-25 深度巡查后的修复回归测试。

覆盖本轮修复的四个主题：
1. Cookie 通道 stop 序列成员级空串过滤（与 Express 通道对齐）；
2. Cookie 非流式错误体统一为 message/type/code/param 四字段；
3. Express 流式首帧 delta 携带 role:"assistant"（真流式注入点 + 假流式）；
4. 代理分享链接解析加固：HY2 mport 显式拒绝、Trojan 未编码冒号密码拼接、
   重复查询参数告警；
5. 控制台前端脚本语法完整性（channel-config.js / dashboard.js）。
"""

import asyncio
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-review-fixes-")
os.environ.setdefault("STATE_DIR", _STATE.name)
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app"))

from models import OpenAIRequest  # noqa: E402
import api_helpers  # noqa: E402
from proxies.protocols import ProxyResolveError, _qs, parse_share_uri_to_outbound  # noqa: E402
import upstreams.cookie_proxy as cookie_proxy  # noqa: E402
import upstreams.cookie_request as cookie_request  # noqa: E402
from upstream_errors import create_openai_error_response  # noqa: E402

USER_MSG = [{"role": "user", "content": "hi"}]


def _request(**kwargs) -> OpenAIRequest:
    return OpenAIRequest(model=kwargs.pop("model", "gemini-3.6-flash"),
                         messages=USER_MSG, **kwargs)


class CookieStopSequenceTests(unittest.TestCase):
    """Cookie 通道 stop 序列：成员级空串过滤（与 generation_config 同口径）。"""

    def _stop_sequences(self, stop):
        body = cookie_request.build_request_body(
            "probe-project", "gemini-3.6-flash", _request(stop=stop))
        return body["variables"]["generationConfig"].get("stopSequences")

    def test_empty_string_members_are_filtered(self):
        self.assertEqual(self._stop_sequences(["", "END", ""]), ["END"])

    def test_all_empty_members_omit_key(self):
        self.assertIsNone(self._stop_sequences(["", ""]))

    def test_string_stop_is_wrapped(self):
        self.assertEqual(self._stop_sequences("END"), ["END"])

    def test_mixed_valid_members_preserved_in_order(self):
        self.assertEqual(self._stop_sequences(["A", "", "B"]), ["A", "B"])


class CookieErrorShapeTests(unittest.TestCase):
    """Cookie 通道错误体统一：message/type/code/param 四字段。"""

    REQUIRED = {"message", "type", "code", "param"}

    def test_helper_shape(self):
        payload = create_openai_error_response(401, "boom", "auth_error")
        self.assertEqual(set(payload["error"]), self.REQUIRED)
        self.assertEqual(payload["error"]["code"], 401)

    def test_missing_cookie_returns_four_field_error(self):
        async def run():
            with mock.patch.object(cookie_proxy.credential_manager, "get_cookie_record",
                                   return_value=None), \
                    mock.patch.object(cookie_proxy, "_get_cookie_string", return_value=""):
                return await cookie_proxy.CookieProxyUpstream().chat_completions(
                    _request(), SimpleNamespace())
        response = asyncio.run(run())
        self.assertEqual(response.status_code, 401)
        error = json.loads(response.body)["error"]
        self.assertEqual(set(error), self.REQUIRED)
        self.assertEqual(error["code"], 401)
        self.assertEqual(error["type"], "auth_error")

    def test_missing_sapisid_returns_four_field_error(self):
        async def run():
            record = {"id": "acc1", "cookie": "SID=abc; HSID=def; SSID=ghi",
                      "project_id": "p", "enabled": True}
            with mock.patch.object(cookie_proxy.credential_manager, "get_cookie_record",
                                   return_value=record):
                return await cookie_proxy.CookieProxyUpstream().chat_completions(
                    _request(), SimpleNamespace())
        response = asyncio.run(run())
        self.assertEqual(response.status_code, 401)
        error = json.loads(response.body)["error"]
        self.assertEqual(set(error), self.REQUIRED)
        self.assertEqual(error["code"], 401)


class ExpressStreamRoleTests(unittest.TestCase):
    """Express 流式首帧 delta 携带 role:"assistant"（对齐 OpenAI 官方帧序）。"""

    @staticmethod
    def _fake_stream_frames():
        async def collect():
            frames = []
            openai_dict = {
                "id": "chatcmpl-test", "model": "gemini-3.6-flash",
                "choices": [{"message": {"role": "assistant", "content": "hello"},
                             "finish_reason": "stop"}],
            }
            async for frame in api_helpers._chunk_openai_response_dict_for_sse(openai_dict):
                frames.append(frame)
            return frames
        return asyncio.run(collect())

    def test_first_delta_carries_role(self):
        frames = self._fake_stream_frames()
        self.assertTrue(frames, "假流式应至少产出一帧")
        data_frames = [f for f in frames if not f.startswith("data: [DONE]")]
        first = json.loads(data_frames[0][len("data:"):])
        delta = first["choices"][0]["delta"]
        self.assertEqual(delta.get("role"), "assistant")
        # 假流式会把正文切块：完整内容按帧拼接断言
        joined = "".join(
            (json.loads(f[len("data:"):])["choices"][0]["delta"].get("content") or "")
            for f in data_frames)
        self.assertEqual(joined, "hello")

    def test_only_first_frame_has_role(self):
        frames = self._fake_stream_frames()
        roles = 0
        for frame in frames:
            if frame.startswith("data: [DONE]"):
                continue
            payload = json.loads(frame[len("data:"):])
            if payload.get("choices") and payload["choices"][0]["delta"].get("role"):
                roles += 1
        self.assertEqual(roles, 1, "role 应只在首帧出现一次")


class ProxyShareLinkHardeningTests(unittest.TestCase):
    """分享链接解析加固：fail-closed 与静默丢弃的显式化。"""

    def test_hysteria2_mport_rejected(self):
        with self.assertRaises(ProxyResolveError) as ctx:
            parse_share_uri_to_outbound("hysteria2://pass@host:443?mport=443,8443&sni=a.com")
        self.assertIn("mport", str(ctx.exception))

    def test_hysteria2_mport_range_rejected(self):
        with self.assertRaises(ProxyResolveError):
            parse_share_uri_to_outbound("hysteria2://pass@host:443?mport=20000-30000")

    def test_hysteria2_without_mport_still_works(self):
        ob = parse_share_uri_to_outbound("hysteria2://pass@host:443?sni=a.com")
        self.assertEqual(ob["type"], "hysteria2")
        self.assertEqual(ob["password"], "pass")

    def test_trojan_password_with_raw_colon_is_restored(self):
        ob = parse_share_uri_to_outbound("trojan://my:pass@host:443?sni=example.com")
        self.assertEqual(ob["password"], "my:pass",
                         "未编码冒号应拼回完整密码，而不是静默截断")

    def test_trojan_encoded_password_unchanged(self):
        ob = parse_share_uri_to_outbound("trojan://my%3Apass@host:443?sni=example.com")
        self.assertEqual(ob["password"], "my:pass")

    def test_qs_duplicate_distinct_values_warn_and_take_first(self):
        with mock.patch("builtins.print") as printed:
            parsed = _qs("path=/a&path=/b&sni=x")
        self.assertEqual(parsed["path"], "/a")
        self.assertTrue(any("path" in str(call) for call in printed.call_args_list),
                        "同名不同值参数应打印告警")

    def test_qs_duplicate_identical_values_stay_silent(self):
        with mock.patch("builtins.print") as printed:
            parsed = _qs("sni=a&sni=a")
        self.assertEqual(parsed["sni"], "a")
        self.assertFalse(printed.call_args_list, "同值重复参数不应告警")


class ConsoleScriptSyntaxTests(unittest.TestCase):
    """控制台前端脚本语法完整性（本轮新增 testCookieAccount 等函数）。"""

    SCRIPTS = ["channel-config.js", "dashboard.js", "console-forms.js"]

    def test_scripts_parse(self):
        node = shutil.which("node")
        if node is None:
            self.skipTest("node 不可用，跳过 JS 语法检查")
        for name in self.SCRIPTS:
            path = ROOT / "app" / "static" / name
            result = subprocess.run([node, "--check", str(path)],
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 0,
                             f"{name} 语法错误：{result.stderr[:400]}")

    def test_cookie_test_entry_point_exists(self):
        source = (ROOT / "app" / "static" / "channel-config.js").read_text(encoding="utf-8")
        self.assertIn("async function testCookieAccount", source,
                      "前端应提供 Cookie 会话测试函数")
        self.assertIn("/api/cookie-accounts/test", source,
                      "前端应调用后端测试端点")
        dashboard_html = (ROOT / "app" / "templates" / "dashboard.html").read_text(encoding="utf-8")
        self.assertIn("测试已保存 Cookie", dashboard_html,
                      "工具栏应提供批量测试按钮")
        self.assertIn("h_cookies", dashboard_html, "Cookie 列表应有帮助块")


if __name__ == "__main__":
    unittest.main(verbosity=2)

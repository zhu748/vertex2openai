"""共享错误处理、异常重试配置和 SDK 拦截响应的离线回归。"""

import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-error-policy-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from google.genai import types

import upstreams.express_pipeline as api_helpers
import config
import retry_policy
from logger import ProxyStats
from models import OpenAIRequest
from response_validation import PendingMetadataGate, SafetyBlockedError
from runtime_state import app_state
from upstream_errors import extract_upstream_error
from upstreams.express_sdk import _build_thinking_config


class RetrySettingsTests(unittest.TestCase):
    def test_oversized_legacy_integers_cannot_overflow_delay_conversion(self):
        legacy = {
            "retry_max": 10**400,
            "retry_backoff_seconds": 10**400,
            "retry_jitter_seconds": -(10**400),
        }
        with mock.patch.object(app_state, "get_setting", side_effect=legacy.get):
            self.assertEqual(retry_policy.get_retry_settings(), (50, 5.0, 1.0))

    def test_retryable_status_whitelist_excludes_deterministic_5xx(self):
        # 2026-11 C-4 回归：501/505 等确定性 5xx 重试必然得到相同结果，
        # 不得进入重试白名单；429 与可自愈的 500/502/503/504 保留。
        for code in (429, 500, 502, 503, 504):
            self.assertTrue(retry_policy.is_retryable_status(code), code)
        for code in (400, 401, 403, 404, 408, 501, 505, 507, 511):
            self.assertFalse(retry_policy.is_retryable_status(code), code)

    def test_retry_after_honored_for_same_identity_clamped_for_rotation(self):
        # 2026-11 P2-5 回归：同一身份重试（Cookie 通道）时上游 Retry-After
        # 有约束力（30s 提示 → 等 30s，不再压到 5.5s 窗口）；换身份重试
        # （Express 通道）维持旧钳位语义；两种模式都保留 120s 绝对上限。
        settings = {"retry_backoff_mode": "fixed"}
        with mock.patch.object(app_state, "get_setting",
                               side_effect=lambda key, default=None: settings.get(key, default)):
            same = retry_policy._retry_delay_value("30", 5.0, 0, 1.0, identity_rotates=False)
            rotate = retry_policy._retry_delay_value("30", 5.0, 0, 1.0, identity_rotates=True)
            self.assertEqual(same, 30.0)
            self.assertLessEqual(rotate, 5.5)
            # 天文数字防御：两种模式都封顶 120 秒。
            self.assertEqual(
                retry_policy._retry_delay_value("9999", 5.0, 0, 1.0, identity_rotates=False),
                120.0)
            # 更短的服务器提示照常生效（不人为拉长）。
            self.assertEqual(
                retry_policy._retry_delay_value("2", 5.0, 0, 1.0, identity_rotates=False),
                2.0)

    def test_sdk_thinking_configuration_is_valid_for_the_pinned_dependency(self):
        obj = OpenAIRequest(model="gemini-3.6-flash", messages=[], reasoning_effort="high")
        with mock.patch.object(
            app_state, "get_effective_settings", return_value=dict(config.DEFAULT_SETTINGS)
        ):
            result = _build_thinking_config(obj.model, obj)
        self.assertEqual(result["thinking_level"], "high")
        self.assertIsNotNone(types.ThinkingConfig(**result).thinking_level)


class BlockedResponseTests(unittest.IsolatedAsyncioTestCase):
    async def test_unspecified_block_reason_does_not_reject_valid_replies(self):
        valid = types.GenerateContentResponse(
            prompt_feedback={"block_reason": "BLOCKED_REASON_UNSPECIFIED"},
            candidates=[{"content": {"parts": [{"text": "valid answer"}]}, "finish_reason": "STOP"}],
        )
        for mode in ("json", "stream", "fake"):
            with self.subTest(mode=mode):
                settings = {"retry_max": 0, "fake_streaming": mode == "fake", "fake_streaming_interval": 0}

                async def stream():
                    yield valid

                models = SimpleNamespace(
                    generate_content=mock.AsyncMock(return_value=valid),
                    generate_content_stream=mock.AsyncMock(side_effect=lambda **_: stream()),
                )
                client = SimpleNamespace(aio=SimpleNamespace(models=models))
                obj = OpenAIRequest(model="gemini-3.6-flash", messages=[], stream=mode != "json")
                with mock.patch.object(app_state, "get_setting", side_effect=settings.get):
                    result = await api_helpers.execute_gemini_call(client, obj.model, lambda _: [], {}, obj)
                    body = ("".join([frame async for frame in result.body_iterator])
                            if mode != "json" else result.body.decode())
                self.assertIn("valid answer", body)
                self.assertNotIn('"error"', body)
                self.assertEqual(models.generate_content.await_count + models.generate_content_stream.await_count, 1)

    async def test_all_delivery_modes_report_400_without_retry(self):
        blocked = types.GenerateContentResponse(
            prompt_feedback={
                "block_reason": "SAFETY",
                "block_reason_message": "upstream detail",
            }
        )
        for mode in ("json", "stream", "fake"):
            with self.subTest(mode=mode):
                settings = {
                    "retry_max": 3,
                    "retry_backoff_seconds": 0,
                    "retry_jitter_seconds": 0,
                    "fake_streaming": mode == "fake",
                    "fake_streaming_interval": 0,
                }
                stats = ProxyStats()

                async def stream():
                    yield blocked

                models = SimpleNamespace(
                    generate_content=mock.AsyncMock(return_value=blocked),
                    generate_content_stream=mock.AsyncMock(side_effect=lambda **_: stream()),
                )
                client = SimpleNamespace(aio=SimpleNamespace(models=models))
                obj = OpenAIRequest(model="gemini-3.6-flash", messages=[], stream=mode != "json")
                with (
                    mock.patch.object(app_state, "get_setting", side_effect=settings.get),
                    mock.patch.object(api_helpers, "stats", stats),
                ):
                    if mode == "json":
                        with self.assertRaises(SafetyBlockedError) as caught:
                            await api_helpers.execute_gemini_call(client, obj.model, lambda _: [], {}, obj)
                        self.assertEqual(extract_upstream_error(caught.exception)[0], 400)
                        self.assertIn("upstream detail", str(caught.exception))
                    else:
                        response = await api_helpers.execute_gemini_call(
                            client, obj.model, lambda _: [], {}, obj
                        )
                        body = "".join([frame async for frame in response.body_iterator])
                        errors = [
                            json.loads(line[6:])["error"]
                            for line in body.splitlines()
                            if line.startswith("data: ") and '"error"' in line
                        ]
                        self.assertEqual(len(errors), 1)
                        self.assertEqual(errors[0]["code"], 400)
                        self.assertIn("upstream detail", errors[0]["message"])
                        self.assertTrue(body.endswith("data: [DONE]\n\n"))
                        self.assertEqual(stats.error_requests, 1)
                self.assertEqual(stats.retry_counts, 0)
                self.assertEqual(stats.success_requests, 0)
                self.assertEqual(
                    models.generate_content.await_count + models.generate_content_stream.await_count, 1
                )


class MetadataBufferTests(unittest.TestCase):
    def test_draining_releases_the_byte_budget(self):
        gate = PendingMetadataGate()
        gate.LIMIT_BYTES = 8
        gate.add("first", 6)
        self.assertEqual(gate.drain(), ["first"])
        gate.add("second", 6)
        self.assertEqual(gate.drain(), ["second"])


if __name__ == "__main__":
    unittest.main()

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

import api_helpers
import config
import retry_policy
from logger import ProxyStats
from models import OpenAIRequest
from response_validation import SafetyBlockedError
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

    def test_sdk_thinking_configuration_is_valid_for_the_pinned_dependency(self):
        obj = OpenAIRequest(model="gemini-3.6-flash", messages=[], reasoning_effort="high")
        with mock.patch.object(
            app_state, "get_effective_settings", return_value=dict(config.DEFAULT_SETTINGS)
        ):
            result = _build_thinking_config(obj.model, obj)
        self.assertEqual(result["thinking_level"], "high")
        self.assertIsNotNone(types.ThinkingConfig(**result).thinking_level)


class BlockedResponseTests(unittest.IsolatedAsyncioTestCase):
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


if __name__ == "__main__":
    unittest.main()

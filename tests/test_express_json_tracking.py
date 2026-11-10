"""Express 非流式（json 模式）请求跟踪同样上报思考档位与输出上限。

回归背景（2026-11-07 用户反馈）：collect_response 旧实现只上报
mode/upstream_model，请求监控对非流式请求永远显示
「思考档位 / 输出上限：默认 / 默认」，让人误以为控制台的强制档位
（native_thinking_mode=console）在非流式路径没有生效。实际上
gen_config_dict 在 chat_completions 阶段就已带上 thinking_config，
流式与非流式共用同一份配置——缺失的只是监控字段。
"""
import asyncio
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-express-json-tracking-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import upstreams.express_pipeline as api_helpers


def _generation(thinking_config=None, max_output_tokens=None):
    gen_config = {}
    if thinking_config is not None:
        gen_config["thinking_config"] = thinking_config
    if max_output_tokens is not None:
        gen_config["max_output_tokens"] = max_output_tokens
    return api_helpers.SDKGeneration(
        current_client=mock.Mock(),
        model_to_call=(
            "projects/p/locations/global/publishers/google/models/"
            "gemini-3.5-flash-lite"
        ),
        gen_config_dict=gen_config,
        request_obj=mock.Mock(stream=False, model="gemini-3.5-flash-lite"),
        actual_prompt_for_call=[],
        is_auto_attempt=False,
        # 客户端“已断开”→ 重试循环首轮即返回 499，无需 mock 上游模型调用，
        # 足以断言 collect_response 入口处的 track_configure 上报内容。
        fastapi_request=mock.Mock(is_disconnected=mock.AsyncMock(return_value=True)),
        prefill_text="",
        fallback_model=None,
        fallback_client_factory=None,
        retry_target_factory=None,
        text_transport=None,
        credential_describer=None,
    )


class ExpressJsonTrackingTests(unittest.TestCase):
    def _collect(self, gen):
        with mock.patch.object(api_helpers, "track_configure") as configure, \
                mock.patch.object(
                    api_helpers, "get_retry_settings", return_value=(0, 0, 0)
                ):
            response = asyncio.run(gen.collect_response())
        return configure, response

    def test_collect_response_reports_thinking_level_and_token_limit(self):
        gen = _generation(
            thinking_config={"thinking_level": "high", "include_thoughts": True},
            max_output_tokens=2048,
        )
        configure, response = self._collect(gen)
        configure.assert_called_once_with(
            mode="json",
            upstream_model=gen.model_to_call,
            thinking_level="high",
            max_output_tokens=2048,
        )
        self.assertEqual(response.status_code, 499)

    def test_collect_response_reports_absent_config_as_none(self):
        """无思考配置（legacy 模型等）时上报 None，监控侧显示「默认」。"""
        gen = _generation()
        configure, response = self._collect(gen)
        configure.assert_called_once_with(
            mode="json",
            upstream_model=gen.model_to_call,
            thinking_level=None,
            max_output_tokens=None,
        )
        self.assertEqual(response.status_code, 499)


if __name__ == "__main__":
    unittest.main(verbosity=2)

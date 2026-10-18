"""生成参数重构后的请求优先级、结构化输出和消息隔离回归。"""

import copy
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-config-test-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import config
import generation_config
import model_capabilities as mc
from models import OpenAIRequest
from runtime_state import app_state
from upstreams.request_preparation import prepare_messages


def request(**kwargs):
    return OpenAIRequest(model="gemini-2.5-flash", messages=[{"role": "user", "content": "hello"}], **kwargs)


class GenerationConfigTests(unittest.TestCase):
    def test_explicit_zero_and_token_alias_take_precedence_over_defaults(self):
        settings = dict(
            config.DEFAULT_SETTINGS, default_temperature=0.8, default_top_p=0.9, default_max_tokens=100
        )
        with mock.patch.object(app_state, "get_effective_settings", return_value=settings):
            explicit = generation_config.create_generation_config(
                request(temperature=0, top_p=0, max_tokens=12, max_completion_tokens=20)
            )
            alias = generation_config.create_generation_config(request(max_completion_tokens=20))
            defaults = generation_config.create_generation_config(request())
        self.assertEqual(
            (explicit["temperature"], explicit["top_p"], explicit["max_output_tokens"]), (0, 0, 12)
        )
        self.assertEqual(alias["max_output_tokens"], 20)
        self.assertEqual(defaults["max_output_tokens"], 100)

    def test_tool_and_output_schemas_are_copied_without_mutating_request(self):
        schema = {"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object"}
        obj = request(
            tools=[{"type": "function", "function": {"name": "lookup", "parameters": schema}}],
            tool_choice={"type": "function", "function": {"name": "lookup"}},
            response_format={"type": "json_schema", "json_schema": {"schema": schema}},
        )
        before = copy.deepcopy(obj.model_dump())
        result = generation_config.create_generation_config(obj)
        self.assertEqual(result["tools"][0]["function_declarations"][0]["parameters"], {"type": "object"})
        self.assertEqual(result["response_schema"], {"type": "object"})
        self.assertEqual(
            result["tool_config"]["function_calling_config"]["allowed_function_names"], ["lookup"]
        )
        self.assertEqual(obj.model_dump(), before)

    def test_system_text_parts_and_logprobs_keep_openai_semantics(self):
        obj = OpenAIRequest(
            model="gemini-2.5-flash",
            logprobs=True,
            top_logprobs=3,
            messages=[
                {"role": "system", "content": [{"type": "text", "text": "first"}]},
                {"role": "system", "content": "second"},
            ],
        )
        result = generation_config.create_generation_config(obj)
        self.assertEqual(result["system_instruction"], "first\nsecond")
        self.assertEqual(result["logprobs"], 3)
        self.assertIs(result["response_logprobs"], True)

    def test_shared_preparation_preserves_input_and_skips_prefill_for_tool_history(self):
        obj = request()
        settings = dict(config.DEFAULT_SETTINGS, inject_system_instruction="policy", inject_prefill="prefix")
        before = obj.model_dump()
        with mock.patch.object(app_state, "get_setting", side_effect=lambda key, default=None: default):
            prepared, prefill, active = prepare_messages(
                obj, mc.get_profile(obj.model), settings, has_tools=False
            )
            tool_request, tool_prefill, tool_active = prepare_messages(
                obj, mc.get_profile(obj.model), settings, has_tools=True
            )
        self.assertEqual(obj.model_dump(), before)
        self.assertTrue(active)
        self.assertEqual(prefill, "prefix")
        self.assertEqual(prepared.messages[0].content, "policy")
        self.assertEqual(tool_request.messages[-1].role, "user")
        self.assertEqual(tool_prefill, "")
        self.assertFalse(tool_active)


if __name__ == "__main__":
    unittest.main()

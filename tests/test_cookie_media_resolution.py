"""Cookie 通道生图 mediaResolution 枚举值的回归测试。

契约：
- nano-banana 家族的 generationConfig.mediaResolution 必须是 GraphQL 枚举成员
  全名 MEDIA_RESOLUTION_HIGH——私有接口类型
  google_cloud_aiplatform_ui_GenerationConfig_MediaResolution 只接受
  UNSPECIFIED/LOW/MEDIUM/HIGH 四个全名成员，历史版本发 "HIGH" 会触发
  "Invalid value for variable $generationConfig" 整个请求被上游拒绝；
- 非 nano-banana 生图模型不下发 mediaResolution（历史行为不变）；
- 文本模型不进入生图分支。
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path

_STATE = tempfile.TemporaryDirectory(prefix="v2o-mediares-test-")
os.environ.setdefault("STATE_DIR", _STATE.name)
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from models import OpenAIRequest  # noqa: E402
from upstreams.cookie_request import build_request_body  # noqa: E402

VALID_MEDIA_RESOLUTION = {
    "MEDIA_RESOLUTION_UNSPECIFIED",
    "MEDIA_RESOLUTION_LOW",
    "MEDIA_RESOLUTION_MEDIUM",
    "MEDIA_RESOLUTION_HIGH",
}

USER_MSG = [{"role": "user", "content": "画一只猫"}]


def _generation_config(model: str) -> dict:
    request = OpenAIRequest(model=model, messages=USER_MSG, stream=False)
    body = build_request_body("test-project", model, request)
    variables = body.get("variables", {})
    return variables.get("generationConfig", {})


class MediaResolutionTests(unittest.TestCase):
    def test_nano_banana_sends_full_enum_name(self):
        config = _generation_config("gemini-nano-banana-2.1")
        value = config.get("mediaResolution")
        self.assertIn(
            value, VALID_MEDIA_RESOLUTION,
            f"mediaResolution={value!r} 不是合法枚举成员全名；"
            "发送短名（如 'HIGH'）会被 batchGraphql 以 "
            "'Invalid value for variable $generationConfig' 整体拒绝",
        )
        self.assertEqual(value, "MEDIA_RESOLUTION_HIGH")

    def test_non_nano_banana_image_model_omits_media_resolution(self):
        for model in ("gemini-3.1-flash-image", "gemini-3-pro-image"):
            with self.subTest(model=model):
                config = _generation_config(model)
                self.assertNotIn(
                    "mediaResolution", config,
                    "media_resolution 是 Nano Banana 家族专属参数",
                )
                self.assertEqual(config.get("responseModalities"), ["TEXT", "IMAGE"])

    def test_text_model_has_no_image_fields(self):
        config = _generation_config("gemini-3.1-pro-preview")
        self.assertNotIn("mediaResolution", config)
        self.assertNotIn("responseModalities", config)


if __name__ == "__main__":
    unittest.main()

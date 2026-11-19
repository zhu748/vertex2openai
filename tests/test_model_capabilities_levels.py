"""思考档位官方矩阵回归（核对于 2026-11-19）。

出处：aistudio.google.com/docs/thinking（per-model 档位表）+
      aistudio.google.com/docs/latest-model（Neon 条目）。

官方档位表（本次全量重核的结论，逐型号锁定为回归）：
  gemini-4-neon        low / medium / high(默认) / xhigh
  gemini-4-hydrogen    low / medium / high(默认) / xhigh
  gemini-4-carbon      low / medium(默认) / high
  gemini-4-argon       low / medium(默认) / high
  gemini-3.8-flash     low / medium(默认) / high
  gemini-3.6-flash     minimal / low / medium(默认) / high
  gemini-3.5-flash-lite minimal(默认) / low / medium / high
  gemini-3.1-pro-preview low / medium / high(默认)
  gemini-3.1-flash-lite minimal(默认) / low / medium / high
  gemini-3-pro-preview low / high(默认)
  官方另注明：minimal 不支持 Gemini 4 全系与 3.8-flash；
             xhigh 仅 4 Neon / 4 Hydrogen。
3.7-flash / 3.5-flash / 3-flash-preview 不在官方表中，沿用项目既有的
家族判定（3.7 无 minimal、3.6- 有 minimal；出处见 model_capabilities.py）。
"""

import unittest

import model_capabilities as mc

# (模型, 官方合法档位集合, 官方默认档)
OFFICIAL_LEVEL_MATRIX = [
    ("gemini-4-neon", {"low", "medium", "high", "xhigh"}, "high"),
    ("gemini-4-hydrogen", {"low", "medium", "high", "xhigh"}, "high"),
    ("gemini-4-carbon", {"low", "medium", "high"}, "medium"),
    ("gemini-4-argon", {"low", "medium", "high"}, "medium"),
    ("gemini-3.8-flash", {"low", "medium", "high"}, "medium"),
    ("gemini-3.6-flash", {"minimal", "low", "medium", "high"}, "medium"),
    ("gemini-3.5-flash-lite", {"minimal", "low", "medium", "high"}, "minimal"),
    ("gemini-3.1-pro-preview", {"low", "medium", "high"}, "high"),
    ("gemini-3.1-flash-lite", {"minimal", "low", "medium", "high"}, "minimal"),
    ("gemini-3-pro-preview", {"low", "high"}, "high"),
    # 官方表未列、按项目家族判定的型号（防意外回归一并锁定）
    ("gemini-3.7-flash", {"low", "medium", "high"}, "medium"),
    ("gemini-3.5-flash", {"minimal", "low", "medium", "high"}, "medium"),
    ("gemini-3-flash-preview", {"minimal", "low", "medium", "high"}, "medium"),
]


class OfficialLevelMatrixTests(unittest.TestCase):
    def test_profiles_match_official_level_table(self):
        for model, levels, default in OFFICIAL_LEVEL_MATRIX:
            with self.subTest(model=model):
                prof = mc.get_profile(model)
                self.assertEqual(prof["thinking_kind"], "level", model)
                self.assertEqual(prof["thinking_levels"], levels, model)
                self.assertEqual(prof["default_level"], default, model)

    def test_minimal_absent_on_gemini_4_and_3_8_flash(self):
        """官方明确：minimal 不支持 Gemini 4 全系与 3.8-flash（下发即 400）。"""
        for model in ("gemini-4-neon", "gemini-4-hydrogen", "gemini-4-carbon",
                      "gemini-4-argon", "gemini-3.8-flash"):
            with self.subTest(model=model):
                self.assertNotIn("minimal", mc.get_profile(model)["thinking_levels"], model)

    def test_xhigh_only_on_neon_and_hydrogen(self):
        for model, has_xhigh in (("gemini-4-neon", True), ("gemini-4-hydrogen", True),
                                 ("gemini-4-argon", False), ("gemini-4-carbon", False),
                                 ("gemini-3.8-flash", False), ("gemini-3.6-flash", False),
                                 ("gemini-3.1-pro-preview", False)):
            with self.subTest(model=model):
                levels = mc.get_profile(model)["thinking_levels"]
                self.assertEqual("xhigh" in levels, has_xhigh, model)

    def test_effort_xhigh_passes_through_on_xhigh_models(self):
        """reasoning_effort=xhigh 在 Neon/Hydrogen 上保留第四档（旧实现折成 high）。"""
        for model, expected in (("gemini-4-neon", "xhigh"), ("gemini-4-hydrogen", "xhigh"),
                                ("gemini-4-argon", "high"), ("gemini-4-carbon", "high"),
                                ("gemini-3.8-flash", "high"), ("gemini-3-pro", "high")):
            with self.subTest(model=model):
                request = type("R", (), {"reasoning_effort": "xhigh",
                                         "model_extra": None, "model": model})()
                resolved = mc.resolve_thinking(model, request, {})
                self.assertEqual(resolved["level"], expected, model)

    def test_max_effort_maps_to_model_maximum(self):
        request = type("R", (), {"reasoning_effort": "max",
                                 "model_extra": None, "model": "gemini-4-neon"})()
        self.assertEqual(mc.resolve_thinking("gemini-4-neon", request, {})["level"], "xhigh")
        request.model = "gemini-4-carbon"
        self.assertEqual(mc.resolve_thinking("gemini-4-carbon", request, {})["level"], "high")

    def test_console_level_whitelist_accepts_xhigh(self):
        """控制台强制档位 thinking_g3_level=xhigh 能下发到 Neon（合法档校验通过）。"""
        request = type("R", (), {"reasoning_effort": None,
                                 "model_extra": None, "model": "gemini-4-neon"})()
        resolved = mc.resolve_thinking("gemini-4-neon", request,
                                       {"thinking_g3_level": "xhigh"})
        self.assertEqual(resolved["level"], "xhigh")

    def test_suppression_floors_xhigh_models_to_low(self):
        """关闭原生思考：Neon/Hydrogen 无 minimal，最低合法档为 low（对齐 NONE→LOW）。"""
        request = type("R", (), {"reasoning_effort": "xhigh",
                                 "model_extra": None, "model": "gemini-4-neon"})()
        resolved = mc.resolve_thinking("gemini-4-neon", request, {"native_thinking_mode": "off"})
        self.assertEqual(resolved["level"], "low")
        self.assertFalse(resolved["include_thoughts"])

    def test_default_level_used_without_request_effort(self):
        """无请求 effort / 控制台设置时用模型默认档：Neon/Hydrogen=high（官方）。"""
        request = type("R", (), {"reasoning_effort": None,
                                 "model_extra": None, "model": "gemini-4-neon"})()
        for model, expected in (("gemini-4-neon", "high"), ("gemini-4-hydrogen", "high"),
                                ("gemini-4-carbon", "medium"), ("gemini-4-argon", "medium")):
            with self.subTest(model=model):
                resolved = mc.resolve_thinking(model, request, {})
                self.assertEqual(resolved["level"], expected, model)

    def test_level_order_and_sorting_include_xhigh(self):
        self.assertEqual(mc._LEVEL_ORDER[-1], "xhigh")
        self.assertEqual(mc.sort_levels({"high", "low", "xhigh", "medium"}),
                         ["low", "medium", "high", "xhigh"])
        summary = mc.capabilities_summary("gemini-4-neon")
        self.assertEqual(summary["thinking"]["levels"],
                         ["low", "medium", "high", "xhigh"])

    def test_neon_sampling_and_family_profile(self):
        """Neon 按家族规则归档：4 系采样参数剥离、search 别名、要求 user 收尾。"""
        prof = mc.get_profile("gemini-4-neon")
        self.assertEqual(prof["family"], "g3")
        self.assertTrue(prof["supports_search"])
        self.assertTrue(prof["requires_user_last_turn"])
        for key in ("temperature", "top_p", "top_k", "candidate_count",
                    "presence_penalty", "frequency_penalty"):
            self.assertNotIn(key, prof["allowed_sampling"], key)
        self.assertIn("max_output_tokens", prof["allowed_sampling"])


if __name__ == "__main__":
    unittest.main()

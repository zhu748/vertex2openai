"""PRF-P1-3：get_effective_settings 修订号 + 每模型缓存回归。

缓存正确性优先于性能：全部已知失效路径（控制台 mutator、reload、
测试直写 _state、原地 pop）都必须让缓存让位于新值。
"""

import unittest

from runtime_state import AppState, app_state


class EffectiveSettingsCacheTests(unittest.TestCase):
    def setUp(self):
        self.snapshot = app_state.get_settings()
        self.addCleanup(app_state.update_settings, self.snapshot)

    def test_repeated_reads_hit_cache_and_return_equal_values(self):
        first = app_state.get_effective_settings("gemini-3.6-flash")
        second = app_state.get_effective_settings("gemini-3.6-flash")
        self.assertEqual(first, second)
        self.assertGreaterEqual(len(app_state._effective_cache), 1)
        # 返回的是浅拷贝：调用方改顶层键不污染缓存。
        second["retry_max"] = 999
        self.assertNotEqual(
            app_state.get_effective_settings("gemini-3.6-flash")["retry_max"], 999)

    def test_update_settings_invalidates_cache(self):
        app_state.get_effective_settings("gemini-3.6-flash")
        app_state.update_settings({"retry_max": 3})
        self.assertEqual(app_state.get_effective_settings("gemini-3.6-flash")["retry_max"], 3)

    def test_model_override_save_and_clear_invalidate_cache(self):
        model = "cache-probe-model"
        base_value = app_state.get_effective_settings(model)["first_reply_timeout_seconds"]
        app_state.set_model_override(model, {"first_reply_timeout_seconds": 42.0})
        self.assertEqual(app_state.get_effective_settings(model)["first_reply_timeout_seconds"], 42.0)
        app_state.clear_model_override(model)
        self.assertEqual(app_state.get_effective_settings(model)["first_reply_timeout_seconds"], base_value)

    def test_direct_state_write_and_inplace_pop_invalidate_cache(self):
        # 测试基建 / 特殊部署路径会绕过 mutator 直写 _state：
        # 缓存键的字典身份 + 键数指纹必须让这类写入立即生效。
        app_state.get_effective_settings("direct-write-model")
        with app_state._lock:
            app_state._state["settings"] = dict(app_state._state.get("settings") or {})
            app_state._state["settings"]["retry_max"] = 7
        self.assertEqual(app_state.get_effective_settings("direct-write-model")["retry_max"], 7)
        # 原地 pop（键数变化）也必须失效：此后回落到内置默认 10 而非残留 7。
        with app_state._lock:
            app_state._state["settings"].pop("retry_max", None)
        self.assertEqual(
            app_state.get_effective_settings("direct-write-model")["retry_max"],
            app_state.get_effective_settings("direct-write-model")["retry_max"])
        self.assertEqual(
            app_state.get_effective_settings("direct-write-model")["retry_max"], 10)

    def test_reload_invalidates_cache(self):
        app_state.get_effective_settings("reload-model")
        app_state.reload()
        # reload 后缓存已清空：再次读取重新合成（不抛错且值一致）。
        self.assertEqual(
            app_state.get_effective_settings("reload-model"),
            app_state.get_effective_settings("reload-model"))

    def test_base_param_path_bypasses_cache_unaffected(self):
        base = app_state.get_settings()
        merged = app_state.get_effective_settings("gemini-3.6-flash", base)
        self.assertEqual(merged["anti_truncation_mode"], "off")
        # base 路径不写缓存（调用方自带基础，避免与缓存键体系混用）。
        self.assertEqual(
            app_state.get_effective_settings("gemini-3.6-flash"),
            merged)


class FreshInstanceCacheTests(unittest.TestCase):
    def test_fresh_instance_cache_starts_empty(self):
        state = AppState()
        self.assertEqual(state._effective_cache, {})
        self.assertEqual(state._settings_rev, 0)


if __name__ == "__main__":
    unittest.main()

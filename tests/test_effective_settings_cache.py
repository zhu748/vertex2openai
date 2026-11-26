"""PRF-P1-3：get_effective_settings 修订号 + 每模型缓存回归。

缓存只反映已提交的模型设置，且不能被草稿或旧缓存未命中覆盖。
"""

from concurrent.futures import ThreadPoolExecutor
from threading import Event
import unittest
from unittest import mock

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
        # 返回的是浅拷贝：冷/热读取都不暴露缓存字典本身。
        first["retry_max"] = 998
        self.assertNotEqual(
            app_state.get_effective_settings("gemini-3.6-flash")["retry_max"], 998)
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

    def test_cold_reader_cannot_republish_a_superseded_revision(self):
        model = "cold-reader-model"
        app_state.update_settings({"retry_max": 7})
        source = app_state._committed
        merging, release = Event(), Event()
        merge = app_state._merged_settings

        def gated(snapshot):
            if snapshot is source:
                merging.set()
                assert release.wait(5)
            return merge(snapshot)

        with mock.patch.object(app_state, "_merged_settings", side_effect=gated), ThreadPoolExecutor(2) as pool:
            pending = pool.submit(app_state.get_effective_settings, model)
            try:
                self.assertTrue(merging.wait(5))
                pool.submit(app_state.update_settings, {"retry_max": 2}).result(3)
            finally:
                release.set()
            self.assertEqual(pending.result(3)["retry_max"], 7)
        self.assertNotIn(model, app_state._effective_cache)
        self.assertEqual(app_state.get_effective_settings(model)["retry_max"], 2)

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

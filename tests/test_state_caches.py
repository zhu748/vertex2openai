"""签名缓存过期顺序、图片旧配置与生成参数快照回归。"""

import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-state-caches-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import config
import image_processing
from models import OpenAIRequest
from runtime_state import app_state
from signature_store import SignatureStore, SignatureRecord, SignatureState
from upstreams import cookie_request


class SignatureExpiryTests(unittest.TestCase):
    def test_expired_recently_read_record_does_not_evict_live_record(self):
        store = SignatureStore(ttl_seconds=10, max_entries=2)
        with mock.patch("signature_store.time.monotonic") as clock:
            clock.return_value = 0
            store.put_unknown("old")
            clock.return_value = 5
            store.put_unknown("live")
            clock.return_value = 6
            self.assertIsNotNone(store.get_record("old"))
            clock.return_value = 11
            store.put_unknown("new")
            self.assertIsNone(store.get_record("old"))
            self.assertIsNotNone(store.get_record("live"))
            self.assertIsNotNone(store.get_record("new"))

    def test_overwrite_refreshes_expiry_and_preserves_new_signature(self):
        store = SignatureStore(ttl_seconds=10, max_entries=2)
        with mock.patch("signature_store.time.monotonic") as clock:
            clock.return_value = 0
            store.put_unknown("call")
            clock.return_value = 5
            record = SignatureRecord(SignatureState.SIGNED, b"new-signature")
            store.put_record("call", record)
            clock.return_value = 11
            store.put_unknown("new")
            self.assertEqual(store.get_record("call"), record)

    def test_capacity_still_uses_lru_and_both_indexes_remain_bounded(self):
        store = SignatureStore(ttl_seconds=100, max_entries=2)
        store.put_unknown("a")
        store.put_unknown("b")
        store.get_record("a")
        store.put_unknown("c")
        self.assertIsNone(store.get_record("b"))
        self.assertIsNotNone(store.get_record("a"))
        for i in range(100):
            store.put_unknown(str(i))
        self.assertEqual(len(store._data), 2)
        self.assertEqual(len(store._expiry), 2)


class ConfigurationSnapshotTests(unittest.TestCase):
    def test_legacy_nonfinite_image_settings_fall_back_without_overflow(self):
        for key, value in (
            ("img_compress_max_mb", float("inf")),
            ("img_compress_max_mb", 10**400),
            ("img_compress_max_dim", float("inf")),
            ("img_compress_quality", float("inf")),
        ):
            settings = dict(config.DEFAULT_SETTINGS, **{key: value})
            with self.subTest(key=key), mock.patch.object(app_state, "get_settings", return_value=settings):
                self.assertEqual(
                    image_processing.optimize_image_bytes(b"small-image", "image/png"),
                    (b"small-image", "image/png"),
                )

    def test_cookie_request_uses_its_settings_snapshot_for_thinking(self):
        first = dict(config.DEFAULT_SETTINGS, thinking_g3_level="high")
        later = dict(config.DEFAULT_SETTINGS, thinking_g3_level="low")
        request = OpenAIRequest(model="gemini-3.6-flash", messages=[])
        with mock.patch.object(app_state, "get_effective_settings", side_effect=[first, later]):
            body = cookie_request.build_request_body("project", request.model, request)
        self.assertEqual(body["variables"]["generationConfig"]["thinkingConfig"]["thinkingLevel"], "HIGH")


if __name__ == "__main__":
    unittest.main()

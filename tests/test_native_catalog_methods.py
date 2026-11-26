"""Gemini list/get must advertise the native methods that can actually be routed."""
from types import SimpleNamespace
import unittest
from unittest import mock

from credential_manager import credential_manager
from routes import gemini_native


class NativeCatalogMethodsTests(unittest.IsolatedAsyncioTestCase):
    async def test_list_and_get_align_on_count_tokens_for_enabled_express_and_cookie_only(self):
        for keys, mode in ((0, "api_key"), (0, "web_proxy"), (0, "mixed"),
                           (1, "web_proxy"), (1, "api_key")):
            manager = SimpleNamespace(get_total_keys=lambda: keys)
            req = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(express_key_manager=manager)))
            with self.subTest(keys=keys, mode=mode), \
                 mock.patch.object(gemini_native, "get_express_models",
                                   mock.AsyncMock(return_value=["gemini-test"])), \
                 mock.patch.object(gemini_native.app_state, "get_channel_mode", return_value=mode), \
                 mock.patch.object(credential_manager, "has_cookie_record", return_value=True):
                listing = await gemini_native.native_list_models(req, "offline")
                entry = await gemini_native.native_get_model("gemini-test", req, "offline")
            self.assertIn(entry, listing["models"])
            for item in listing["models"]:
                methods = item["supportedGenerationMethods"]
                self.assertIn("generateContent", methods)
                self.assertIn("streamGenerateContent", methods)
                self.assertEqual("countTokens" in methods, bool(keys))

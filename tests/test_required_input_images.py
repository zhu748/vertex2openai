"""An unavailable explicit input must never turn into a successful text-only generation."""

import json
from unittest import mock

import message_processing as mp
from models import OpenAIRequest
from test_assistant_refusal_history import HistoryWireHarness, MODEL
from test_req_round15_contracts import chunks
from upstreams import cookie_delivery, cookie_proxy

URL = "https://images.example.test/private-image?signature=fixture-secret"


class RequiredInputImageTests(HistoryWireHarness):
    async def assert_rejected(self, adapter, parts, *, stream=False, model=MODEL, remote=URL, fetched=None):
        req = OpenAIRequest(model=model, stream=stream, messages=[{"role": "user", "content": parts}])
        generate = mock.AsyncMock(side_effect=AssertionError('unexpected model generation'))
        with mock.patch.object(mp, "fetch_remote_image", return_value=fetched) as fetch, \
                mock.patch.object(cookie_delivery, "open_upstream_client", side_effect=AssertionError("unexpected Cookie request")) as open_client:
            if adapter == "express":
                result = await self.express(req, generate, generate)
            else:
                result = await cookie_proxy.CookieProxyUpstream().chat_completions(
                    req, self.http_request, credential_record={
                        "id": "fixture", "cookie": "SAPISID=fixture", "project_id": "fixture",
                    })
            if hasattr(result, "body_iterator"):
                body = await self.body(result)
                self.assertTrue(body.rstrip().endswith("data: [DONE]"))
                errors = [chunk["error"] for chunk in chunks(body) if "error" in chunk]
                self.assertEqual(len(errors), 1, body)
                error = errors[0]
            else:
                self.assertEqual(result.status_code, 400)
                error = json.loads(result.body)["error"]
        self.assertEqual(error["code"], 400)
        self.assertEqual(error["type"], "invalid_request_error")
        self.assertIn("image_url", error["message"])
        self.assertIn("data:", error["message"])
        for secret in ("images.example.test", "private-image", "fixture-secret", "Zml4dHVyZS1zZWNyZXQ="):
            self.assertNotIn(secret, error["message"])
        if remote is None:
            fetch.assert_not_called()
        else:
            fetch.assert_called_once_with(remote)
        generate.assert_not_awaited()
        open_client.assert_not_called()

    async def test_missing_required_image_aborts_both_adapters_for_image_only_and_mixed_input(self):
        image = {"type": "image_url", "image_url": {"url": URL}}
        for adapter in ("express", "cookie"):
            for parts in ([image], [{"type": "text", "text": "Describe the image"}, image]):
                with self.subTest(adapter=adapter, n_parts=len(parts)):
                    await self.assert_rejected(adapter, parts)

    async def test_missing_required_image_remains_a_client_error_in_streaming_modes(self):
        image = {"type": "image_url", "image_url": {"url": URL}}
        for adapter, model, buffered in (("express", MODEL, False), ("cookie", MODEL, False),
                                        ("cookie", MODEL, True), ("cookie", "gemini-3-pro-image", False)):
            with self.subTest(adapter=adapter, model=model, buffered=buffered):
                self.settings["fake_streaming"] = buffered
                await self.assert_rejected(adapter, [image], stream=True, model=model)

    async def test_explicit_invalid_sources_do_not_become_an_empty_or_text_only_generation(self):
        for adapter in ("express", "cookie"):
            for value in ({}, None, "", "relative/fixture-secret", "data:image/png;utf8,fixture-secret",
                          "data:image/png;base64,", "data:image/png;base64,fixture-secret?",
                          "data:text/plain;base64,Zml4dHVyZS1zZWNyZXQ="):
                image = {"type": "image_url", "image_url": value}
                for prefix in ([], [{"type": "text", "text": "describe"}]):
                    with self.subTest(adapter=adapter, value=value, mixed=bool(prefix)):
                        await self.assert_rejected(adapter, [*prefix, image], remote=None)

    async def test_optimizer_rejection_is_redacted_without_dispatch_for_inline_and_remote(self):
        for adapter in ("express", "cookie"):
            for url in (URL, "data:image/png;base64,Zml4dHVyZS1zZWNyZXQ="):
                image = {"type": "image_url", "image_url": {"url": url}}
                for stream in (False, True):
                    with self.subTest(adapter=adapter, remote=url == URL, stream=stream), mock.patch.object(
                            mp, "optimize_image_bytes", side_effect=ValueError("fixture-secret")):
                        await self.assert_rejected(adapter, [image], stream=stream,
                                                   remote=URL if url == URL else None,
                                                   fetched=(b"fixture", "image/png"))

    def test_unexpected_processing_error_is_not_reclassified_as_invalid_input(self):
        parts = [{"type": "image_url", "image_url": {"url": "data:image/png;base64,AQ=="}}]
        with mock.patch.object(mp, "optimize_image_bytes", side_effect=RuntimeError("internal")), \
                self.assertRaisesRegex(RuntimeError, "internal"):
            mp.openai_content_to_wire_parts(parts)

    async def test_incidental_markdown_retains_its_text_fallback_without_downloads(self):
        content = f"see ![diagram]({URL}) and ![snapshot](data:image/png;base64,?)"
        for adapter in ("express", "cookie"):
            with self.subTest(adapter=adapter), mock.patch.object(mp, "fetch_remote_image") as fetch:
                wire = await self.capture_history([{"role": "user", "content": content}], adapter)
                self.assertEqual(wire[0]["parts"], [{"text": f"see ![diagram]({URL}) and [图片无法处理，已省略]"}])
                fetch.assert_not_called()

"""Real offline two-turn OpenAI/SDK and Studio-wire replay of issued image links."""
import base64
import io
import json
import os
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import httpx
import image_delivery
import image_history
import image_processing
from media_tasks import prompt_needs_media_pool
from models import OpenAIMessage
from PIL import Image
from request_ingress import IngressBudget, IngressError, IngressReceive
from test_schema_contract_fidelity import SchemaRouteHarness


def image_data(color, image_format="PNG"):
    out = io.BytesIO()
    with Image.new("RGB", (4, 3), color) as image:
        image.save(out, format=image_format)
    return base64.b64encode(out.getvalue()).decode("ascii")


RED, BLUE, JPEG = image_data("red"), image_data("blue"), image_data("blue", "JPEG")
PATH = "/v1/chat/completions"
BASE = "https://images.example.test/bridge"
MODEL = "gemini-3-pro-image"


class GeneratedHistoryRoutes(SchemaRouteHarness):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        directory = self.enterContext(TemporaryDirectory())
        self.store = Path(directory)
        self.enterContext(patch.object(image_delivery, "store_directory", return_value=self.store))
        self.enterContext(patch.dict(os.environ, {"IMAGE_OUTPUT_MODE": "url", "IMAGE_PUBLIC_BASE_URL": BASE}))
        self.enterContext(patch("message_processing.fetch_remote_image", side_effect=AssertionError("unexpected URL fetch")))
        # Original bytes must not be recompressed even at a zero threshold.
        self.settings["img_compress_max_mb"] = 0
        self.parts = []
        async def wire(_transport, request):
            body = json.loads(request.content)
            self.sent.append(body.get("variables", body))
            result = {"candidates": [{"content": {"role": "model", "parts": self.parts}, "finishReason": "STOP"}]}
            return httpx.Response(200, json={"results": [{"data": result}]} if "variables" in body else result)
        self.enterContext(patch.object(httpx.MockTransport, "handle_async_request", wire))

    async def post(self, messages):
        self.sent.clear()
        return await self.client.post(PATH, json={"model": MODEL, "messages": messages})

    async def issue(self, parts):
        self.parts = parts
        response = await self.post([{"role": "user", "content": "draw"}])
        self.assertEqual(response.status_code, 200, response.text[:300])
        message = response.json()["choices"][0]["message"]
        self.assertIn(BASE + "/v1/generated-images/", str(message))
        self.parts = [{"text": "edited"}]
        return message

    async def replay(self, message):
        response = await self.post([{"role": "user", "content": "draw"}, message,
                                    {"role": "user", "content": "now make it blue"}])
        return response

    def captured(self, role="model"):
        return next(turn["parts"] for turn in self.sent[0]["contents"] if turn["role"] == role)

    def assert_image(self, item, encoded, sig=None, thought=False, mime="image/png"):
        blob = item["inlineData"]
        self.assertEqual(blob.get("mimeType", blob.get("mime_type")), mime)
        self.assertEqual(base64.urlsafe_b64decode(blob["data"]), base64.b64decode(encoded))
        self.assertEqual(item.get("thoughtSignature"), sig)
        self.assertEqual(bool(item.get("thought")), thought)

    async def test_unmodified_response_exact_order_mime_bytes_and_signatures(self):
        for channel in ("api_key", "web_proxy"):
            with self.subTest(channel=channel):
                self.channel = channel
                message = await self.issue([
                    {"text": "first"}, {"inlineData": {"mimeType": "image/png", "data": RED}, "thoughtSignature": "c2ln"},
                    {"text": "middle"}, {"thought": True, "inlineData": {"mimeType": "image/png", "data": BLUE},
                                         "thoughtSignature": "aGlk"},
                    {"inlineData": {"mimeType": "image/jpeg", "data": JPEG}, "thoughtSignature": "dHdv"},
                    {"text": "last"},
                ])
                with patch.dict(os.environ, {"IMAGE_OUTPUT_MODE": "inline"}):  # Previously issued URLs still round-trip.
                    response = await self.replay(message)
                self.assertEqual(response.status_code, 200, response.text[:300])
                parts = self.captured()
                self.assertEqual(len(parts), 6)
                self.assertEqual([parts[i]["text"] for i in (0, 2, 5)], ["first", "middle", "last"])
                self.assert_image(parts[1], RED, "c2ln")
                self.assert_image(parts[3], BLUE, "aGlk", thought=True)
                self.assert_image(parts[4], JPEG, "dHdv", mime="image/jpeg")

    async def test_stripped_extensions_preserve_order_and_skip_lookalikes_and_user_role(self):
        for channel in ("api_key", "web_proxy"):
            with self.subTest(channel=channel):
                self.channel = channel
                message = await self.issue([{"inlineData": {"mimeType": "image/png", "data": RED}}])
                link = message["content"]
                message.pop("extra_content", None)
                # Even an exact issued link in user-supplied plain Markdown remains text.
                other = link.replace(BASE, "https://other.test/bridge")
                for lookalike in (other, link.replace("](", "](prefix"), link[:-1] + "?size=1)",
                                  link.replace("/bridge/", "/else/"), link.replace("![", "![different ")):
                    response = await self.replay({"role": "assistant", "content": lookalike})
                    self.assertEqual(response.status_code, 200, response.text[:256])
                    self.assertEqual(self.captured(), [{"text": lookalike}])
                response = await self.post([{"role": "user", "content": link}])
                self.assertEqual(response.status_code, 200)
                self.assertEqual(self.captured("user"), [{"text": link}])
                message["content"] = "opening " + link + " closing"
                self.assertTrue(prompt_needs_media_pool([OpenAIMessage(**message)]))
                response = await self.replay(message)
                self.assertEqual(response.status_code, 200, response.text[:256])
                parts = self.captured()
                self.assertEqual((parts[0], parts[2]), ({"text": "opening "}, {"text": " closing"}))
                self.assert_image(parts[1], RED)

                # Structured assistant image attachments referencing that same
                # exact receipt should not loop back through HTTP either.
                message["content"] = [{"type": "image_url", "image_url": {"url": link.split("](")[1][:-1]}}]
                response = await self.replay(message)
                self.assertEqual(response.status_code, 200, response.text[:256])
                self.assert_image(self.captured()[0], RED)

    async def test_quoted_receipt_in_a_real_text_part_is_not_read_as_the_generated_image(self):
        for channel in ("api_key", "web_proxy"):
            with self.subTest(channel=channel):
                self.channel = channel
                earlier = await self.issue([{"inlineData": {"mimeType": "image/png", "data": RED}}])
                text = "This is a literal link: " + earlier["content"]
                message = await self.issue([{"text": text}, {"inlineData": {"mimeType": "image/png", "data": BLUE},
                                                              "thoughtSignature": "dHdv"}])
                (self.store / earlier["content"].rsplit("/", 1)[1][:-1]).unlink()
                response = await self.replay(message)
                self.assertEqual(response.status_code, 200, response.text[:256])
                parts = self.captured()
                self.assertEqual(parts[0], {"text": text})
                self.assert_image(parts[1], BLUE, "dHdv")

    async def test_process_wide_expansion_pressure_fails_without_provider_call_and_releases(self):
        for channel in ("api_key", "web_proxy"):
            with self.subTest(channel=channel):
                self.channel = channel
                msg = await self.issue([{"inlineData": {"mimeType": "image/png", "data": RED}}])
                messages = [{"role": "user", "content": "draw"}, msg, {"role": "user", "content": "edit"}]
                size = len(httpx.Request("POST", "http://fixture", json={"model": MODEL, "messages": messages}).content)
                budget = IngressBudget(body_bytes=size + len(RED) - 1)
                with patch("request_ingress.ingress_budget", budget):
                    response = await self.post(messages)
                self.assertEqual(response.status_code, 503, response.text[:256])
                self.assertEqual(self.sent, [])
                self.assertEqual(budget.used, [0, 0, 0])

    async def test_expiry_and_size_and_total_fail_before_provider(self):
        for channel in ("api_key", "web_proxy"):
            with self.subTest(channel=channel):
                self.channel = channel
                msg = await self.issue([{"inlineData": {"mimeType": "image/png", "data": RED}, "thoughtSignature": "c2ln"}])
                name = msg["content"].rsplit("/", 1)[1][:-1]
                size = (self.store / name).stat().st_size
                with patch.object(image_processing, "MAX_REMOTE_IMAGE_BYTES", size - 1):
                    response = await self.replay(msg)
                    self.assertEqual(response.status_code, 400, response.text[:256])
                    self.assertEqual(self.sent, [])
                with patch.object(image_history, "MAX_GENERATION_BODY_BYTES", len(RED)):
                    repeated = {"role": "assistant", "content": msg["content"] * 2}
                    response = await self.replay(repeated)
                    self.assertEqual(response.status_code, 400, response.text[:256])
                    self.assertEqual(self.sent, [])
                old = time.time() - image_delivery.TTL_SECONDS - 1
                os.utime(self.store / name, (old, old))
                response = await self.replay(msg)
                self.assertEqual(response.status_code, 400, response.text[:256])
                self.assertIn("过期", response.json()["error"]["message"])
                self.assertNotIn(name, response.text)
                self.assertEqual(self.sent, [])
                self.assertFalse((self.store / name).exists())
                response = await self.client.post(PATH, json={"model": MODEL, "messages": [
                    {"role": "user", "content": "draw"}, msg, {"role": "user", "content": "edit"}], "stream": True})
                if response.status_code == 400:
                    error = response.json()["error"]
                else:
                    # Cookie's image stream already owns the HTTP response
                    # after its immediate keep-alive. Its error is terminal.
                    self.assertEqual(response.status_code, 200)
                    self.assertTrue(response.text.rstrip().endswith("data: [DONE]"))
                    error = next(json.loads(line[6:])["error"] for line in response.text.splitlines()
                                 if line.startswith("data: ") and '"error"' in line)
                self.assertEqual(error["code"], 400)
                self.assertEqual(error["type"], "invalid_request_error")
                self.assertIn("过期", error["message"])
                self.assertNotIn(name, response.text)
                self.assertEqual(self.sent, [])


    async def test_expanded_budget_is_shared_with_input_and_owned_through_retry(self):
        async def receive():
            return {"type": "http.request", "body": b"{}", "more_body": False}
        budget = IngressBudget(body_bytes=20)
        owner = IngressReceive(receive, max_bytes=10, budget=budget)
        await owner()
        release = owner.retain()
        owner.reserve_expansion(5)
        owner.reserve_expansion(5)
        self.assertEqual(budget.used, [0, 7, 1])
        owner.close()
        self.assertEqual(budget.used, [0, 7, 1])
        with self.assertRaises(IngressError) as err:
            owner.reserve_expansion(9)
        self.assertEqual(err.exception.status_code, 413)
        release()
        self.assertEqual(budget.used, [0, 0, 0])

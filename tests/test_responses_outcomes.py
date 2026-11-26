"""Only a public Responses outcome should settle a request, not its Chat producer."""

import asyncio
from unittest.mock import patch

import httpx
from fastapi.responses import JSONResponse
from starlette.requests import ClientDisconnect

import channel_dispatch
from logger import ProxyStats
import main
from routes import responses_api
from test_replay_http_delivery import Peer
from test_responses_api import MODEL, PATH, event_list, request
from test_schema_contract_fidelity import SchemaRouteHarness
from upstreams import cookie_delivery, express_pipeline


class ResponsesOutcomes(SchemaRouteHarness):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.counters = ProxyStats()
        for mod in (main, channel_dispatch, responses_api, cookie_delivery, express_pipeline):
            self.enterContext(patch.object(mod, "stats", self.counters))
        self.usage_metadata = {"promptTokenCount": 7, "candidatesTokenCount": 5, "totalTokenCount": 12}

    def assert_count_delta(self, before, *, success, error, tokens=None):
        after = self.counters.get_json_stats()
        if tokens is None:
            tokens = (7, 5) if self.channel == "api_key" else (0, 0)
        expected = {"total": 1, "success": success, "error": error,
                    "prompt_tokens": tokens[0], "completion_tokens": tokens[1]}
        self.assertEqual({key: after[key] - before[key] for key in expected}, expected)

    async def post_at(self, body, phase):
        if phase != "late":
            with patch.object(responses_api, "SETUP_FAST_WINDOW_SECONDS", 2):
                return await asyncio.wait_for(self.client.post(PATH, json=body), 5)

        # Guarantee the status is committed before the real offline producer
        # starts, independent of scheduler and mock transport speed.
        producer = responses_api.call_upstream
        ready = asyncio.Event()

        async def wait_for_sse(*args):
            await ready.wait()
            return await producer(*args)

        async def send(message):
            if message.get("body", b"").startswith(b"event: response.in_progress"):
                ready.set()

        peer = Peer(PATH, payload=body)
        with patch.object(responses_api, "SETUP_FAST_WINDOW_SECONDS", 0), patch.object(
                responses_api, "call_upstream", wait_for_sse):
            await asyncio.wait_for(peer.run(send), 5)
        start, *frames = peer.messages
        return httpx.Response(start["status"], headers=start["headers"],
                              content=b"".join(frame.get("body", b"") for frame in frames))

    async def test_projection_failure_and_completed_output_are_distinct_on_both_transports(self):
        for self.channel in ("api_key", "web_proxy"):
            for phase in ("json", "early", "late"):
                for rejected in (False, True):
                    with self.subTest(channel=self.channel, phase=phase, rejected=rejected):
                        self.reply = {"text": "This is a genuine provider answer."}
                        before = self.counters.get_json_stats()
                        resp = await self.post_at(request(stream=phase != "json",
                                                          tool_choice="required" if rejected else "auto"), phase)
                        if rejected and phase != "late":
                            self.assertEqual(resp.status_code, 502)
                            self.assertIn("tool_choice", resp.json()["error"]["message"])
                        else:
                            self.assertEqual(resp.status_code, 200)
                            public = (resp.json() if phase == "json"
                                      else event_list(resp.text)[-1]["response"])
                            self.assertEqual(public["status"], "failed" if rejected else "completed")
                            if not rejected:
                                self.assertEqual(public["usage"]["total_tokens"], 12)
                        self.assert_count_delta(before, success=int(not rejected), error=int(rejected))

    async def test_deliverable_incomplete_keeps_one_outcome(self):
        for self.channel in ("api_key", "web_proxy"):
            for stream in (False, True):
                with self.subTest(channel=self.channel, stream=stream):
                    self.reply = {"text": "An unfinished answer."}
                    self.finish_reason = "MAX_TOKENS"
                    before = self.counters.get_json_stats()
                    response = await self.client.post(PATH, json=request(stream=stream))
                    self.assertEqual(response.status_code, 200)
                    public = event_list(response.text)[-1]["response"] if stream else response.json()
                    self.assertEqual(public["status"], "incomplete")
                    self.assertEqual(public["incomplete_details"], {"reason": "max_output_tokens"})
                    self.assert_count_delta(before, success=1, error=0)

    async def test_failed_terminal_render_is_counted_only_by_middleware(self):
        event = responses_api.ResponsesEvents.event

        def render(writer, kind, **payload):
            if kind == "response.failed":
                raise ValueError("fixture failed terminal rendering")
            yield from event(writer, kind, **payload)

        before = self.counters.get_json_stats()
        with patch.object(responses_api.ResponsesEvents, "event", render):
            with self.assertRaisesRegex(ValueError, "fixture failed terminal rendering"):
                await self.post_at(request(stream=True, tool_choice="required"), "late")
        self.assert_count_delta(before, success=0, error=1)

    async def test_losing_primary_proposal_is_not_charged_after_standby_succeeds(self):
        self.channel = "web_proxy"
        self.settings["cross_channel_fallback"] = True

        async def primary_failure(*args, **kwargs):
            self.counters.add_error()
            return JSONResponse({"error": {"message": "Unavailable"}}, status_code=503)

        with patch.object(channel_dispatch.cookie_upstream, "chat_completions", primary_failure):
            for phase in ("json", "early", "late"):
                with self.subTest(phase=phase):
                    before = self.counters.get_json_stats()
                    self.sent.clear()
                    self.reply = {"text": "From Express."}
                    response = await self.post_at(request(stream=phase != "json"), phase)
                    self.assertEqual(response.status_code, 200)
                    public = response.json() if phase == "json" else event_list(response.text)[-1]["response"]
                    self.assertEqual(public["status"], "completed")
                    self.assertEqual(public["output"][0]["content"][0]["text"], "From Express.")
                    self.assertEqual(len(self.sent), 1)
                    self.assertNotIn("variables", self.sent[0])
                    self.assert_count_delta(before, success=1, error=0, tokens=(7, 5))

    async def test_cookie_legacy_notice_and_identically_spelled_real_output(self):
        self.channel = "web_proxy"
        self.reply = []
        before = self.counters.get_json_stats()
        chat = await self.client.post("/v1/chat/completions", json={
            "model": MODEL, "messages": [{"role": "user", "content": "Test the legacy notice."}],
            # A client cannot select the server's private production contract.
            "_completion_target": "responses",
        })
        self.assertEqual(chat.status_code, 200, chat.text[:240])
        diagnostic = chat.json()["choices"][0]["message"]["content"]
        self.assertIn("[Studio 提示]", diagnostic)
        self.assert_count_delta(before, success=0, error=1)

        for self.channel in ("web_proxy", "api_key"):
            for phase in ("json", "early", "late"):
                with self.subTest(channel=self.channel, phase=phase):
                    self.reply = {"text": diagnostic}
                    self.settings["safety_score"] = True
                    self.safety_ratings = [{"category": "HARM_CATEGORY_HARASSMENT", "probability": "LOW"}]
                    before = self.counters.get_json_stats()
                    response = await self.post_at(request(stream=phase != "json"), phase)
                    self.assertEqual(response.status_code, 200)
                    public = response.json() if phase == "json" else event_list(response.text)[-1]["response"]
                    self.assertEqual(public["status"], "completed")
                    self.assertEqual(public["output"][0]["content"][0]["text"], diagnostic)
                    self.assert_count_delta(before, success=1, error=0)

    async def test_missing_finish_and_empty_stops_are_upstream_failures(self):
        for self.channel in ("web_proxy", "api_key"):
            for phase in ("json", "early", "late"):
                for self.finish_reason, self.reply in (
                    ("STOP", []), ("STOP", [{"text": "  "}]),
                    (None, []), (None, [{"text": "thought without a terminal", "thought": True}]),
                    ("FINISH_REASON_UNSPECIFIED", []),
                ):
                    with self.subTest(channel=self.channel, phase=phase, finish=self.finish_reason, reply=self.reply):
                        before = self.counters.get_json_stats()
                        response = await self.post_at(request(stream=phase != "json"), phase)
                        if phase == "late":
                            self.assertEqual(response.status_code, 200)
                            events = event_list(response.text)
                            self.assertEqual([e["type"] for e in events], [
                                "response.created", "response.in_progress", "response.failed",
                            ])
                            self.assertEqual(events[-1]["response"]["output"], [])
                            self.assertEqual(events[-1]["response"]["error"]["code"], "server_error")
                        else:
                            self.assertEqual(response.status_code, 502, response.text[:220])
                            self.assertEqual(response.json()["error"]["type"], "upstream_error")
                        self.assertNotIn("[Studio 提示]", response.text)
                        self.assert_count_delta(before, success=0, error=1)

    async def test_real_empty_terminals_and_reasoning_only_outputs_are_typed(self):
        self.settings["safety_score"] = True
        self.safety_ratings = [{"category": "HARM_CATEGORY_HARASSMENT", "probability": "LOW"}]
        text = "Actually visible reasoning."
        thought = {"text": text, "thought": True}
        for self.channel in ("web_proxy", "api_key"):
            for phase in ("json", "early", "late"):
                for self.finish_reason, self.reply, incomplete in (
                    ("STOP", [thought], None),
                    ("MAX_TOKENS", [], "max_output_tokens"),
                    ("MAX_TOKENS", [thought], "max_output_tokens"),
                    ("SAFETY", [], "content_filter"),
                    ("SAFETY", [thought], "content_filter"),
                ):
                    with self.subTest(channel=self.channel, phase=phase, finish=self.finish_reason, reply=self.reply):
                        before = self.counters.get_json_stats()
                        response = await self.post_at(request(stream=phase != "json"), phase)
                        self.assertEqual(response.status_code, 200, response.text[:220])
                        public = response.json() if phase == "json" else event_list(response.text)[-1]["response"]
                        self.assertEqual(public["status"], "incomplete" if incomplete else "completed")
                        self.assertEqual(public["incomplete_details"], {"reason": incomplete} if incomplete else None)
                        self.assertEqual(public["usage"]["output_tokens"], 5)
                        if self.reply:
                            self.assertEqual([item["type"] for item in public["output"]], ["reasoning"])
                            self.assertEqual(public["output"][0]["content"], [{"type": "reasoning_text", "text": text}])
                        else:
                            self.assertEqual(public["output"], [])
                        self.assert_count_delta(before, success=1, error=0)

    async def test_prompt_feedback_is_a_typed_terminal_on_both_channels(self):
        self.settings["cross_channel_fallback"] = True
        self.settings["anti_truncation_mode"] = "nonstream"
        self.usage_metadata.update(cachedContentTokenCount=3, thoughtsTokenCount=2, totalTokenCount=14)
        contradictory = [
            {"text": "unexpected candidate text"},
            {"text": "unexpected candidate thinking", "thought": True},
            {"functionCall": {"name": "count_colors", "id": "unexpected-call", "args": {}},
             "thoughtSignature": "Y29udHJhZGljdG9yeQ=="},
            {"functionCall": {"name": "count_colors", "id": "partial-call", "willContinue": True}},
        ]
        with patch.object(channel_dispatch.credential_manager, "has_cookie_record", return_value=True):
            for self.channel in ("web_proxy", "api_key"):
                for phase in ("json", "early", "late"):
                    for model, reason, self.finish_reason, self.reply in (
                        (MODEL, "SAFETY", None, []),
                        (MODEL, "PROHIBITED_CONTENT", "STOP", contradictory),
                        (MODEL + "-antitruncation-nonstream", "OTHER", None, []),
                    ):
                        with self.subTest(channel=self.channel, phase=phase, model=model, reason=reason):
                            self.prompt_feedback = {"blockReason": reason, "blockReasonMessage": "operator detail"}
                            before = self.counters.get_json_stats()
                            self.sent.clear()
                            response = await self.post_at(request(model=model, stream=phase != "json"), phase)
                            self.assertEqual(response.status_code, 200, response.text[:240])
                            if phase == "json":
                                public = response.json()
                            else:
                                events = event_list(response.text)
                                self.assertEqual([e["type"] for e in events], [
                                    "response.created", "response.in_progress", "response.incomplete",
                                ])
                                public = events[-1]["response"]
                            self.assertEqual(public["status"], "incomplete")
                            self.assertEqual(public["incomplete_details"], {"reason": "content_filter"})
                            self.assertIsNone(public["error"])
                            self.assertEqual(public["output"], [])
                            self.assertEqual(public["usage"], {
                                "input_tokens": 7, "output_tokens": 7, "total_tokens": 14,
                                "input_tokens_details": {"cached_tokens": 3, "cache_write_tokens": 0},
                                "output_tokens_details": {"reasoning_tokens": 2},
                            })
                            for marker in ("operator detail", "unexpected", "partial-call", "Y29udHJhZGljdG9yeQ=="):
                                self.assertNotIn(marker, response.text)
                            wire, = self.sent  # Neither retries nor standby may reissue a blocked prompt.
                            self.assertEqual("variables" in wire, self.channel == "web_proxy")
                            if model != MODEL:
                                generation = wire.get("variables", wire)
                                self.assertTrue(any(fn["name"].startswith("v2o_emit_") for tool in generation["tools"]
                                                    for fn in tool.get("functionDeclarations", [])))
                            self.assert_count_delta(before, success=1, error=0,
                                                    tokens=(7, 7) if self.channel == "api_key" else (0, 0))

    async def test_unknown_prompt_feedback_is_not_a_typed_block_or_ordinary_completion(self):
        for self.channel in ("web_proxy", "api_key"):
            for reason, expected_status in (("BLOCKED_REASON_UNSPECIFIED", 200), ("FUTURE_UNSPECIFIED", 502)):
                with self.subTest(channel=self.channel, reason=reason):
                    self.prompt_feedback = {"blockReason": reason, "blockReasonMessage": "operator detail"}
                    self.reply = {"text": "A real response."}
                    response = await self.client.post(PATH, json=request())
                    self.assertEqual(response.status_code, expected_status, response.text[:240])
                    self.assertNotIn("operator detail", response.text)
                    if expected_status == 200:
                        self.assertEqual(response.json()["output"][0]["content"][0]["text"], "A real response.")
                    else:
                        self.assertNotIn("FUTURE_UNSPECIFIED", response.text)
                        self.assertNotIn("A real response.", response.text)

    async def test_chat_keeps_legacy_prompt_block_presentation(self):
        self.finish_reason, self.reply = None, []
        self.prompt_feedback = {"blockReason": "SAFETY", "blockReasonMessage": "operator detail"}
        for self.channel in ("web_proxy", "api_key"):
            with self.subTest(channel=self.channel):
                response = await self.client.post("/v1/chat/completions", json={
                    "model": MODEL, "messages": [{"role": "user", "content": "Test the legacy notice."}],
                })
                if self.channel == "web_proxy":
                    self.assertEqual(response.status_code, 200)
                    choice, = response.json()["choices"]
                    self.assertEqual(choice["finish_reason"], "content_filter")
                    self.assertIn("[Studio 提示]", choice["message"]["content"])
                else:
                    self.assertEqual(response.status_code, 400)
                    self.assertIn("operator detail", response.json()["error"]["message"])

    async def test_cookie_empty_and_blocked_output_never_estimates_the_gateway_notice(self):
        self.channel = "web_proxy"
        self.usage_metadata = None
        self.settings["usage_estimation"] = True
        for phase in ("json", "early", "late"):
            for self.finish_reason, self.prompt_feedback in (
                ("MAX_TOKENS", None), (None, {"blockReason": "SAFETY", "blockReasonMessage": "fixture hint"}),
            ):
                self.reply = []
                before = self.counters.get_json_stats()
                response = await self.post_at(request(stream=phase != "json"), phase)
                self.assertEqual(response.status_code, 200, response.text[:240])
                public = response.json() if phase == "json" else event_list(response.text)[-1]["response"]
                incomplete = "content_filter" if self.prompt_feedback else "max_output_tokens"
                self.assertEqual(public["status"], "incomplete")
                self.assertEqual(public["incomplete_details"], {"reason": incomplete})
                self.assertEqual(public["output"], [])
                self.assertNotIn("fixture hint", response.text)
                self.assertEqual(public["usage"]["output_tokens"], 0)
                self.assertTrue(public["usage"]["estimated"])
                self.assert_count_delta(before, success=1, error=0)

    async def test_success_waits_for_final_send_even_when_provider_usage_is_already_known(self):
        for self.channel in ("api_key", "web_proxy"):
            for stream in (False, True):
                for loss in (None, "silent", "raised"):
                    with self.subTest(channel=self.channel, stream=stream, loss=loss):
                        self.reply = {"text": "Provider finished."}
                        peer = Peer(PATH, payload=request(stream=stream))
                        before = self.counters.get_json_stats()
                        final_seen = False

                        async def send(message):
                            nonlocal final_seen
                            if message["type"] != "http.response.body" or message.get("more_body", False):
                                return
                            final_seen = True
                            self.assert_count_delta(before, success=0, error=0)
                            if loss == "silent":
                                await peer.drop()
                            elif loss == "raised":
                                raise OSError("fixture final send failed")

                        if loss == "raised":
                            with self.assertRaises(ClientDisconnect if stream else OSError):
                                await asyncio.wait_for(peer.run(send), 5)
                        else:
                            await asyncio.wait_for(peer.run(send), 5)
                        self.assertTrue(final_seen)
                        self.assert_count_delta(before, success=int(loss is None), error=0)

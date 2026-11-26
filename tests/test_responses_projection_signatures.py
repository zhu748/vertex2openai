"""Only the public Responses receipt may record its provider's tool batch."""

import asyncio
import base64
from unittest.mock import patch

from starlette.requests import ClientDisconnect

from routes import responses_api
from signature_store import signature_store
from test_replay_http_delivery import Peer
from test_responses_api import (MODEL, PATH, SIGNATURES, TOOLS, assistant, calls_in_wire, continuation,
                                event_list, request, signature, thin_item)
import test_responses_outcomes
from test_responses_typed_parts import public
from test_schema_contract_fidelity import SchemaRouteHarness


class ResponsesProjectionSignatures(SchemaRouteHarness):
    post_at = test_responses_outcomes.ResponsesOutcomes.post_at

    async def test_tool_choice_rejected_by_public_projection_has_no_bare_or_scoped_record(self):
        choices = (
            {"tools": []},
            {"tool_choice": "none"},
            {"tool_choice": {"type": "function", "name": "another_tool"},
             "tools": [*TOOLS, {**TOOLS[0], "name": "another_tool"}]},
        )
        for self.channel in ("api_key", "web_proxy"):
            for phase in ("json", "early", "late"):
                for n, choice in enumerate(choices):
                    with self.subTest(channel=self.channel, phase=phase, choice=choice):
                        ident = f"unpublished-projection:{self.channel}:{phase}:{n}"
                        self.sent.clear()
                        self.reply = [assistant(ident, SIGNATURES[0])]
                        with patch.object(signature_store, "register_call_signature",
                                          wraps=signature_store.register_call_signature) as record, patch.object(
                                responses_api, "publish_responses_item_origins") as publish:
                            reply = await self.post_at(request(stream=phase != "json", **choice), phase)
                        if phase == "late":
                            self.assertEqual([event["type"] for event in event_list(reply.text)],
                                             ["response.created", "response.in_progress", "response.failed"])
                        else:
                            self.assertEqual(reply.status_code, 502, reply.text[:240])
                        self.assertNotIn(ident, reply.text)
                        self.assertEqual(len(self.sent), 1)
                        record.assert_not_called()
                        publish.assert_not_called()
                        self.assertIsNone(signature_store.get_record(ident))

    async def test_lost_final_send_cannot_register_a_valid_but_undelivered_batch(self):
        for self.channel in ("api_key", "web_proxy"):
            for stream in (False, True):
                with self.subTest(channel=self.channel, stream=stream):
                    ident = f"lost-responses-final:{self.channel}:{stream}"
                    self.reply = [assistant(ident, SIGNATURES[0])]
                    peer = Peer(PATH, payload=request(stream=stream))

                    async def reject_final(message):
                        if message["type"] == "http.response.body" and not message.get("more_body", False):
                            raise OSError("fixture rejected the final send")

                    with patch.object(signature_store, "register_call_signature",
                                      wraps=signature_store.register_call_signature) as record, patch.object(
                            responses_api, "publish_responses_item_origins") as publish:
                        with self.assertRaises((OSError, ClientDisconnect)):
                            await asyncio.wait_for(peer.run(reject_final), 3)
                    self.assertEqual(peer.messages[0]["status"], 200)
                    record.assert_not_called()
                    publish.assert_not_called()
                    self.assertIsNone(signature_store.get_record(ident))

    async def test_delivered_opaque_id_retains_explicit_and_scoped_receipts_not_bare_records(self):
        for provider, other in (("api_key", "web_proxy"), ("web_proxy", "api_key")):
            for phase in ("json", "early", "late"):
                with self.subTest(provider=provider, phase=phase):
                    self.channel = provider
                    ident = f"responses-opaque:{provider}:{phase}__thought__YQ=="
                    prompt = f"Check scoped Responses receipt ({provider}/{phase})."
                    self.reply = [assistant(ident, SIGNATURES[0])]
                    with patch.object(signature_store, "register_call_signature",
                                      wraps=signature_store.register_call_signature) as record:
                        reply = await self.post_at(request(input=prompt, stream=phase != "json"), phase)
                    self.assertEqual(reply.status_code, 200, reply.text[:240])
                    call, = public(reply, phase)["output"]
                    self.assertEqual(call["call_id"], ident)
                    self.assertEqual(base64.b64decode(call["extra_content"]["google"]["thought_signature"]), SIGNATURES[0])
                    record.assert_not_called()
                    self.assertIsNone(signature_store.get_record(ident))
                    thin = thin_item(call)
                    old_client = {key: value for key, value in thin.items() if key != "id"}
                    self.reply = {"text": "done"}
                    for self.channel, item, expected in ((provider, call, SIGNATURES[0]), (provider, thin, SIGNATURES[0]),
                            (provider, old_client, SIGNATURES[0]), (other, thin, None)):
                        replay = await self.post_at(request(input=continuation(prompt, [item])), "json")
                        self.assertEqual(replay.status_code, 200, replay.text[:240])
                        wire_call, = calls_in_wire(self.sent[-1], "functionCall")
                        wire_result, = calls_in_wire(self.sent[-1], "functionResponse")
                        self.assertEqual(wire_call["functionCall"]["id"], ident)
                        self.assertEqual(wire_result["functionResponse"]["id"], ident)
                        self.assertEqual(signature(wire_call), expected)

    async def test_legacy_chat_still_registers_bare_id_and_sends_original_bytes(self):
        for self.channel in ("api_key", "web_proxy"):
            with self.subTest(channel=self.channel):
                ident = f"bare-legacy-chat:{self.channel}"
                self.reply = [assistant(ident, SIGNATURES[0])]
                with patch.object(signature_store, "register_call_signature",
                                  wraps=signature_store.register_call_signature) as record:
                    reply = await self.client.post("/v1/chat/completions", json={
                        "model": MODEL, "messages": [{"role": "user", "content": "fixture"}],
                        "tools": [{"type": "function", "function": {"name": "count_colors"}}],
                    })
                self.assertEqual(reply.status_code, 200, reply.text[:240])
                call, = reply.json()["choices"][0]["message"]["tool_calls"]
                self.assertEqual(call["id"], ident)
                self.assertEqual(base64.b64decode(call["extra_content"]["google"]["thought_signature"]), SIGNATURES[0])
                record.assert_called_once_with(ident, SIGNATURES[0], unsigned_follower=False)
                self.assertEqual(signature_store.get_record(ident).signature, SIGNATURES[0])

                self.reply = [assistant(ident, SIGNATURES[1])]
                rejected = await self.post_at(request(tool_choice="none"), "json")
                self.assertEqual(rejected.status_code, 502, rejected.text[:240])
                self.assertEqual(signature_store.get_record(ident).signature, SIGNATURES[0])

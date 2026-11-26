"""Offline full native bridge, including the real HTTP reader and retry boundary."""
from contextlib import asynccontextmanager
import json
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import httpx

from upstreams import cookie_native as bridge


MODEL = "gemini-3.5-flash-lite"
RECORD = {"cookie": "SAPISID=test", "project_id": "test", "id": "test"}
HIGH = [{"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "probability": "HIGH"}]
RETRY = {"error": {"code": 503, "message": "temporarily unavailable"}}
MODES = (("generateContent", {}), ("streamGenerateContent", {"alt": "sse"}),
         ("streamGenerateContent", {"alt": "json"}))


def block(candidate=None, **extra):
    data = {"candidates": [candidate]} if candidate is not None else {}
    return {"results": [{"data": {**data, **extra}}]}


async def request(attempts, mode):
    calls = []

    def respond(_request):
        calls.append(1)
        return httpx.Response(200, json=attempts[len(calls) - 1])

    @asynccontextmanager
    async def opened(_):
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            yield client

    action, query = mode
    stub = SimpleNamespace(query_params=query, is_disconnected=AsyncMock(return_value=False))
    with (patch.object(bridge, "open_upstream_client", opened),
          patch.object(bridge, "build_headers", return_value={}),
          patch.object(bridge.credential_manager, "get_cookie_record", return_value=RECORD),
          patch.object(bridge.credential_manager, "record_cookie_success"),
          patch.object(bridge, "get_retry_settings", return_value=(1, 0, 0))):
        response = await bridge._handle_native_request_impl(
            stub, MODEL, action, {}, requested_model=MODEL, deadline=time.monotonic() + 3)
        if hasattr(response, "body_iterator"):
            chunks = [frag.decode() if isinstance(frag, bytes) else frag async for frag in response.body_iterator]
            text = "".join(chunks)
            frames = ([json.loads(line[6:]) for line in text.splitlines() if line.startswith("data: ")]
                      if query.get("alt") == "sse" else json.loads(text))
        else:
            frames = [json.loads(response.body)]
    return len(calls), frames


class AttemptMetadata(unittest.IsolatedAsyncioTestCase):
    async def test_retry_drops_losing_usage_finish_safety_and_keeps_winning_snapshots(self):
        losing = [block({"finishReason": "SAFETY", "safetyRatings": HIGH}, usageMetadata={"promptTokenCount": 99}),
                  RETRY]
        winner = [block(usageMetadata={"cachedContentTokenCount": 1}),
                  block({"content": {"parts": [{"text": "ok"}]}, "finishReason": "STOP"},
                        usageMetadata={"candidatesTokenCount": 1})]
        for mode in MODES:
            with self.subTest(mode=mode):
                calls, frames = await request([losing, winner], mode)
                assert calls == 2, frames
                assert frames[-1]["usageMetadata"] == {"cachedContentTokenCount": 1, "candidatesTokenCount": 1}
                assert frames[-1]["candidates"][0]["finishReason"] == "STOP"
                assert "safetyRatings" not in frames[-1]["candidates"][0]
                assert [p["text"] for f in frames for c in f["candidates"]
                        for p in c.get("content", {}).get("parts", [])] == ["ok"]

    async def test_real_no_content_terminal_and_prompt_feedback_keep_their_own_usage(self):
        for mode in MODES:
            for kind in ("finish", "prompt"):
                with self.subTest(mode=mode, kind=kind):
                    kwargs = {"usageMetadata": {"promptTokenCount": 3}}
                    raw = (block({"finishReason": "SAFETY", "safetyRatings": HIGH}, **kwargs)
                           if kind == "finish" else block(promptFeedback={"blockReason": "SAFETY"}, **kwargs))
                    calls, frames = await request([[raw]], mode)
                    assert calls == 1
                    last = frames[-1]
                    assert last.get("usageMetadata") == kwargs["usageMetadata"]
                    if kind == "finish":
                        assert last["candidates"][0]["safetyRatings"] == HIGH
                    else:
                        assert last["promptFeedback"] == {"blockReason": "SAFETY"} and "candidates" not in last

    async def test_committed_part_is_not_replayed_after_failure(self):
        for mode in MODES:
            with self.subTest(mode=mode):
                calls, frames = await request([[block({"content": {"parts": [{"text": "partial"}]}}), RETRY]], mode)
                assert calls == 1
                assert frames[-1]["error"]["code"] == 503


GROUND = {
    "groundingChunks": [{"web": {"uri": "https://example.org/library", "title": "Library"}}],
    "groundingSupports": [{"segment": {"text": "ok", "startIndex": 0, "endIndex": 2},
                           "groundingChunkIndices": [0]}],
}
CITES = {"citations": [{"startIndex": 0, "endIndex": 2, "uri": "https://example.org/citation", "license": "CC0"}]}


class GroundedMetadata(unittest.IsolatedAsyncioTestCase):
    async def test_raw_candidates_preserve_queries_sources_and_citations_only_on_winner(self):
        losing = [block({"groundingMetadata": {"webSearchQueries": ["ignore this"]}, "citationMetadata": CITES}), RETRY]
        query = {"webSearchQueries": ["library"]}
        winning = [block({"groundingMetadata": query}),
                   block({"content": {"parts": [{"text": "ok"}]}, "groundingMetadata": GROUND}),
                   block({"finishReason": "STOP", "citationMetadata": CITES})]
        for mode in MODES:
            with self.subTest(mode=mode):
                calls, frames = await request([losing, winning], mode)
                assert calls == 2
                candidate = frames[-1]["candidates"][0]
                assert candidate.get("groundingMetadata") == {**query, **GROUND}
                assert candidate.get("citationMetadata") == CITES
                assert "ignore this" not in json.dumps(frames)

    async def test_retry_to_ungrounded_winner_does_not_fabricate_sources(self):
        losing = [block({"groundingMetadata": GROUND, "citationMetadata": CITES}), RETRY]
        winning = [block({"content": {"parts": [{"text": "plain"}]}, "finishReason": "STOP"})]
        for mode in MODES:
            with self.subTest(mode=mode):
                _, frames = await request([losing, winning], mode)
                assert not any(key in c for f in frames for c in f["candidates"]
                               for key in ("citationMetadata", "groundingMetadata"))

    async def test_invalid_reference_shapes_never_escape_as_verified_citations(self):
        bad_examples = ({"groundingMetadata": {"groundingChunks": "not sources"}},
                        {"groundingMetadata": {**GROUND, "groundingSupports": [{"groundingChunkIndices": [3]}]}},
                        {"groundingMetadata": {"groundingChunks": [{"privateOnly": True}],
                                               "groundingSupports": [{"groundingChunkIndices": [0]}]}},
                        {"citationMetadata": {"citations": [{"uri": 42}]}},
                        {"citationMetadata": {"citations": [{"startIndex": 7, "endIndex": 3}]}})
        for mode in MODES:
            for bad in bad_examples:
                with self.subTest(mode=mode, bad=bad):
                    calls, frames = await request([[block({"content": {"parts": [{"text": "ok"}]},
                                                           "finishReason": "STOP", **bad})]], mode)
                    assert calls == 1
                    assert frames[-1].get("error", {}).get("code") == 502

    async def test_replaced_sources_cannot_silently_retarget_older_supports(self):
        changed = {"groundingChunks": [{"web": {"uri": "https://example.org/unrelated"}}]}
        for mode in MODES:
            with self.subTest(mode=mode):
                _, frames = await request([[block({"groundingMetadata": GROUND}),
                                            block({"finishReason": "STOP", "groundingMetadata": changed})]], mode)
                assert frames[-1].get("error", {}).get("code") == 502

    async def test_default_empty_metadata_does_not_change_ungrounded_output(self):
        for mode in MODES:
            with self.subTest(mode=mode):
                _, frames = await request([[block({"finishReason": "STOP", "groundingMetadata": {"groundingChunks": []},
                                                   "citationMetadata": {"citations": []}})]], mode)
                candidate = frames[-1]["candidates"][0]
                assert "groundingMetadata" not in candidate and "citationMetadata" not in candidate

    async def test_public_citation_lists_keep_their_source_spelling_without_private_extras(self):
        ml = {"citationSources": [{**CITES["citations"][0], "__typename": "InternalType"}], "__typename": "Private"}
        _, frames = await request([[block({"finishReason": "STOP", "citationMetadata": ml})]], MODES[0])
        assert frames[-1]["candidates"][0]["citationMetadata"] == {"citationSources": CITES["citations"]}

    async def test_metadata_budget_applies_after_combining_sparse_frames_to_all_modes(self):
        from upstreams import cookie_candidate_metadata
        uri = "https://example.org/" + "x" * 190
        frames_in = [block({"content": {"parts": [{"text": "ok"}]},
                            "groundingMetadata": {"webSearchQueries": ["q" * 210]}}),
                     block({"finishReason": "STOP", "citationMetadata": {"citations": [{"uri": uri}]}})]
        with patch.object(cookie_candidate_metadata, "MAX_METADATA_BYTES", 400):
            for mode in MODES:
                with self.subTest(mode=mode):
                    _, frames = await request([frames_in], mode)
                    assert frames[-1].get("error", {}).get("code") == 502

    async def test_pre_reply_gate_rejects_a_too_large_metadata_prefix(self):
        from response_validation import PendingMetadataGate

        async def events(*args, **kwargs):
            yield "usage", {"promptTokenCount": 1}
            yield "finish", "STOP"

        with (patch.object(PendingMetadataGate, "LIMIT_BYTES", 8), patch.object(bridge, "stream_events", events)):
            for mode in MODES:
                with self.subTest(mode=mode):
                    calls, frames = await request([], mode)
                    assert calls == 0 and frames[-1].get("error", {}).get("code") == 502

    def test_repeated_identical_snapshots_do_not_grow_the_assembled_wire_budget(self):
        candidate_metadata = {"groundingMetadata": GROUND, "citationMetadata": CITES}
        assembler = bridge.NativeAssembler(MODEL)
        assembler.feed("candidate_metadata", candidate_metadata)
        total = assembler.response_bytes
        assembler.max_response_bytes = total + 20
        for _ in range(20):
            assembler.feed("candidate_metadata", candidate_metadata)
        assert assembler.response_bytes == total
        assembler.feed("finish", "STOP")
        assert assembler.payload()["candidates"][0]["citationMetadata"] == CITES
        with self.assertRaises(bridge.NativeBridgeError):
            assembler.feed("text", "x" * 30)


class NativePromptFeedback(unittest.IsolatedAsyncioTestCase):
    async def test_winning_refusal_keeps_public_feedback_but_never_content_or_losing_usage(self):
        real = {"blockReason": "SAFETY", "blockReasonMessage": "说明（detail）",
                "safetyRatings": [{**HIGH[0], "blocked": False, "probabilityScore": 0.25}]}
        wire = {**real, "__typename": "private", "safetyRatings": [{**real["safetyRatings"][0], "private": 5}]}
        losing = [block(usageMetadata={"promptTokenCount": 99}, promptFeedback={
            "blockReason": "BLOCKED_REASON_UNSPECIFIED", "blockReasonMessage": "not a refusal"}), RETRY]
        winner = [block({"content": {"parts": [{"text": "must not be emitted"}]}},
                        promptFeedback=wire, usageMetadata={"promptTokenCount": 3})]
        for mode in MODES:
            with self.subTest(mode=mode):
                calls, frames = await request([losing, winner], mode)
                assert calls == 2 and len(frames) == 1
                assert frames[0]["promptFeedback"] == real
                assert frames[0]["usageMetadata"] == {"promptTokenCount": 3} and "candidates" not in frames[0]

    async def test_malformed_or_oversized_details_do_not_erase_a_real_block_or_escape(self):
        for fields in ({"blockReasonMessage": ["private"]}, {"safetyRatings": [{"blocked": "false"}]},
                       {"blockReasonMessage": "x" * (65536 + 1)}, {"safetyRatings": [HIGH[0]] * 65}):
            for mode in MODES:
                with self.subTest(mode=mode, keys=list(fields)):
                    calls, frames = await request([[block(promptFeedback={"blockReason": "SAFETY", **fields})]], mode)
                    assert calls == 1 and frames[-1]["promptFeedback"] == {"blockReason": "SAFETY"}
                    assert "candidates" not in frames[-1]

    async def test_bad_block_reason_does_not_get_stringified_or_become_a_success(self):
        for mode in MODES:
            with self.subTest(mode=mode):
                _, frames = await request([[block({"finishReason": "STOP", "content": {"parts": [{"text": "no"}]}},
                                                 promptFeedback={"blockReason": {"private": True}})]], mode)
                assert frames[-1].get("error", {}).get("code") == 502
                assert "private" not in json.dumps(frames) and "candidates" not in frames[-1]

    def test_ordinary_cookie_channel_keeps_its_text_diagnostic_contract(self):
        from upstreams.cookie_response import extract_events
        raw = block(promptFeedback={"blockReason": "SAFETY", "blockReasonMessage": "detail", "safetyRatings": HIGH})
        assert list(extract_events(raw)) == [("blocked", "SAFETY（detail）")]

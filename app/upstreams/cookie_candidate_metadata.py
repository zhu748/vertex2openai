"""Bounded, public response metadata for one native Cookie candidate. Never resolve source URIs."""
import json
from typing import Any, Iterator

from pydantic import ValidationError

from response_validation import IncompleteResponseError

# Per native candidate (not per field/frame). Protect both model-validation
# allocations and the eventual terminal payload, including after sparse updates.
MAX_METADATA_BYTES = 1024 * 1024
_MAX_NODES = 16_384
_SCHEMAS = {"groundingMetadata": "GroundingMetadata", "citationMetadata": "CitationMetadata"}


def _bounded_wire(value: Any, *, limit: int | None = None) -> bytes:
    limit = MAX_METADATA_BYTES if limit is None else limit
    nodes = chars = 0

    def inspect(item: Any, depth: int) -> None:
        nonlocal nodes, chars
        nodes += 1
        if isinstance(item, str):
            chars += len(item)
        if nodes > _MAX_NODES or depth > 32 or chars > limit:
            raise IncompleteResponseError("上游引用元数据超过大小或结构限制。")
        if isinstance(item, dict):
            for key, child in item.items():
                inspect(key, depth + 1)
                inspect(child, depth + 1)
        elif isinstance(item, list):
            for child in item:
                inspect(child, depth + 1)

    inspect(value, 0)
    try:
        wire = json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (ValueError, TypeError) as exc:
        raise IncompleteResponseError("上游引用元数据不是有效 JSON。") from exc
    if len(wire) > limit:
        raise IncompleteResponseError("上游引用元数据超过大小限制。")
    return wire


def native_prompt_feedback(feedback: Any) -> dict | None:
    """Keep a real refusal even when optional diagnostics cannot safely be represented."""
    if feedback is None:
        return None
    if not isinstance(feedback, dict):
        raise IncompleteResponseError("上游 promptFeedback 不是对象。")
    reason = feedback.get("blockReason")
    if reason is None or reason == "":
        return None
    if not isinstance(reason, str) or len(reason) > 128 or not reason.isascii() or not reason.isidentifier():
        raise IncompleteResponseError("上游 promptFeedback.blockReason 无效。")
    if reason.upper().endswith("UNSPECIFIED"):
        return None  # proto default in ordinary responses; never invent a block
    refused = {"blockReason": reason}
    details = {k: feedback[k] for k in ("blockReasonMessage", "safetyRatings") if feedback.get(k) is not None}
    if not details:
        return refused
    from google.genai import types
    if "safetyRatings" in details:
        ratings = details["safetyRatings"]
        if not isinstance(ratings, list) or len(ratings) > 64:
            return refused
        enums = {"category": types.HarmCategory, "probability": types.HarmProbability,
                 "severity": types.HarmSeverity, "overwrittenThreshold": types.HarmBlockThreshold}
        fields = (*enums, "blocked", "probabilityScore", "severityScore")
        clean = []
        for raw in ratings:
            if not isinstance(raw, dict):
                return refused
            rating = {key: raw[key] for key in fields if raw.get(key) is not None}
            # The SDK accepts arbitrary unknown enum values, with a warning. Do
            # not publish those or turn private-only entries into empty ratings.
            if not rating or any(not isinstance(rating[k], str) or rating[k] not in enum.__members__
                                 for k, enum in enums.items() if k in rating):
                return refused
            clean.append(rating)
        details["safetyRatings"] = clean
    try:
        wire = _bounded_wire(details, limit=64 * 1024)
        checked = types.GenerateContentResponsePromptFeedback.model_validate_json(wire, strict=True, extra="forbid")
    except (IncompleteResponseError, ValidationError):
        return refused
    return {**refused, **checked.model_dump(mode="json", by_alias=True, exclude_none=True, exclude_unset=True)}


def native_candidate_metadata(candidate: dict) -> dict:
    """Accept public fields by their pinned SDK types, not private extras or fabricated citations."""
    result = {}
    for field, schema in _SCHEMAS.items():
        raw = candidate.get(field)
        if raw is None or raw == {}:
            continue
        # The Gemini API and Vertex use different public names for this list.
        # Validate the content with the same model but retain the source spelling.
        mldev = field == "citationMetadata" and isinstance(raw, dict) and "citationSources" in raw
        wire = _bounded_wire(raw)
        if mldev:
            if "citations" in raw:
                raise IncompleteResponseError("上游 citationMetadata 同时包含两种引用列表。")
            wire = _bounded_wire({("citations" if k == "citationSources" else k): v for k, v in raw.items()})
        # No schema compilation/SDK initialization on ordinary ungrounded requests.
        from google.genai import types
        try:
            model = getattr(types, schema).model_validate_json(wire, strict=True, extra="ignore")
        except ValidationError as exc:
            raise IncompleteResponseError(f"上游 {field} 格式无效。") from exc
        public = model.model_dump(mode="json", by_alias=True, exclude_none=True, exclude_unset=True)
        if mldev and "citations" in public:
            public["citationSources"] = public.pop("citations")
        if any(public.values()):
            result[field] = public
    return result


def _check_span(span: dict) -> None:
    start, end = span.get("startIndex", 0), span.get("endIndex")
    if start < 0 or end is not None and end < start or span.get("partIndex", 0) < 0:
        raise IncompleteResponseError("上游引用范围无效。")


def _indices(grounding: dict) -> Iterator[int]:
    for support in grounding.get("groundingSupports", []):
        _check_span(support.get("segment", {}))
        yield from support.get("groundingChunkIndices", [])


def check_grounding_references(metadata: dict, *, final: bool) -> None:
    grounding = metadata.get("groundingMetadata", {})
    chunks = grounding.get("groundingChunks")
    for index in _indices(grounding):
        if index < 0 or final and (index >= len(chunks or ()) or not any(chunks[index].values())):
            raise IncompleteResponseError("上游引用索引没有对应的 groundingChunks 来源。")


def merge_candidate_metadata(previous: dict, update: dict) -> tuple[dict, int]:
    """Merge sparse field snapshots within this attempt, never shift source indices."""
    merged = dict(previous)
    for field, incoming in update.items():
        old = previous.get(field, {})
        if (field == "groundingMetadata" and "groundingChunks" in incoming
                and "groundingChunks" in old and "groundingSupports" not in incoming):
            sources = incoming["groundingChunks"]
            for index in _indices(old):
                # A separately delivered support may still await its source.
                if index < len(old["groundingChunks"]) and (
                        index >= len(sources) or old["groundingChunks"][index] != sources[index]):
                    raise IncompleteResponseError("上游更新了已引用的 groundingChunks，无法可靠关联来源。")
        if field == "citationMetadata":
            # Each list is a snapshot. Never combine different spellings from separate frames.
            merged[field] = incoming
            for citation in incoming.get("citations", incoming.get("citationSources", [])):
                _check_span(citation)
        else:
            merged[field] = {**old, **incoming}
    check_grounding_references(merged, final=False)
    return merged, len(_bounded_wire(merged))

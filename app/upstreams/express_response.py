"""SDK response-to-OpenAI conversion and SSE payload rendering.

Owns tool indices, part/signature topology and prefill adjustment. Upstream
client selection, network calls and retries remain in api_helpers.
"""

import asyncio
import json
import math
import time
import uuid
from typing import Any, Dict, Iterator, Optional

from google.genai.types import Candidate, Content, GenerateContentResponse, Part, SafetyRating

from message_processing import (
    GIANT_CONTENT_SLICE,
    GIANT_CONTENT_THRESHOLD,
    safety_ratings_presentation,
    iter_giant_content,
    logprobs_payload_from_candidate,
    sdk_ordinary_part_fields,
    parse_gemini_response_for_reasoning_and_content,
)
from prefill import PrefillDeduper, strip_prefill_overlap
from response_validation import (
    IncompleteResponseError, ResponsesMediaError, ResponsesTextCallOwner, StreamToolCallIds,
    map_finish_reason_to_openai,
    complete_function_arguments,
    require_responses_supported_part,
    responses_partial_call,
    sdk_function_call,
    sdk_media_payload,
)
from thought_signatures import (
    attach_signature_extra,
    PendingCallSignatures,
    publish_tool_call_signatures,
    stage_tool_call_id,
    ordinary_part_metadata,
    signature_bytes,
    thought_signature_extra,
)


def _nonblank(value: Any) -> bool:
    return isinstance(value, str) and bool(value) and not value.isspace()


# Executor admission inspects only bounded, already decoded SDK fields. In
# particular, GenerateContentResponse.text builds an aggregate of its Parts.
_PLAIN_RESPONSE_FIELDS = {"candidates", "usage_metadata", "sdk_http_response", "create_time",
                          "model_version", "response_id"}
_PLAIN_CANDIDATE_FIELDS = {"content", "finish_reason", "index", "token_count", "avg_logprobs",
                           "safety_ratings"}
_PLAIN_PART_FIELDS = {"text", "thought"}
_PLAIN_RESPONSE_PARTS = 32
_PLAIN_RESPONSE_CHARS = 32 * 1024


def completed_response_needs_media_pool(response: Any) -> bool:
    """Only proven small plain SDK replies may share the bounded text-prep pool."""
    if (type(response) is not GenerateContentResponse
            or not response.model_fields_set <= _PLAIN_RESPONSE_FIELDS
            or not response.candidates or len(response.candidates) > 4):
        return True
    parts_left, chars_left = _PLAIN_RESPONSE_PARTS, _PLAIN_RESPONSE_CHARS
    for candidate in response.candidates:
        if (type(candidate) is not Candidate or not candidate.model_fields_set <= _PLAIN_CANDIDATE_FIELDS
                or type(candidate.content) is not Content or not candidate.content.parts):
            return True
        # Optional normal safety scores can decorate bounded text. Richer
        # candidate metadata (notably logprobs) has already failed the allowlist.
        ratings = candidate.safety_ratings or ()
        if len(ratings) > 8 or any(type(r) is not SafetyRating or len(r.category or "") > 80
                                   or len(r.probability or "") > 80 for r in ratings):
            return True
        parts_left -= len(candidate.content.parts)
        if parts_left < 0:
            return True
        for part in candidate.content.parts:
            if (type(part) is not Part or not part.model_fields_set <= _PLAIN_PART_FIELDS
                    or part.thought or not isinstance(part.text, str)):
                return True
            chars_left -= len(part.text)
            if chars_left < 0:
                return True
    return False


def responses_sdk_source(response: GenerateContentResponse, private_text_name: str | None = None) -> GenerateContentResponse:
    """Gate the original typed candidate before Chat renders media or registers a call.

    A length/filter finish never authorizes an executable subset. Leave a private
    text-transport call to its own decoder; its arguments represent visible text.
    """
    candidates = getattr(response, "candidates", None) or ()
    if len(candidates) != 1:
        raise IncompleteResponseError("Upstream completion must contain one candidate.")
    candidate, = candidates
    parts = getattr(candidate.content, "parts", None) or ()
    finish = map_finish_reason_to_openai(candidate.finish_reason, default=None)
    incomplete = finish in ("length", "content_filter")
    kept = None
    owner = ResponsesTextCallOwner(private_text_name) if private_text_name else None
    for index, part in enumerate(parts):
        if sdk_media_payload(part) is not None:
            raise ResponsesMediaError()
        require_responses_supported_part(part)
        if incomplete and (fc := sdk_function_call(part)) is not None:
            fragmented = responses_partial_call(fc)
            if not (owner and owner.owns(fc.name, fc.id, fc.will_continue, fragmented)):
                # Validate the shape, never stage a public ID/signature for dropped data.
                if not fragmented:
                    complete_function_arguments(fc)
                if kept is None:
                    kept = parts[:index]
                continue
        if kept is not None:
            kept.append(part)
    if kept is None:
        return response
    content = candidate.content.model_copy(update={"parts": kept})
    return response.model_copy(update={"candidates": [candidate.model_copy(update={"content": content})]})


def is_gemini_response_valid(response: Any, *, allow_empty_length: bool = False) -> bool:
    if response is None:
        return False
    for cand in getattr(response, "candidates", None) or ():
        # Responses can carry an explicit token limit even with no content.
        finish = map_finish_reason_to_openai(getattr(cand, "finish_reason", None), default=None)
        if finish == "content_filter" or (allow_empty_length and finish == "length"):
            return True
        for part in getattr(getattr(cand, "content", None), "parts", None) or ():
            if (_nonblank(getattr(part, "text", None))
                    or sdk_function_call(part) is not None
                    or sdk_media_payload(part) is not None):
                return True
        if _nonblank(getattr(cand, "text", None)):
            return True
    # The SDK's response.text re-serializes every Part and concatenates the
    # first candidate. It cannot reveal text absent from the scan above.
    # Legacy wrappers (including subclasses) may provide independent text.
    return type(response) is not GenerateContentResponse and _nonblank(getattr(response, "text", None))


class ToolCallIndexer:
    """一次流式响应内，为 tool_calls 分配稳定递增的 index（P0-3）。

    OpenAI 客户端按 delta.tool_calls[].index 累积函数调用。旧实现把 index 硬编码成 0，
    多个并行调用会被前端合并成一个；同时旧实现遇到第一个 function_call 就 break，
    直接丢掉其余并行调用——而官方要求并行调用必须完整按
    FC1,FC2,FR1,FR2 的顺序回传，缺一个就 400。

    序号必须由调用方持有：并行调用可能分布在不同 chunk 里。
    每次重试都要新建一个实例（重试会重发整轮，序号需要归零）。
    """

    def __init__(self, keep_unsigned_parts: bool = True):
        self.keep_unsigned_parts = keep_unsigned_parts
        self._tool_ids = StreamToolCallIds()
        self._next: Dict[int, int] = {}
        self._seen_tool_calls: set[int] = set()
        self.signature_origins: dict = {}
        self._ordinary_parts: Dict[int, list] = {}
        self._part_order: Dict[int, list] = {}

    def next_index(self, candidate_index: int = 0, call_id: Optional[str] = None) -> int:
        self._tool_ids.observe(call_id, candidate_index)
        i = self._next.get(candidate_index, 0)
        self._next[candidate_index] = i + 1
        self._seen_tool_calls.add(candidate_index)
        return i

    def record_tool_call(self, candidate_index: int, tool_index: int) -> None:
        self._part_order.setdefault(candidate_index, []).append(
            {"type": "tool_call", "index": tool_index}
        )

    def record_ordinary_part(self, candidate_index: int, metadata: dict) -> int:
        items = self._ordinary_parts.setdefault(candidate_index, [])
        index = len(items)
        items.append(metadata)
        self._part_order.setdefault(candidate_index, []).append(
            {"type": "ordinary", "index": index}
        )
        return index

    def topology(self, candidate_index: int = 0) -> tuple[list, list]:
        return (
            list(self._ordinary_parts.get(candidate_index, [])),
            list(self._part_order.get(candidate_index, [])),
        )

    def has_tool_calls(self, candidate_index: int = 0) -> bool:
        return candidate_index in self._seen_tool_calls


def _build_chunk_payload(
    chunk: Any,
    model_name: str,
    response_id: str,
    candidate_index: int = 0,
    indexer: Optional[ToolCallIndexer] = None,
    *, structured_output: bool = False, created: Optional[int] = None,
) -> dict:
    """构造单条 OpenAI chunk 的 payload dict（不序列化）。

    热路径优化：真流式循环里每个 chunk 要做判空、预填充去重再输出，
    旧实现“dict→json.dumps→str→json.loads→dict→改→json.dumps→str”
    往返两次序列化；拆出本函数后判空/去重直接操作 dict，只在最终
    yield 时序列化一次（convert_chunk_to_openai 保留字符串返回，
    外部调用方与测试脚本行为不变）。
    """
    delta_payload = {}
    pending_signatures: PendingCallSignatures = []
    openai_finish_reason = None
    output_index = candidate_index

    if (
        hasattr(chunk, "candidates")
        and chunk.candidates
        and len(chunk.candidates) > candidate_index
    ):
        candidate = chunk.candidates[candidate_index]
        if getattr(candidate, "index", None) is not None:
            output_index = candidate.index
        raw_gemini_finish_reason = getattr(candidate, "finish_reason", None)
        openai_finish_reason = map_finish_reason_to_openai(
            raw_gemini_finish_reason, default=None
        )

        # Collect every parallel call and keep signatures on their exact tool-call
        # deltas. Message-level signatures cover ordinary/signature-only Parts.
        tool_call_deltas = []
        ordinary_metadata = []
        part_order = []
        ordinary_signature = None
        ordinary_signature_kind = None
        if (
            hasattr(candidate, "content")
            and hasattr(candidate.content, "parts")
            and candidate.content.parts
        ):
            for part in candidate.content.parts:
                fc = sdk_function_call(part)
                sig = signature_bytes(part, fc)
                if fc is None:
                    fields = sdk_ordinary_part_fields(part, sig)
                    if fields is None:
                        continue
                    kind, text_value = fields
                    metadata = ordinary_part_metadata(kind, text_value, sig, thought=getattr(part, "thought", None) is True)
                    # Restored private text has no upstream Part signature to
                    # replay. Repeating its entire accumulated text in metadata
                    # on every token would make a native stream quadratic.
                    if indexer and not indexer.keep_unsigned_parts and not sig:
                        continue
                    if indexer:
                        ordinary_index = indexer.record_ordinary_part(
                            output_index, metadata
                        )
                    else:
                        ordinary_index = len(ordinary_metadata)
                    part_order.append({"type": "ordinary", "index": ordinary_index})
                    ordinary_metadata.append(metadata)
                    if sig:
                        ordinary_signature = sig
                        ordinary_signature_kind = kind
                    continue
                arguments = complete_function_arguments(fc)
                tc_index = (
                    indexer.next_index(output_index, fc.id)
                    if indexer
                    else len(tool_call_deltas)
                )
                tc = {
                    "index": tc_index,
                    "id": stage_tool_call_id(pending_signatures, fc, sig, unsigned_follower=tc_index > 0),
                    "type": "function",
                    "function": {
                        "name": fc.name,
                        "arguments": json.dumps(arguments, ensure_ascii=False, allow_nan=False),
                    },
                }
                attach_signature_extra(tc, sig)
                part_order.append({"type": "tool_call", "index": tc_index})
                if indexer:
                    indexer.record_tool_call(output_index, tc_index)
                tool_call_deltas.append(tc)

        # Google often emits the functionCall in one chunk and a final STOP in a
        # later empty chunk. Normal completion after any call must remain
        # tool_calls; truncation and filtering
        # must survive so clients do not execute an incomplete tool request.
        if tool_call_deltas or (indexer and indexer.has_tool_calls(output_index)):
            openai_finish_reason = map_finish_reason_to_openai(
                raw_gemini_finish_reason, default=None, has_tool_calls=True
            )

        if tool_call_deltas:
            delta_payload["tool_calls"] = tool_call_deltas

        reasoning_text, normal_text = parse_gemini_response_for_reasoning_and_content(
            candidate
        )

        # Only append safety ratings to the final chunk.
        if openai_finish_reason:
            normal_text += safety_ratings_presentation(
                getattr(candidate, "safety_ratings", None), structured_output=structured_output)

        if reasoning_text:
            delta_payload["reasoning_content"] = reasoning_text
        if normal_text:
            delta_payload["content"] = normal_text
        elif tool_call_deltas:
            delta_payload["content"] = None
        elif not reasoning_text and openai_finish_reason is None:
            delta_payload["content"] = ""

        google_extra = {}
        if indexer and openai_finish_reason:
            metadata_to_emit, order_to_emit = indexer.topology(output_index)
        elif indexer:
            # A final snapshot preserves ordinary Parts/signatures for clients
            # which replace extension objects. Per-token cumulative snapshots
            # turn an otherwise linear stream (and its replay buffer) quadratic.
            metadata_to_emit, order_to_emit = [], []
        else:
            metadata_to_emit, order_to_emit = ordinary_metadata, part_order

        # Emit the complete topology only on the final candidate chunk.
        cumulative_signature = None
        cumulative_signature_kind = None
        for item in reversed(metadata_to_emit):
            if item.get("thought_signature"):
                cumulative_signature = item["thought_signature"]
                cumulative_signature_kind = item.get("kind")
                break
        if cumulative_signature:
            google_extra["thought_signature"] = cumulative_signature
            google_extra["thought_signature_part"] = cumulative_signature_kind
        elif ordinary_signature:
            google_extra.update(
                (
                    thought_signature_extra(ordinary_signature, ordinary_signature_kind)
                    or {}
                ).get("google", {})
            )
        if metadata_to_emit:
            google_extra["ordinary_parts"] = metadata_to_emit
        if order_to_emit:
            google_extra["part_order"] = order_to_emit
        if google_extra:
            delta_payload["extra_content"] = {"google": google_extra}

    if not delta_payload and openai_finish_reason is None:
        delta_payload["content"] = ""

    choice_item = {
        "index": output_index,
        "delta": delta_payload,
        "finish_reason": openai_finish_reason,
    }
    # 流式 logprobs：读真实字段 logprobs_result（旧实现误用 hasattr(candidate,
    # "logprobs") 恒 False），与 OpenAI 流式格式一致地挂在 choice 层级。
    logprobs_payload = (
        logprobs_payload_from_candidate(chunk.candidates[candidate_index])
        if (
            hasattr(chunk, "candidates")
            and chunk.candidates
            and len(chunk.candidates) > candidate_index
        )
        else None
    )
    if logprobs_payload is not None:
        choice_item["logprobs"] = logprobs_payload
    chunk_data = {
        "id": response_id,
        "object": "chat.completion.chunk",
        "created": int(time.time()) if created is None else created,
        "model": model_name,
        "choices": [choice_item],
    }
    # Only this complete, accepted chunk can supply replay fallback records.
    if pending_signatures:
        publish_tool_call_signatures(pending_signatures)
    return chunk_data


def convert_chunk_to_openai(
    chunk: Any,
    model_name: str,
    response_id: str,
    candidate_index: int = 0,
    indexer: Optional[ToolCallIndexer] = None,
    *, created: Optional[int] = None,
) -> str:
    """单条 Gemini chunk → OpenAI SSE 帧（对外保持字符串返回，兼容既有调用方）。"""
    return f"data: {json.dumps(_build_chunk_payload(chunk, model_name, response_id, candidate_index, indexer, created=created), ensure_ascii=False)}\n\n"


def _large_delta_fields(choice: dict) -> tuple[str, ...]:
    delta = choice.get("delta")
    if not isinstance(delta, dict):
        return ()
    reasoning, content = delta.get("reasoning_content"), delta.get("content")
    if isinstance(reasoning, str) and len(reasoning) > GIANT_CONTENT_THRESHOLD:
        # Carry ordinary visible text after the exposed reasoning, as the
        # fake-stream path does. All other delta metadata is still sent once.
        return ("reasoning_content", "content") if isinstance(content, str) else ("reasoning_content",)
    return ("content",) if isinstance(content, str) and len(content) > GIANT_CONTENT_THRESHOLD else ()


def iter_giant_chunk_payload(chunk_payload: dict) -> Iterator[dict]:
    """Split large true-stream content on demand; never stage unconsumed slices."""
    choices = chunk_payload.get("choices") if isinstance(chunk_payload, dict) else None
    if not isinstance(choices, (list, tuple)):
        yield chunk_payload
        return
    needs_split = False
    for choice in choices:
        if not isinstance(choice, dict):
            yield chunk_payload  # Unexpected envelope: never drop or partly replay it.
            return
        if _large_delta_fields(choice):
            needs_split = True
    if not needs_split:
        yield chunk_payload  # Normal chunks (including usage-only) keep identity.
        return

    # The SDK pipeline normally passes one choice per payload. Do not drop
    # sibling choices if the input is a combined OpenAI envelope.
    for position, choice in enumerate(choices):
        fields = _large_delta_fields(choice)
        if not fields:
            yield {**chunk_payload, "choices": [choice]}
            continue
        source = choice["delta"]
        first_delta = {k: v for k, v in source.items() if k not in fields}
        last_fields = {k: v for k, v in choice.items() if k not in ("index", "delta", "finish_reason")}
        for field in fields:
            text = source[field]
            last_piece = (len(text) - 1) // GIANT_CONTENT_SLICE if len(text) > GIANT_CONTENT_THRESHOLD else 0
            for i, piece in enumerate(iter_giant_content(text, threshold=GIANT_CONTENT_THRESHOLD, size=GIANT_CONTENT_SLICE)):
                delta = {**first_delta, field: piece} if field == fields[0] and i == 0 else {field: piece}
                last = field == fields[-1] and i == last_piece
                sub_choice = {"index": choice.get("index", position), "delta": delta,
                              "finish_reason": choice.get("finish_reason") if last else None}
                if last:
                    sub_choice.update(last_fields)
                yield {**chunk_payload, "choices": [sub_choice]}


def split_giant_chunk_payload(chunk_payload: dict) -> list[dict]:
    """Compatibility list API; the streaming pipeline uses the lazy iterator."""
    return list(iter_giant_chunk_payload(chunk_payload))


async def _chunk_openai_response_dict_for_sse(
    openai_response_dict: Dict[str, Any],
    response_id_override: Optional[str] = None,
    model_name_override: Optional[str] = None,
):
    created_time = openai_response_dict.get("created")
    if created_time is None:
        created_time = int(time.time())
    resp_id = (response_id_override or openai_response_dict.get("id")
               or f"chatcmpl-fakestream-{created_time}-{uuid.uuid4().hex[:12]}")
    model_name = model_name_override or openai_response_dict.get("model", "unknown")

    choices = openai_response_dict.get("choices", [])
    if not choices:
        yield f"data: {json.dumps({'id': resp_id, 'object': 'chat.completion.chunk', 'created': created_time, 'model': model_name, 'choices': [{'index': 0, 'delta': {}, 'finish_reason': 'error'}]}, ensure_ascii=False)}\n\n"
        yield "data: [DONE]\n\n"
        return

    # SDK stream accumulators index their choice arrays by choice.index. Keep
    # vendor IDs (zero is valid), but introduce choices in ascending order.
    indexed = ((pos if choice.get("index") is None else choice["index"], choice)
               for pos, choice in enumerate(choices))
    for choice_idx, choice in sorted(indexed, key=lambda pair: pair[0]):
        message = choice.get("message", {})
        final_finish_reason = choice.get("finish_reason", "stop")
        # 假流式首帧同样补 role:"assistant"，与真流式/Cookie 通道及 OpenAI
        # 官方帧序对齐。默认参数绑定避免闭包延迟绑定（B023）。
        first_delta_pending = {"v": True}

        def _sse(delta: dict, *, finish_reason: Optional[str] = None,
                 logprobs: Optional[dict] = None, _first: dict = first_delta_pending,
                 _choice_idx: int = choice_idx) -> str:
            if _first["v"]:
                delta = {"role": "assistant", **delta}
                _first["v"] = False
            item = {"index": _choice_idx, "delta": delta, "finish_reason": finish_reason}
            if logprobs is not None:
                item["logprobs"] = logprobs
            payload = {
                "id": resp_id,
                "object": "chat.completion.chunk",
                "created": created_time,
                "model": model_name,
                "choices": [item],
            }
            return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

        # A response may legitimately mix thoughts, visible text, tool calls and
        # message-level signature metadata in the same assistant turn. Emit each
        # independently instead of the old mutually-exclusive tool/text branches.
        if message.get("extra_content") is not None:
            yield _sse({"extra_content": message["extra_content"]})

        reasoning_content = message.get("reasoning_content", "")
        actual_content = message.get("content")
        if reasoning_content:
            pause = 0.0 if len(reasoning_content) > GIANT_CONTENT_THRESHOLD else 0.01
            for piece in iter_giant_content(reasoning_content):
                yield _sse({"reasoning_content": piece})
                await asyncio.sleep(pause)

        if actual_content is not None:
            content_to_chunk = actual_content
            # Keep generated images whole; chunk ordinary text for fake-stream UX.
            # 超大 content（4K 生图 base64）优先按内存分帧：单帧发射会在
            # json.dumps + f"data:..." + ASGI encode 同一瞬间再造三份全量
            # 拷贝，叠加转换链后 512MB 容器（Render 免费档）直接 OOMKill
            # ——进程级 SIGKILL，无 Python 异常，表现为“收到首个上游分片
            # 后服务崩溃重启”。切片拼接结果与单帧逐字节一致（见
            # message_processing.slice_giant_content）。
            #
            # 惰性发射（2026-11 第二轮）：旧实现 pieces = slice_giant_content(...)
            # 会先把全部切片物化成第二份全量拷贝（原文仍被
            # openai_response_dict 引用），发射阶段恒驻 2×content；改为
            # iter_giant_content 按需产片，每帧只在 yield 瞬间存在一个
            # 256KiB 切片，峰值降为 1×+256KiB。
            #
            # 步进节奏（2026-11 第三轮）：巨帧切片不再逐帧 sleep 0.01——
            # data URL 对前端不可增量渲染（收完才能显示），步进是纯延迟
            #（4K ≈52-160 帧 → 0.5-1.6s，2K ≈16 帧 → 160ms），且 Cookie
            # 通道同路径从不步进。改 sleep(0) 仅让出事件循环，慢客户端由
            # TCP 背压自然限速；文本假流式的 10 段节奏保持 0.01 不变。
            if len(content_to_chunk) > GIANT_CONTENT_THRESHOLD:
                pieces: Any = iter_giant_content(content_to_chunk)
                multi_frame = True
                frame_pause = 0.0
            elif "![Image](data:image/" in content_to_chunk:
                pieces = [content_to_chunk] if content_to_chunk else []
                multi_frame = False
                frame_pause = 0.01
            else:
                chunk_size = (
                    max(1, math.ceil(len(content_to_chunk) / 10))
                    if content_to_chunk
                    else 1
                )
                pieces = [
                    content_to_chunk[i : i + chunk_size]
                    for i in range(0, len(content_to_chunk), chunk_size)
                ]
                multi_frame = len(pieces) > 1
                frame_pause = 0.01
            if not content_to_chunk and not reasoning_content:
                yield _sse({"content": ""})
            else:
                for piece in pieces:
                    yield _sse({"content": piece})
                    if multi_frame:
                        await asyncio.sleep(frame_pause)

        for tc_item_idx, tool_call_item in enumerate(message.get("tool_calls") or []):
            tool_delta = {
                "index": tc_item_idx,
                "id": tool_call_item["id"],
                "type": "function",
                "function": {
                    "name": tool_call_item["function"]["name"],
                    "arguments": "",
                },
            }
            if tool_call_item.get("extra_content") is not None:
                tool_delta["extra_content"] = tool_call_item["extra_content"]
            yield _sse({"tool_calls": [tool_delta]})
            await asyncio.sleep(0.01)
            yield _sse(
                {
                    "tool_calls": [
                        {
                            "index": tc_item_idx,
                            "function": {
                                "arguments": tool_call_item["function"]["arguments"]
                            },
                        }
                    ]
                }
            )
            await asyncio.sleep(0.01)

        # The aggregate belongs to this choice, not every synthetic text slice.
        yield _sse({}, finish_reason=final_finish_reason, logprobs=choice.get("logprobs"))

    yield "data: [DONE]\n\n"


def _prepend_prefill(openai_dict: Dict[str, Any], prefill_text: str) -> Dict[str, Any]:
    """把预填充文本拼回到最终输出开头（预填充智能兼容用，带重叠去重）。"""
    if not prefill_text:
        return openai_dict
    try:
        # P2-3：n>1 时每个 choice 都要拼回预填充，旧实现处理完第一个就 break
        for choice in openai_dict.get("choices") or []:
            msg = choice.get("message")
            if not isinstance(msg, dict) or msg.get("tool_calls"):
                continue
            existing = msg.get("content") or ""
            msg["content"] = prefill_text + strip_prefill_overlap(
                prefill_text, existing
            )
    except Exception as e:
        # 预填充拼回失败静默丢弃会让「兼容模式突然失效」无从排查。
        print(f"⚠️ [预填充] 拼回响应失败（{e}），本条按原始输出返回。")
    return openai_dict


def _dedup_chunk_payload(
    payload: dict, deduper: PrefillDeduper, force_flush: bool = False
) -> Optional[dict]:
    """改写 delta.content 做预填充去重；返回 None 表示整条跳过。"""
    if deduper.done and not force_flush:
        return payload
    try:
        choices = payload.get("choices") or []
        if not choices:
            return payload
        choice = choices[0]
        delta = choice.get("delta") or {}
        content = delta.get("content")
        has_finish = bool(choice.get("finish_reason"))

        out = deduper.feed(content) if content else ""
        if has_finish or force_flush:
            out += deduper.flush()

        if out:
            delta["content"] = out
            choice["delta"] = delta
            return payload

        # 没有可放出的正文：若 chunk 还有其他信息（角色/思考/finish 等）则去掉 content 保留其余
        if content is not None:
            delta.pop("content", None)
        if delta or has_finish or "logprobs" in choice:
            choice["delta"] = delta
            return payload
        return None  # 纯 content 且被暂存 → 整条跳过
    except Exception as e:
        # 去重器状态一旦异常绝不能停留在未完成态继续吃缓冲：置为 done
        # 强制后续 chunk 原样透传（宁可重复，不可卡死或无界暂存），并
        # 留下可排查的日志。
        print(f"⚠️ [预填充去重] 处理异常（{e}），后续内容原样透传。")
        deduper.done = True
        return payload


class ChoicePrefills:
    """One independent prefill per actual upstream choice, scoped to this attempt."""

    def __init__(self, text: str, response_id: str, model: str, created: int) -> None:
        self.text = text
        self.envelope = {"id": response_id, "object": "chat.completion.chunk", "model": model, "created": created}
        self._dedupers: dict[int, PrefillDeduper] = {}
        self._sent: set[int] = set()

    def apply(self, payload: dict) -> Optional[dict]:
        choice = payload["choices"][0]
        index = choice["index"]
        if index not in self._dedupers:
            if not choice["delta"].get("content"):
                return payload
            self._dedupers[index] = PrefillDeduper(self.text)
        return _dedup_chunk_payload(payload, self._dedupers[index])

    def received_text(self, index: Optional[int] = None) -> bool:
        # A genuine upstream echo can be completely consumed by deduplication.
        # Never treat the configured prefix itself as evidence of a reply.
        return bool(self._dedupers) if index is None else index in self._dedupers

    def _frame(self, index: int, delta: dict) -> str:
        choice = {"index": index, "delta": delta, "finish_reason": None}
        payload = dict(self.envelope, choices=[choice])
        return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

    def first(self, index: int) -> Optional[str]:
        if index in self._sent:
            return None
        self._sent.add(index)
        return self._frame(index, {"role": "assistant", "content": self.text})

    def flush(self) -> Iterator[tuple[str, int]]:
        for index, deduper in self._dedupers.items():
            if tail := deduper.flush():
                if prefix := self.first(index):
                    yield prefix, 0
                yield self._frame(index, {"content": tail}), len(tail)

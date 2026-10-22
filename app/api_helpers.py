"""SDK 调用、响应转换与 OpenAI 流式输出编排。"""
import asyncio
import inspect
import json
import math
import time
import uuid
from contextlib import suppress
from typing import Any, Callable, Dict, List, Optional

import config as app_config
import model_capabilities as mc
from anti_truncation import TextTransport
from express_endpoint import is_location_pin_failure
from fastapi.responses import JSONResponse, StreamingResponse
from google.genai import types

# 引入报错重试统计器
from logger import stats
from message_processing import (
    _create_safety_ratings_html,
    convert_to_openai_format,
    logprobs_payload_from_candidate,
    parse_gemini_response_for_reasoning_and_content,
)
from models import OpenAIMessage, OpenAIRequest
from prefill import PrefillDeduper, strip_prefill_overlap
from response_validation import (
    IncompleteResponseError,
    PendingMetadataGate,
    has_finish_reason,
    map_finish_reason_to_openai,
    require_complete_response,
    require_unblocked_response,
)
from retry_policy import (
    exception_retry_delay,
    get_retry_settings,
    is_retryable_exception,
    wait_for_retry,
)
from runtime_state import app_state
from signature_store import SignatureState
from thought_signatures import (
    attach_signature_extra,
    build_tool_call_id,
    ordinary_part_metadata,
    signature_bytes,
    thought_signature_extra,
)
from upstream_errors import (
    create_openai_error_response,
    error_status,
    extract_upstream_error,
)
from usage import make_usage_chunk, map_usage, wants_usage
from streaming import disconnected as _request_disconnected
from request_tracking import (attempt_started_at, configure as track_configure,
                              upstream_attempt, upstream_chunk, record_usage)
from first_reply import FirstReplyGuard, FirstReplyTimeout, sdk_has_reply, timed_call, timeout_seconds


def _safety_score_enabled() -> bool:
    try:
        return bool(app_state.get_setting("safety_score", app_config.SAFETY_SCORE))
    except Exception:
        return bool(app_config.SAFETY_SCORE)


def _keepalive_frame(model_name: str) -> str:
    """假流式等待期间的心跳帧（空 delta chunk，前端按普通 chunk 忽略）。"""
    payload = {"id": "chatcmpl-keepalive", "object": "chat.completion.chunk",
               "created": int(time.time()), "model": model_name,
               "choices": [{"delta": {"content": ""}, "index": 0, "finish_reason": None}]}
    return f"data: {json.dumps(payload)}\n\n"


def _pin_failure_fallback(fallback_model: str, error: Exception, get_fallback_client):
    """location 钉定路径失败的统一退回（真流式与非流式共用同一句提示）。

    返回 (裸模型名, 普通客户端)；调用方需自行保证仅在尚未输出内容时才走到这里。
    """
    print(f"↩️ [上游端点] 钉定路径调用失败（{str(error)[:80]}），"
          f"已退回默认路由 {fallback_model} 重试一次。"
          "如持续出现，请确认「通道与凭证」里的 Project ID 属于该 API Key 且已开启计费，"
          "或把「标准模式 location」设为“默认（后端自选）”。")
    print("ℹ️ [流量等级] 回退默认路由已使用普通请求。")
    return fallback_model, get_fallback_client()


def _extract_usage(resp: Any) -> tuple[int, int, int]:
    """从 SDK 响应里取 (prompt, completion, total) token 数。"""
    usage = map_usage(getattr(resp, "usage_metadata", None))
    record_usage(usage)
    return usage["prompt_tokens"], usage["completion_tokens"], usage["total_tokens"]


def _record_usage(resp: Any) -> dict:
    """记录 token 用量，并打印上游实际返回的流量等级。

    返回完整 map_usage 结果（含 prompt_tokens_details / completion_tokens_details
    细分字段，供响应体透传——Claude 侧映射 cache_read_input_tokens 与
    output_tokens_details.thinking_tokens 依赖这两个细节）。
    """
    usage_metadata = getattr(resp, "usage_metadata", None)
    traffic_type = getattr(usage_metadata, "traffic_type", None) if usage_metadata else None
    if traffic_type:
        value = getattr(traffic_type, "value", None) or str(traffic_type)
        print(f"🚦 [流量等级] 上游实际 traffic_type={value}")

    usage = map_usage(usage_metadata)
    record_usage(usage)
    p_tk, c_tk = usage["prompt_tokens"], usage["completion_tokens"]
    stats.add_tokens(p_tk, c_tk)
    if p_tk or c_tk:
        print(f"💰 [算力消耗统计] 提示词: {p_tk} | 思考与生成: {c_tk} | 总计: {usage['total_tokens']} Tokens")
    return usage


def is_gemini_response_valid(response: Any) -> bool:
    if response is None: return False
    if hasattr(response, "text") and isinstance(response.text, str) and response.text.strip(): return True
    if hasattr(response, "candidates") and response.candidates:
        for cand in response.candidates:
            if hasattr(cand, "text") and isinstance(cand.text, str) and cand.text.strip(): return True
            if hasattr(cand, "content") and hasattr(cand.content, "parts") and cand.content.parts:
                for part in cand.content.parts:
                    if getattr(part, "function_call", None) is not None: return True
                    if getattr(part, "inline_data", None) is not None: return True
                    if hasattr(part, "text") and isinstance(getattr(part, "text", None), str) and getattr(part, "text", "").strip(): return True
    return False


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
        self._next: Dict[int, int] = {}
        self._seen_tool_calls: set[int] = set()
        self._ordinary_parts: Dict[int, list] = {}
        self._part_order: Dict[int, list] = {}

    def next_index(self, candidate_index: int = 0) -> int:
        i = self._next.get(candidate_index, 0)
        self._next[candidate_index] = i + 1
        self._seen_tool_calls.add(candidate_index)
        return i

    def record_tool_call(self, candidate_index: int, tool_index: int) -> None:
        self._part_order.setdefault(candidate_index, []).append(
            {"type": "tool_call", "index": tool_index})

    def record_ordinary_part(self, candidate_index: int, metadata: dict) -> int:
        items = self._ordinary_parts.setdefault(candidate_index, [])
        index = len(items)
        items.append(metadata)
        self._part_order.setdefault(candidate_index, []).append(
            {"type": "ordinary", "index": index})
        return index

    def topology(self, candidate_index: int = 0) -> tuple[list, list]:
        return (
            list(self._ordinary_parts.get(candidate_index, [])),
            list(self._part_order.get(candidate_index, [])),
        )

    def has_tool_calls(self, candidate_index: int = 0) -> bool:
        return candidate_index in self._seen_tool_calls


def _build_chunk_payload(chunk: Any, model_name: str, response_id: str, candidate_index: int = 0,
                        indexer: Optional[ToolCallIndexer] = None) -> dict:
    """构造单条 OpenAI chunk 的 payload dict（不序列化）。

    热路径优化：真流式循环里每个 chunk 要做判空、预填充去重再输出，
    旧实现“dict→json.dumps→str→json.loads→dict→改→json.dumps→str”
    往返两次序列化；拆出本函数后判空/去重直接操作 dict，只在最终
    yield 时序列化一次（convert_chunk_to_openai 保留字符串返回，
    外部调用方与测试脚本行为不变）。
    """
    delta_payload = {}
    openai_finish_reason = None
    output_index = candidate_index

    if hasattr(chunk, "candidates") and chunk.candidates and len(chunk.candidates) > candidate_index:
        candidate = chunk.candidates[candidate_index]
        if getattr(candidate, "index", None) is not None:
            output_index = candidate.index
        raw_gemini_finish_reason = getattr(candidate, "finish_reason", None)
        openai_finish_reason = map_finish_reason_to_openai(raw_gemini_finish_reason, default=None)

        # Collect every parallel call and keep signatures on their exact tool-call
        # deltas. Message-level signatures cover ordinary/signature-only Parts.
        tool_call_deltas = []
        ordinary_metadata = []
        part_order = []
        ordinary_signature = None
        ordinary_signature_kind = None
        if hasattr(candidate, "content") and hasattr(candidate.content, "parts") and candidate.content.parts:
            for part in candidate.content.parts:
                fc = getattr(part, "function_call", None)
                sig = signature_bytes(part, fc)
                if fc is None:
                    text_value = getattr(part, "text", None)
                    if text_value is None and getattr(part, "inline_data", None) is not None:
                        # 同 message_processing 非流式路径：ordinary_parts 的
                        # round-trip 消费端拒绝 data: 图片，再编码一遍 base64
                        # 只会把每个生图回合的响应体积翻倍（2026-10 修复）。
                        text_value = "[image]"
                    elif text_value is None and getattr(part, "file_data", None) is not None:
                        text_value = f"![Image]({part.file_data.file_uri})"
                    text_value = "" if text_value is None else str(text_value)
                    if getattr(part, "thought", None) is True:
                        kind = "thought"
                    elif text_value == "" and sig:
                        kind = "signature_only"
                    else:
                        kind = "text"
                    metadata = ordinary_part_metadata(kind, text_value, sig)
                    # Restored private text has no upstream Part signature to
                    # replay. Repeating its entire accumulated text in metadata
                    # on every token would make a native stream quadratic.
                    if indexer and not indexer.keep_unsigned_parts and not sig:
                        continue
                    if indexer:
                        ordinary_index = indexer.record_ordinary_part(output_index, metadata)
                    else:
                        ordinary_index = len(ordinary_metadata)
                    part_order.append({"type": "ordinary", "index": ordinary_index})
                    ordinary_metadata.append(metadata)
                    if sig:
                        ordinary_signature = sig
                        ordinary_signature_kind = kind
                    continue
                tc_index = indexer.next_index(output_index) if indexer else len(tool_call_deltas)
                tc = {
                    "index": tc_index,
                    "id": build_tool_call_id(
                        fc, part,
                        missing_state=(SignatureState.UNSIGNED_FOLLOWER
                                       if tc_index > 0 else SignatureState.UNKNOWN)),
                    "type": "function",
                    "function": {
                        "name": fc.name,
                        "arguments": json.dumps(fc.args) if fc.args is not None else "",
                    },
                }
                attach_signature_extra(tc, sig)
                part_order.append({"type": "tool_call", "index": tc_index})
                if indexer:
                    indexer.record_tool_call(output_index, tc_index)
                tool_call_deltas.append(tc)

        # Google often emits the functionCall in one chunk and a final STOP in a
        # later empty chunk. Once this candidate has produced any tool call, the
        # OpenAI finish reason for that turn must remain tool_calls.
        if (openai_finish_reason == "stop" and indexer
                and indexer.has_tool_calls(output_index)):
            openai_finish_reason = "tool_calls"

        if tool_call_deltas:
            delta_payload["tool_calls"] = tool_call_deltas

        reasoning_text, normal_text = parse_gemini_response_for_reasoning_and_content(candidate)

        # Only append safety ratings to the final chunk.
        if (openai_finish_reason and _safety_score_enabled()
                and hasattr(candidate, "safety_ratings") and candidate.safety_ratings):
            normal_text += _create_safety_ratings_html(candidate.safety_ratings)

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
            google_extra.update((thought_signature_extra(
                ordinary_signature, ordinary_signature_kind) or {}).get("google", {}))
        if metadata_to_emit:
            google_extra["ordinary_parts"] = metadata_to_emit
        if order_to_emit:
            google_extra["part_order"] = order_to_emit
        if google_extra:
            delta_payload["extra_content"] = {"google": google_extra}
    
    if not delta_payload and openai_finish_reason is None:
        delta_payload["content"] = ""

    choice_item = {"index": output_index, "delta": delta_payload, "finish_reason": openai_finish_reason}
    # 流式 logprobs：读真实字段 logprobs_result（旧实现误用 hasattr(candidate,
    # "logprobs") 恒 False），与 OpenAI 流式格式一致地挂在 choice 层级。
    logprobs_payload = logprobs_payload_from_candidate(
        chunk.candidates[candidate_index]) if (
        hasattr(chunk, "candidates") and chunk.candidates
        and len(chunk.candidates) > candidate_index) else None
    if logprobs_payload is not None:
        choice_item["logprobs"] = logprobs_payload
    chunk_data = {
        "id": response_id, "object": "chat.completion.chunk", "created": int(time.time()), "model": model_name,
        "choices": [choice_item]
    }
    return chunk_data


def convert_chunk_to_openai(chunk: Any, model_name: str, response_id: str, candidate_index: int = 0,
                            indexer: Optional[ToolCallIndexer] = None) -> str:
    """单条 Gemini chunk → OpenAI SSE 帧（对外保持字符串返回，兼容既有调用方）。"""
    return f"data: {json.dumps(_build_chunk_payload(chunk, model_name, response_id, candidate_index, indexer), ensure_ascii=False)}\n\n"

async def _chunk_openai_response_dict_for_sse(
    openai_response_dict: Dict[str, Any],
    response_id_override: Optional[str] = None, 
    model_name_override: Optional[str] = None
):
    resp_id = response_id_override or openai_response_dict.get("id", f"chatcmpl-fakestream-{int(time.time())}-{uuid.uuid4().hex[:12]}")
    model_name = model_name_override or openai_response_dict.get("model", "unknown")
    created_time = openai_response_dict.get("created", int(time.time()))
    
    choices = openai_response_dict.get("choices", [])
    if not choices: 
        yield f"data: {json.dumps({'id': resp_id, 'object': 'chat.completion.chunk', 'created': created_time, 'model': model_name, 'choices': [{'index': 0, 'delta': {}, 'finish_reason': 'error'}]}, ensure_ascii=False)}\n\n"
        yield "data: [DONE]\n\n"
        return

    for choice_idx, choice in enumerate(choices): 
        message = choice.get("message", {})
        final_finish_reason = choice.get("finish_reason", "stop")

        def _sse(delta):
            payload = {"id": resp_id, "object": "chat.completion.chunk", "created": created_time,
                       "model": model_name, "choices": [{"index": choice_idx, "delta": delta,
                                                           "finish_reason": None}]}
            return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

        # A response may legitimately mix thoughts, visible text, tool calls and
        # message-level signature metadata in the same assistant turn. Emit each
        # independently instead of the old mutually-exclusive tool/text branches.
        if message.get("extra_content") is not None:
            yield _sse({"extra_content": message["extra_content"]})

        reasoning_content = message.get("reasoning_content", "")
        actual_content = message.get("content")
        if reasoning_content:
            yield _sse({"reasoning_content": reasoning_content})
            await asyncio.sleep(0.01)

        if actual_content is not None:
            content_to_chunk = actual_content
            # Keep generated images whole; chunk ordinary text for fake-stream UX.
            if "![Image](data:image/" in content_to_chunk:
                chunk_size = max(1, len(content_to_chunk))
            else:
                chunk_size = max(1, math.ceil(len(content_to_chunk) / 10)) if content_to_chunk else 1
            if not content_to_chunk and not reasoning_content:
                yield _sse({"content": ""})
            else:
                for i in range(0, len(content_to_chunk), chunk_size):
                    yield _sse({"content": content_to_chunk[i:i + chunk_size]})
                    if len(content_to_chunk) > chunk_size:
                        await asyncio.sleep(0.01)

        for tc_item_idx, tool_call_item in enumerate(message.get("tool_calls") or []):
            tool_delta = {
                "index": tc_item_idx,
                "id": tool_call_item["id"],
                "type": "function",
                "function": {"name": tool_call_item["function"]["name"], "arguments": ""},
            }
            if tool_call_item.get("extra_content") is not None:
                tool_delta["extra_content"] = tool_call_item["extra_content"]
            yield _sse({"tool_calls": [tool_delta]})
            await asyncio.sleep(0.01)
            yield _sse({"tool_calls": [{
                "index": tc_item_idx,
                "id": tool_call_item["id"],
                "function": {"arguments": tool_call_item["function"]["arguments"]},
            }]})
            await asyncio.sleep(0.01)
        
        yield f"data: {json.dumps({'id': resp_id, 'object': 'chat.completion.chunk', 'created': created_time, 'model': model_name, 'choices': [{'index': choice_idx, 'delta': {}, 'finish_reason': final_finish_reason}]})}\n\n"

    yield "data: [DONE]\n\n"

def _prepend_prefill(openai_dict: Dict[str, Any], prefill_text: str) -> Dict[str, Any]:
    """把预填充文本拼回到最终输出开头（预填充智能兼容用，带重叠去重）。"""
    if not prefill_text:
        return openai_dict
    try:
        # P2-3：n>1 时每个 choice 都要拼回预填充，旧实现处理完第一个就 break
        for choice in (openai_dict.get("choices") or []):
            msg = choice.get("message")
            if not isinstance(msg, dict) or msg.get("tool_calls"):
                continue
            existing = msg.get("content") or ""
            msg["content"] = prefill_text + strip_prefill_overlap(prefill_text, existing)
    except Exception as e:
        # 预填充拼回失败静默丢弃会让「兼容模式突然失效」无从排查。
        print(f"⚠️ [预填充] 拼回响应失败（{e}），本条按原始输出返回。")
    return openai_dict


def _dedup_chunk_payload(payload: dict, deduper: PrefillDeduper, force_flush: bool = False) -> Optional[dict]:
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
        if delta or has_finish:
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


def _resolve_retry_target(current_client, current_model, current_fallback, target):
    client, model, fallback = target
    # 池化 SDK 客户端对应一把 Key；单 Key 重试保留本轮已采用的裸模型兜底，
    # 避免每次 429 都先重打已知无效的 projects/locations 路径。
    if client is current_client and current_model == current_fallback == fallback:
        model = current_model
    return client, model, fallback


async def gemini_fake_stream_generator(
    gemini_client_instance: Any,
    model_for_api_call: str,
    prompt_for_api_call: List[types.Content],
    gen_config_dict_for_api_call: Dict[str, Any],
    request_obj: OpenAIRequest,
    is_auto_attempt: bool,
    prefill_text: str = "",
    fastapi_request: Optional[Any] = None,
    fallback_model: Optional[str] = None,
    fallback_client_factory: Optional[Callable[[], Any]] = None,
    retry_target_factory: Optional[Callable[[], Any]] = None,
    text_transport: Optional[TextTransport] = None,
):
    print(f"🌊 [假流式] 已开始调用 Gemini 模型 {model_for_api_call}，客户端请求模型名为 {request_obj.model}。")

    # P1-6：不再使用 tenacity 的硬编码 20 次，改为与真流式/非流式一致的手写退避，
    # 读取控制台的 retry_max / retry_backoff_seconds / retry_jitter_seconds
    # （固定间隔 + 抖动，不随次数放大），并在等待期间检测客户端断开。
    max_retries, backoff_sec, jitter_sec = get_retry_settings()
    # 首回复窗口每请求只读一次设置：重试循环里每次 timed_call 都重复
    # 深拷贝整棵设置树（含全部 model_overrides），重试风暴下放大浪费。
    fr_seconds = timeout_seconds(request_obj.model)

    outer_keep_alive_interval = app_state.get_setting(
        "fake_streaming_interval", app_config.FAKE_STREAMING_INTERVAL_SECONDS)

    api_call_task = None
    raw_gemini_response = None
    last_error = None
    retry_pending = False
    heartbeat_due = 0.0

    try:
        attempt = 0
        while attempt <= max_retries:
            if await _request_disconnected(fastapi_request):
                print(f"ℹ️ [客户端断开] 假流式请求前检测到客户端已断开，停止调用模型 {model_for_api_call}。")
                return

            if retry_pending:
                if retry_target_factory:
                    target = retry_target_factory()
                    if inspect.isawaitable(target):
                        target = await target
                    if target is None:
                        raise last_error or RuntimeError("没有可用的 Express API Key。")
                    gemini_client_instance, model_for_api_call, fallback_model = _resolve_retry_target(
                        gemini_client_instance, model_for_api_call, fallback_model, target)
                stats.add_retry()
                retry_pending = False

            upstream_attempt()
            api_call_task = asyncio.create_task(
                timed_call(request_obj.model, gemini_client_instance.aio.models.generate_content(
                    model=model_for_api_call,
                    contents=prompt_for_api_call,
                    config=gen_config_dict_for_api_call,
                ), whole_call=True, seconds=fr_seconds)
            )

            # 等待期间持续吐 keep-alive，避免前端因长时间无字节而超时
            while not api_call_task.done():
                if outer_keep_alive_interval > 0 and time.monotonic() >= heartbeat_due:
                    yield _keepalive_frame(request_obj.model)
                    heartbeat_due = time.monotonic() + outer_keep_alive_interval
                await asyncio.wait({api_call_task}, timeout=0.5)
                if await _request_disconnected(fastapi_request):
                    print("ℹ️ [客户端断开] 假流式等待期间客户端已断开，正在取消上游任务。")
                    api_call_task.cancel()
                    return

            try:
                raw_gemini_response = await api_call_task
                upstream_chunk()
                break
            except asyncio.CancelledError:
                raise
            except Exception as e_call:
                last_error = e_call
                if (fallback_model and model_for_api_call != fallback_model
                        and is_location_pin_failure(e_call)):
                    model_for_api_call = fallback_model
                    if fallback_client_factory:
                        gemini_client_instance = fallback_client_factory()
                    print(f"↩️ [上游端点] 假流式钉定失败，退回裸模型名 {fallback_model} 重试。")
                    continue
                if is_retryable_exception(e_call) and attempt < max_retries:
                    wait_time = exception_retry_delay(e_call, backoff_sec, attempt, jitter_sec)
                    print(f"⚠️ [自动重试] 假流式上游繁忙（{e_call.__class__.__name__}），"
                          f"第 {attempt + 1} 次退避重试，等待 {wait_time} 秒。")
                    waited = 0.0
                    while waited < wait_time:
                        step = min(0.5, wait_time - waited)
                        await asyncio.sleep(step)
                        waited += step
                        if await _request_disconnected(fastapi_request):
                            print("ℹ️ [客户端断开] 假流式退避期间客户端已断开，停止重试。")
                            return
                        if outer_keep_alive_interval > 0 and time.monotonic() >= heartbeat_due:
                            yield _keepalive_frame(request_obj.model)
                            heartbeat_due = time.monotonic() + outer_keep_alive_interval
                    attempt += 1
                    retry_pending = True
                    continue
                raise

        if raw_gemini_response is None:
            raise last_error or ValueError("上游未返回任何响应（重试已耗尽）。")
        
        require_unblocked_response(raw_gemini_response)
        require_complete_response(raw_gemini_response)
        if text_transport:
            decoder = text_transport.decoder()
            raw_gemini_response = decoder.restore_sdk(raw_gemini_response)
            decoder.validate_end()
        if not is_gemini_response_valid(raw_gemini_response):
            raise ValueError("上游假流式响应无有效内容。")
        openai_response_dict = convert_to_openai_format(raw_gemini_response, request_obj.model)
        _prepend_prefill(openai_response_dict, prefill_text)
        _record_usage(raw_gemini_response)
        async for chunk_sse in _chunk_openai_response_dict_for_sse(
            openai_response_dict=openai_response_dict
        ):
            if chunk_sse.strip() == "data: [DONE]" and wants_usage(request_obj):
                yield make_usage_chunk(openai_response_dict["id"], request_obj.model,
                                       openai_response_dict.get("usage", {}))
            yield chunk_sse

    except asyncio.CancelledError:
        print(f"ℹ️ [客户端断开] 假流式响应期间客户端已断开，正在清理模型 {request_obj.model} 的后台任务。")
        if "api_call_task" in locals() and not api_call_task.done():
            api_call_task.cancel()
        raise
    except Exception as e_outer_gemini:
        err_msg_detail = f"Gemini 假流式生成器异常（模型：{request_obj.model}）：{type(e_outer_gemini).__name__} - {str(e_outer_gemini)}"
        print(f"❌ [API 错误响应] 假流发生器运行崩溃 (Model: {request_obj.model})。错误详情: {err_msg_detail}")
        sse_err_msg_display = str(e_outer_gemini)
        if len(sse_err_msg_display) > 512: sse_err_msg_display = sse_err_msg_display[:512] + "..."
        code, _ = extract_upstream_error(e_outer_gemini)
        err_resp_sse = create_openai_error_response(code, sse_err_msg_display, "server_error")
        json_payload_error = json.dumps(err_resp_sse)
        if not is_auto_attempt:
            # 流式响应已是 200，中间件按状态码计不到这次失败，这里补记
            # （auto 模式抛给外层通道接管，由外层统一计数，避免双计）
            stats.add_error()
            yield f"data: {json_payload_error}\n\n"
            yield "data: [DONE]\n\n"
        if is_auto_attempt: raise
    finally:
        if api_call_task is not None and not api_call_task.done():
            api_call_task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await api_call_task


async def execute_gemini_call(
    current_client: Any,
    model_to_call: str,
    prompt_func: Callable[[List[OpenAIMessage]], List[types.Content]],
    gen_config_dict: Dict[str, Any],
    request_obj: OpenAIRequest,
    is_auto_attempt: bool = False,
    fastapi_request: Optional[Any] = None,
    prefill_text: str = "",
    fallback_model: Optional[str] = None,
    fallback_client_factory: Optional[Callable[[], Any]] = None,
    retry_target_factory: Optional[Callable[[], Any]] = None,
    text_transport: Optional[TextTransport] = None,
):
    def _get_fallback_client():
        if fallback_client_factory is None:
            return current_client
        # 工厂本身走池化；不能缓存上一次 Key 的客户端供换 Key 后兜底使用。
        return fallback_client_factory()

    async def _select_retry_target() -> bool:
        nonlocal current_client, model_to_call, fallback_model
        if retry_target_factory is None:
            return True
        target = retry_target_factory()
        if inspect.isawaitable(target):
            target = await target
        if target is None:
            return False
        current_client, model_to_call, fallback_model = _resolve_retry_target(
            current_client, model_to_call, fallback_model, target)
        return True

    # P1-2：prompt 构建内部有远程图片下载与 PIL 压缩（同步阻塞），
    # 放到线程里执行，避免卡住整个事件循环。
    actual_prompt_for_call = await asyncio.to_thread(prompt_func, request_obj.messages)
    print(f"🚀 [上游请求] 正在调用 Agent Platform Express Mode 模型 {model_to_call}，客户端请求模型名为 {request_obj.model}。")

    if request_obj.stream:
        # 生图判定统一走能力档案（nano-banana-2.1 名字里不带 image，裸 "image" in 会漏判，
        # 漏判会绕开下面的生图假流式保护，走真流式）
        is_image_request = mc.get_profile(request_obj.model)["is_image"]

        if (app_state.get_setting("fake_streaming", app_config.FAKE_STREAMING_ENABLED) or is_image_request
                or (text_transport and text_transport.mode == "nonstream")):
            track_configure(mode="fake_stream", upstream_model=model_to_call,
                            thinking_level=(gen_config_dict.get("thinking_config") or {}).get("thinking_level"),
                            max_output_tokens=gen_config_dict.get("max_output_tokens"))
            if is_image_request:
                 print("🖼️ [生图保护] 图片模型请求已自动切换为假流式输出，以避免上游流式限制。")
            return StreamingResponse(
                gemini_fake_stream_generator(
                    current_client, model_to_call, actual_prompt_for_call,
                    gen_config_dict, request_obj, is_auto_attempt, prefill_text=prefill_text,
                    fastapi_request=fastapi_request,
                    fallback_model=fallback_model,
                    fallback_client_factory=fallback_client_factory,
                    retry_target_factory=retry_target_factory,
                    text_transport=text_transport,
                ), media_type="text/event-stream"
            )
        else: # True Streaming
            track_configure(mode="stream", upstream_model=model_to_call,
                            thinking_level=(gen_config_dict.get("thinking_config") or {}).get("thinking_level"),
                            include_thoughts=(gen_config_dict.get("thinking_config") or {}).get("include_thoughts"),
                            max_output_tokens=gen_config_dict.get("max_output_tokens"))
            response_id_for_stream = f"chatcmpl-realstream-{int(time.time())}-{uuid.uuid4().hex[:12]}"
            async def _gemini_real_stream_generator_inner():
                # 钉定失败要在这里改写模型名与客户端，必须声明 nonlocal。
                nonlocal model_to_call, current_client, fallback_model
                max_retries, backoff_sec, jitter_sec = get_retry_settings()
                fr_seconds = timeout_seconds(request_obj.model)
                has_yielded = False  # 是否已向客户端输出过内容
                thinking = gen_config_dict.get("thinking_config") or {}
                print(f"🌊 [流式模式] {request_obj.model}：真流式，"
                      f"思考档位={thinking.get('thinking_level', '默认')}，"
                      f"回传思考={thinking.get('include_thoughts', '默认')}，"
                      f"输出上限={gen_config_dict.get('max_output_tokens', '上游默认')}。")
                # 立即吐一个 SSE 心跳，尽快建立连接（429 重试期间也保活，防前端超时中断）
                yield ": keep-alive\n\n"
                # 总尝试次数 = retry_max + 1，retry_max=0 时仍会请求一次
                attempt = 0
                retry_pending = False
                last_error = None
                while attempt <= max_retries:
                    # 客户端断开则停止重试，避免无谓的上游调用
                    if await _request_disconnected(fastapi_request):
                        print(f"ℹ️ [客户端断开] 真流式请求前检测到客户端已断开，停止调用模型 {model_to_call}。")
                        return
                    stream_gen_obj = None
                    target_unavailable = False
                    attempt_started = time.monotonic()
                    try:
                        if retry_pending:
                            if not await _select_retry_target():
                                target_unavailable = True
                                raise last_error or RuntimeError("没有可用的 Express API Key。")
                            stats.add_retry()
                            retry_pending = False
                        upstream_attempt()
                        first_reply = FirstReplyGuard(request_obj.model, seconds=fr_seconds)
                        stream_gen_obj = await first_reply.wait(current_client.aio.models.generate_content_stream(
                            model=model_to_call,
                            contents=actual_prompt_for_call,
                            config=gen_config_dict
                        ))

                        # 预填充智能兼容：把预填充文本作为回复开头先发出（仅一次）；
                        # 同时启用流式去重器，模型若复述预填充开头会被自动裁掉（n>1 时不启用）。
                        # 每次 attempt 都重置 tool_calls 序号（重试会重发整轮）
                        tool_indexer = ToolCallIndexer(
                            keep_unsigned_parts=text_transport is None or len(request_obj.tools or []) > 1)
                        text_decoder = text_transport.decoder() if text_transport else None
                        deduper = PrefillDeduper(prefill_text) if (prefill_text and (request_obj.n or 1) == 1) else None
                        # 预填充先行帧：n>1 时为每个候选都发一份，与非流式
                        # _prepend_prefill 的全 choice 行为一致（旧实现只发
                        # choice 0，stream 与非 stream 同请求输出分叉）。
                        _pf_frames = [
                            {"id": response_id_for_stream, "object": "chat.completion.chunk",
                             "created": int(time.time()), "model": request_obj.model,
                             "choices": [{"index": ci, "delta": {"role": "assistant", "content": prefill_text},
                                          "finish_reason": None}]}
                            for ci in range(max(1, request_obj.n or 1))
                        ]

                        final_p_tk, final_c_tk, final_t_tk = 0, 0, 0
                        seen_candidates, finished_candidates = set(), set()
                        received_chunks = 0
                        content_chars, reasoning_chars, tool_deltas = 0, 0, 0
                        finish_reasons = set()
                        deferred_finishes = []
                        pending_metadata = PendingMetadataGate()

                        async for chunk_item_call in first_reply.iterate(stream_gen_obj, sdk_has_reply):
                            upstream_chunk()
                            received_chunks += 1
                            if received_chunks == 1:
                                # 归因到当前出口的尝试起点：多出口切换后
                                # attempt_started_at() 会重置，不把已失败出口
                                # 的等待时间累加进来（外层 attempt_started 只随
                                # 换 key 重试重置，口径是整次尝试总耗时）。
                                elapsed = time.monotonic() - (attempt_started_at() or attempt_started)
                                print(f"📥 [首个上游分片] {request_obj.model}：第 {attempt + 1} 次尝试等待 {elapsed:.1f} 秒收到分片；分片可能仅含思考或用量。")
                            if getattr(chunk_item_call, "usage_metadata", None):
                                final_p_tk, final_c_tk, final_t_tk = _extract_usage(chunk_item_call)

                            # 支持 n>1：按候选序号逐个输出
                            require_unblocked_response(chunk_item_call)
                            for ci, candidate in enumerate(chunk_item_call.candidates or []):
                                index = candidate.index if candidate.index is not None else ci
                                seen_candidates.add(index)
                                if has_finish_reason(candidate.finish_reason):
                                    finished_candidates.add(index)
                            if text_decoder:
                                chunk_item_call = text_decoder.restore_sdk(chunk_item_call)
                            num_candidates = len(getattr(chunk_item_call, "candidates", None) or [])
                            for ci in range(num_candidates):
                                chunk_payload = _build_chunk_payload(
                                    chunk_item_call, request_obj.model, response_id_for_stream, ci,
                                    indexer=tool_indexer)
                                if deduper is not None:
                                    chunk_payload = _dedup_chunk_payload(chunk_payload, deduper)
                                    if chunk_payload is None:
                                        continue  # 正文暂存于去重器，跳过空 chunk
                                choice = (chunk_payload.get("choices") or [{}])[0]
                                delta = choice.get("delta") or {}
                                if delta.get("content"):
                                    if not content_chars:
                                        elapsed = time.monotonic() - (attempt_started_at() or attempt_started)
                                        print(f"✍️ [首段正文] {request_obj.model}：第 {attempt + 1} 次尝试等待 {elapsed:.1f} 秒开始输出正文。")
                                    content_chars += len(delta["content"])
                                reasoning_chars += len(delta.get("reasoning_content") or "")
                                tool_deltas += len(delta.get("tool_calls") or [])
                                if choice.get("finish_reason"):
                                    finish_reasons.add(choice["finish_reason"])
                                signature = ((delta.get("extra_content") or {}).get("google") or {}).get("thought_signature")
                                if not (delta.get("content") or delta.get("reasoning_content")
                                        or delta.get("tool_calls") or signature or choice.get("finish_reason")):
                                    continue
                                if choice.get("finish_reason") and not (content_chars or reasoning_chars or tool_deltas):
                                    # 有些前端收到 finish_reason 就结束读取；先校验空响应，
                                    # 避免成功结束帧遮住后续的明确错误。
                                    deferred_finishes.append(chunk_payload)
                                    continue
                                if not first_reply.ready:
                                    # 序列化一次复用：测量与最终下发共用同一份字符串
                                    # （旧实现每个元数据 chunk 序列化两次）。
                                    frame_text = json.dumps(chunk_payload, ensure_ascii=False)
                                    pending_metadata.add(frame_text, len(frame_text.encode("utf-8")))
                                    continue
                                for metadata in pending_metadata.drain():
                                    yield f"data: {metadata}\n\n"
                                if prefill_text and not has_yielded:
                                    for _pf_chunk in _pf_frames:
                                        yield f"data: {json.dumps(_pf_chunk, ensure_ascii=False)}\n\n"
                                has_yielded = True
                                yield f"data: {json.dumps(chunk_payload, ensure_ascii=False)}\n\n"

                        if text_decoder:
                            text_decoder.validate_end()
                        if seen_candidates - finished_candidates:
                            raise IncompleteResponseError("上游流式回复缺少候选结束标记，内容可能不完整。")
                        # 去重器可能还攒着开头文本（上游没发 finish chunk 的场景）
                        if deduper is not None and not deduper.done:
                            tail = deduper.flush()
                            if tail:
                                if prefill_text and not has_yielded:
                                    for _pf_chunk in _pf_frames:
                                        yield f"data: {json.dumps(_pf_chunk, ensure_ascii=False)}\n\n"
                                has_yielded = True
                                content_chars += len(tail)
                                _tail = {"id": response_id_for_stream, "object": "chat.completion.chunk", "created": int(time.time()), "model": request_obj.model, "choices": [{"index": 0, "delta": {"content": tail}, "finish_reason": None}]}
                                yield f"data: {json.dumps(_tail, ensure_ascii=False)}\n\n"

                        if final_p_tk > 0 or final_c_tk > 0:
                            print(f"💰 [算力消耗统计] 提示词: {final_p_tk} | 思考与生成: {final_c_tk} | 总计: {final_t_tk} Tokens")
                        reasons = ','.join(sorted(finish_reasons)) or '无'
                        print(f"📊 [流式完成] {request_obj.model}：正文 {content_chars} 字符，思考 {reasoning_chars} 字符，"
                              f"工具增量 {tool_deltas}，结束原因={reasons}。")
                        # 结束标记或签名单独到达也会设置 has_yielded，但它们不是回复内容。
                        # 安全拦截允许空正文；只有思考的合法响应保留，同时明确提示诊断。
                        if not (content_chars or reasoning_chars or tool_deltas or 'content_filter' in finish_reasons):
                            hint = "请降低思考档位或提高输出 token 上限。" if 'length' in finish_reasons else "请检查上游响应与生成参数。"
                            raise IncompleteResponseError(f"上游流式响应没有正文、思考或工具调用（结束原因：{reasons}）。{hint}")
                        if reasoning_chars and not (content_chars or tool_deltas):
                            print(f"⚠️ [空正文] {request_obj.model}：上游仅返回思考，客户端可能显示为空。"
                                  f"结束原因={reasons}；请检查思考档位和输出 token 上限。")
                        stats.add_tokens(final_p_tk, final_c_tk)
                        for payload in deferred_finishes:
                            yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

                        # P1-8：Express 真流式此前从不发 usage 块，客户端只能显示 0
                        if wants_usage(request_obj):
                            yield make_usage_chunk(response_id_for_stream, request_obj.model, {
                                "prompt_tokens": final_p_tk,
                                "completion_tokens": final_c_tk,
                                "total_tokens": final_t_tk,
                            })

                        yield "data: [DONE]\n\n"
                        return

                    except asyncio.CancelledError:
                        print(f"ℹ️ [客户端断开] 真流式响应期间客户端已断开，模型 {model_to_call} 的请求已安全终止。")
                        raise
                    except Exception as e_stream_call:
                        last_error = e_stream_call
                        close = getattr(stream_gen_obj, "aclose", None)
                        if close:
                            with suppress(Exception):
                                await close()
                        stream_gen_obj = None
                        is_retryable = is_retryable_exception(e_stream_call)

                        # Older endpoints can reject the preview argument-stream
                        # field. Remove only that field, once, before any output.
                        fcc = gen_config_dict.get("tool_config", {}).get("function_calling_config", {})
                        if (not has_yielded and fcc.get("stream_function_call_arguments")
                                and error_status(e_stream_call) == 400
                                and any(term in str(e_stream_call).lower().replace("_", "")
                                        for term in ("streamfunctioncallarguments", "partialargs"))):
                            fcc.pop("stream_function_call_arguments", None)
                            print("↩️ [抗截断] 上游不支持增量参数流，改用完整函数参数还原。")
                            continue

                        # location 钉定路径不对 → 退回裸模型名再试一次（仅在还没输出内容时）
                        if (fallback_model and model_to_call != fallback_model
                                and not has_yielded and is_location_pin_failure(e_stream_call)):
                            model_to_call, current_client = _pin_failure_fallback(
                                fallback_model, e_stream_call, _get_fallback_client)
                            continue

                        # 关键修复：只有在“尚未向客户端输出任何内容”时才重试；
                        # 否则重试会导致整段答案重复输出（前半段 + 完整重发）。
                        if is_retryable and not has_yielded and not target_unavailable and attempt < max_retries:
                            # F-5：退避时长改读控制台的 retry_backoff_seconds，
                            # 原先硬编码 2**(attempt%4)，控制台设置对真流式完全没作用。
                            wait_time = exception_retry_delay(e_stream_call, backoff_sec, attempt, jitter_sec)
                            code, _ = extract_upstream_error(e_stream_call)
                            elapsed = time.monotonic() - attempt_started
                            # 首回复超时等有完整中文说明的异常直接展示 message，
                            # 与 429 重试日志一致可读，不再只报英文类名。
                            detail = (str(e_stream_call).rstrip("。") if isinstance(e_stream_call, FirstReplyTimeout)
                                      else type(e_stream_call).__name__)
                            print(f"⚠️ [自动重试] Agent Platform Express Mode 流式请求失败（HTTP {code}，"
                                  f"{detail}，本次已等待 {elapsed:.1f} 秒）。"
                                  f"第 {attempt + 1} 次退避重试，等待 {wait_time:.1f} 秒。")
                            if await _request_disconnected(fastapi_request):
                                print(f"ℹ️ [客户端断开] 重试前检测到客户端已断开，停止调用模型 {model_to_call}。")
                                return
                            # 退避等待期间持续吐 SSE 心跳，保活前端连接
                            _waited = 0.0
                            while _waited < wait_time:
                                step = min(3.0, wait_time - _waited)
                                if not await wait_for_retry(fastapi_request, step):
                                    print(f"ℹ️ [客户端断开] 重试等待期间检测到客户端已断开，停止调用模型 {model_to_call}。")
                                    return
                                _waited += step
                                yield ": keep-alive\n\n"
                            attempt += 1
                            retry_pending = True
                            continue

                        err_msg_detail_stream = f"Gemini 流式请求异常（模型：{model_to_call}）：{type(e_stream_call).__name__} - {str(e_stream_call)}"
                        print(f"❌ [API 错误响应] 流式连接异常中断 (Model: {model_to_call})。错误详情: {err_msg_detail_stream}")
                        s_err = str(e_stream_call); s_err = s_err[:1024]+"..." if len(s_err)>1024 else s_err
                        if is_auto_attempt:
                            raise e_stream_call
                        # 流式响应已是 200，中间件按状态码计不到这次失败，
                        # 这里补记（auto 模式抛给外层通道接管，由外层计数）
                        stats.add_error()
                        # 已有内容时不重试，但必须告知失败，不能把半截答案伪装成正常结束。
                        code, _ = extract_upstream_error(e_stream_call)
                        err_resp = create_openai_error_response(code, s_err, "server_error")
                        yield f"data: {json.dumps(err_resp)}\n\n"
                        yield "data: [DONE]\n\n"
                        return
                    finally:
                        close = getattr(stream_gen_obj, "aclose", None)
                        if close:
                            with suppress(Exception):
                                await close()

            return StreamingResponse(_gemini_real_stream_generator_inner(), media_type="text/event-stream")
    else: # Non-streaming
        track_configure(mode="json", upstream_model=model_to_call)
        # 手动退避重试循环（替代 tenacity），以便在每次重试前检测客户端断开
        max_retries, backoff_sec, jitter_sec = get_retry_settings()
        fr_seconds = timeout_seconds(request_obj.model)
        response_obj_call = None
        # 总尝试次数 = retry_max + 1，retry_max=0 时仍会请求一次
        attempt = 0
        retry_pending = False
        last_error = None
        while attempt <= max_retries:
            if await _request_disconnected(fastapi_request):
                print(f"ℹ️ [客户端断开] 非流式请求前检测到客户端已断开，停止调用模型 {model_to_call}。")
                return JSONResponse(
                    status_code=499,
                    content=create_openai_error_response(499, "客户端已断开连接，请求已取消。", "client_closed_request"),
                )
            target_unavailable = False
            try:
                if retry_pending:
                    if not await _select_retry_target():
                        target_unavailable = True
                        raise last_error or RuntimeError("没有可用的 Express API Key。")
                    stats.add_retry()
                    retry_pending = False
                upstream_attempt()
                response_obj_call = await timed_call(request_obj.model, current_client.aio.models.generate_content(
                    model=model_to_call,
                    contents=actual_prompt_for_call,
                    config=gen_config_dict,
                ), whole_call=True, seconds=fr_seconds)
                upstream_chunk()
                break
            except asyncio.CancelledError:
                print(f"ℹ️ [客户端断开] 非流式响应期间客户端已断开，模型 {model_to_call} 的请求已安全终止。")
                raise
            except Exception as e_call:
                last_error = e_call
                if (fallback_model and model_to_call != fallback_model
                        and is_location_pin_failure(e_call)):
                    model_to_call, current_client = _pin_failure_fallback(
                        fallback_model, e_call, _get_fallback_client)
                    continue
                if is_retryable_exception(e_call) and not target_unavailable and attempt < max_retries:
                    wait_time = exception_retry_delay(e_call, backoff_sec, attempt, jitter_sec)
                    detail = str(e_call).rstrip("。") if isinstance(e_call, FirstReplyTimeout) else e_call.__class__.__name__
                    print(f"⚠️ [自动重试] 上游繁忙或触发配额限制（{detail}）。第 {attempt + 1} 次退避重试，等待 {wait_time} 秒。")
                    if not await wait_for_retry(fastapi_request, wait_time):
                        return JSONResponse(status_code=499, content=create_openai_error_response(
                            499, "客户端已断开连接，请求已取消。", "client_closed_request"))
                    attempt += 1
                    retry_pending = True
                    continue
                raise

        # 兜底：绝不让 None 流到下游的有效性检查里变成一条误导性的“无有效内容”
        if response_obj_call is None:
            print(f"❌ [上游无响应] 模型 {model_to_call} 在 {max_retries + 1} 次尝试后仍未返回任何响应。")
            return JSONResponse(
                status_code=502,
                content=create_openai_error_response(
                    502, "上游未返回任何响应（重试已耗尽）。", "upstream_error"),
            )

        require_unblocked_response(response_obj_call)
        require_complete_response(response_obj_call)
        if text_transport:
            decoder = text_transport.decoder()
            response_obj_call = decoder.restore_sdk(response_obj_call)
            decoder.validate_end()
        if not is_gemini_response_valid(response_obj_call):
            error_details = f"Agent Platform 非流式响应无有效内容，模型：{model_to_call}。"
            if hasattr(response_obj_call, "candidates"):
                candidates = response_obj_call.candidates or []
                error_details += f"Candidates: {len(candidates)}. "
                if candidates:
                    candidate = candidates[0]
                    if hasattr(candidate, "content"):
                        error_details += "Has content. "
                        parts = getattr(candidate.content, "parts", None) or []
                        if hasattr(candidate.content, "parts"):
                            error_details += f"Parts: {len(parts)}. "
                            if parts:
                                part = parts[0]
                                if getattr(part, "function_call", None) is not None:
                                    error_details += f"First part is function_call: {part.function_call.name}"
                                elif hasattr(part, "text"):
                                    text_preview = str(getattr(part, "text", ""))[:100]
                                    error_details += f"First part text: '{text_preview}'"
            else:
                error_details += f"Response type: {type(response_obj_call).__name__}"
            raise ValueError(error_details)

        _record_usage(response_obj_call)

        openai_response_content = convert_to_openai_format(response_obj_call, request_obj.model)
        _prepend_prefill(openai_response_content, prefill_text)
        return JSONResponse(content=openai_response_content)

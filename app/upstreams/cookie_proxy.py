"""Cookie 通道调用编排：认证、重试、协议降级与响应交付。

请求构造由 cookie_request 负责，响应解析由 cookie_response 负责。
"""

import asyncio
import json
import time
import traceback
import uuid
from dataclasses import dataclass
from typing import Any, Dict

import config as app_config
import httpx
import model_capabilities as mc
from anti_truncation import (
    TextTransport,
    buffered_sse,
    prepare_transport,
    split_model_alias,
)
from cookie_auth import BATCH_GRAPHQL_URL, build_headers
from credential_manager import credential_manager
from fastapi import Request
from fastapi.responses import JSONResponse, StreamingResponse
from first_reply import timed_call, timeout_seconds
from impersonate import open_upstream_client
from logger import stats
from message_processing import _create_safety_ratings_html
from models import OpenAIRequest
from prefill import PrefillDeduper, strip_prefill_overlap
from proxy_failover import ProxyChain
from request_tracking import (
    configure as track_configure,
)
from request_tracking import (
    upstream_attempt,
    upstream_chunk,
)
from request_validation import validate_openai_request
from channel_capabilities import cookie_feature_error
from response_validation import (
    FINISH_CONTENT_FILTER,
    AntiTruncationError,
    IncompleteResponseError,
    map_finish_reason_to_openai,
    require_finish_reason,
)
from retry_policy import (
    get_retry_settings,
    is_retryable_exception,
    is_retryable_status,
    retry_delay,
    wait_for_retry,
)
from runtime_state import app_state
from schema_validation import SchemaValidationError
from streaming import HEARTBEAT, HEARTBEAT_FRAME, with_heartbeats
from thought_signatures import (
    ordinary_part_metadata,
    thought_signature_extra,
)
from upstream_errors import create_openai_error_response, extract_upstream_error
from upstreams.base import BaseUpstream
from upstreams.cookie_request import (
    build_request_body_async,
    classify_tool_traffic,
    is_forced_builtin_search,
    search_suffix_has_priority,
)
from upstreams.cookie_response import (
    RawSampler,
    TextSource,
    apply_wire_signature,
    extract_events,
    iter_json_objects,
    make_openai_chunk,
    openai_tool_call,
)
from upstreams.cookie_transport import (
    api_error_details,
    is_native_tool_error_response,
    retry_heartbeats,
    stream_events,
)
from upstreams.request_preparation import prepare_messages
from usage import (
    estimate_usage,
    make_usage_chunk,
    map_usage,
    merge_usage_metadata,
    messages_prompt_text,
    wants_usage,
)

_THINKING_RUNAWAY_HINT = (
    "这通常是原生思考在高强度下跑飞或被截断（前端如 SillyTavern 常发 reasoning_effort=xhigh，"
    "会覆盖控制台档位）。解决：控制台“模型参数→思考强度→原生思考控制”选“关闭原生思考”"
    "（可用“保存为该模型专属”只对本模型生效），即忽略前端强度、压到 minimal 并剥离原生思考。"
)


_NO_BODY_HINT = "可能被安全策略拦截或接口行为变化；原始响应样本已写入运行日志，可到大盘“运行日志”页查看。"


def _no_body_detail(finish_raw, blocked_msg) -> str:
    """无正文诊断前缀：结束原因 + promptFeedback 拦截说明（三条路径共用）。"""
    detail = f"finishReason={finish_raw or '无'}"
    if blocked_msg:
        detail += f"；promptFeedback 拦截：{blocked_msg}"
    return detail


def _no_body_desc_hint(has_thought: bool, strip_thoughts: bool) -> tuple:
    """无正文时的 (描述, 处置提示)：只有思考 → 跑飞提示，完全为空 → 通用提示。"""
    desc = "只返回了思考、未返回正文" if has_thought else "未返回任何内容"
    return desc, (
        _THINKING_RUNAWAY_HINT
        if (has_thought and not strip_thoughts)
        else _NO_BODY_HINT
    )


def _no_body_outcome(
    finish_raw, blocked_msg, has_thought: bool, strip_thoughts: bool
) -> tuple:
    """无正文收尾的统一四元组（流式/非流式共用）：

    (日志描述, 诊断detail, 客户端提示文案, finish_reason)。
    content_filter 判定与提示文案在两条交付路径上必须一字不差。
    """
    detail = _no_body_detail(finish_raw, blocked_msg)
    desc, hint = _no_body_desc_hint(has_thought, strip_thoughts)
    filtered = bool(blocked_msg) or (
        (finish_raw or "").upper() in FINISH_CONTENT_FILTER
    )
    finish = "content_filter" if filtered else "stop"
    notice = f"[Studio 提示] 上游{desc}（{detail}）。{hint}"
    return desc, detail, notice, finish


class _RequestBodyCache:
    """batchGraphql 请求体的「原生 / 降级」两形态缓存（流式与非流式共用）。

    构建内部含远程图片下载 + PIL 压缩，旧实现每次重试都重建 = 反复下载
    同一批图片；重试发同一个 body 本就是重试语义。降级形态惰性构建：
    原生函数 Schema 首次被拒才建，同时把降级模式锁定到后续重试。
    """

    def __init__(
        self,
        project_id,
        base_model_name,
        request_obj,
        *,
        prefill_active: bool,
        force_search: bool,
    ):
        self._args = (project_id, base_model_name, request_obj)
        self._prefill_active = prefill_active
        self._force_search = force_search
        self._cache: Dict[str, dict] = {}
        self.native_latched = False

    async def get(self, native: bool) -> dict:
        key = "native" if native else "degraded"
        if key not in self._cache:
            self._cache[key] = await build_request_body_async(
                *self._args,
                prefill_active=self._prefill_active,
                force_search=self._force_search,
                native_tools=native,
            )
        return self._cache[key]

    async def degraded(self) -> dict:
        """取降级 body，并把本次及后续重试锁定为降级模式。"""
        self.native_latched = True
        return await self.get(False)


def _safety_html_if_enabled(ratings: Any) -> str:
    """开关打开时把 batchGraphql 的 safetyRatings 渲染成附加块，否则空串。"""
    if not ratings:
        return ""
    if not app_state.get_setting("safety_score", app_config.SAFETY_SCORE):
        return ""
    return _create_safety_ratings_html(ratings)


def _get_cookie_string() -> str:
    return app_state.get_google_cookie() or ""


def _final_usage(
    usage_meta, request_obj, completion_text: str, reasoning_text: str = ""
) -> dict:
    """上游真实计量优先；未回传且 usage_estimation 开启时给字符级粗估。

    粗估附带 "estimated": true；生图（正文为 base64 data URL）不适用，调用方不走本函数。
    """
    usage = map_usage(usage_meta)
    if usage_meta is None and app_state.get_setting("usage_estimation", False):
        prompt = messages_prompt_text(getattr(request_obj, "messages", None) or [])
        estimated = estimate_usage(
            prompt, (completion_text or "") + "\n" + (reasoning_text or "")
        )
        if estimated["total_tokens"]:
            return estimated
    return usage


def _get_project_id() -> str:
    return app_state.get_project_id() or ""


async def _collect_full_response(
    project_id,
    base_model_name,
    request_obj,
    cookie_str,
    client_kwargs,
    retry_max,
    backoff_sec,
    jitter_sec,
    fastapi_request,
    *,
    prefill_active=False,
    force_search=False,
) -> dict:
    """非流式地完整取回一次响应（供生图假流式复用）。返回 dict。"""
    # 请求体只构建一次：构建内部含远程图片下载 + PIL 压缩，旧实现每次
    # 重试都重建一遍 = 反复下载同一批图片。重试发同一个 body 本就是重试语义。
    if await fastapi_request.is_disconnected():
        return {"kind": "error", "message": "客户端已断开连接。"}
    body = await build_request_body_async(
        project_id,
        base_model_name,
        request_obj,
        prefill_active=prefill_active,
        force_search=force_search,
    )
    fr_seconds = timeout_seconds(base_model_name)
    for attempt in range(retry_max + 1):
        if await fastapi_request.is_disconnected():
            return {"kind": "error", "message": "客户端已断开连接。"}
        if attempt:
            stats.add_retry()
        try:
            # 兼容旧调用形态：本参数历史上是现成的 headers dict（测试桩直传
            # {}），按旧口径原样放行；新形态传 cookie 字符串，按次重算认证头。
            if isinstance(cookie_str, dict):
                req_headers = build_headers(cookie_str.get("cookie", "")) or cookie_str
            else:
                req_headers = build_headers(cookie_str)
                if not req_headers:
                    return {
                        "kind": "error",
                        "code": 401,
                        "message": "Cookie 中未找到 SAPISID，无法计算认证头。",
                    }
            async with open_upstream_client(client_kwargs) as client:
                upstream_attempt()
                # 整取响应（生图假流式路径）：首回复窗口按整调用预算放宽，
                # 否则生图 30-120s 会在默认 15s 窗口下永远超时。
                response = await timed_call(
                    base_model_name,
                    client.post(BATCH_GRAPHQL_URL, headers=req_headers, json=body),
                    whole_call=True,
                    seconds=fr_seconds,
                )

            if is_retryable_status(response.status_code) and attempt < retry_max:
                delay = retry_delay(
                    getattr(response, "headers", {}).get("retry-after"),
                    backoff_sec,
                    attempt,
                    jitter_sec,
                    error=f"HTTP {response.status_code}: {response.text[:200]}",
                )
                print(
                    f"⚠️ [Studio] HTTP {response.status_code}（生图，尝试 {attempt + 1}）: {response.text[:300]} | {delay:.1f}s 后重试..."
                )
                if not await wait_for_retry(fastapi_request, delay):
                    return {"kind": "error", "message": "客户端已断开连接。"}
                continue
            if response.status_code != 200:
                return {
                    "kind": "error",
                    "code": response.status_code,
                    "message": f"HTTP {response.status_code}: {response.text[:300]}",
                }

            full_text, reasoning_text, api_error, usage_meta = "", "", None, None
            api_error_status = 502
            finish_raw, blocked_msg = None, None
            safety_html = ""
            sampler = RawSampler()

            async for obj in iter_json_objects(TextSource(response.text)):
                sampler.add(obj)
                # 一旦拿到错误事件就停止解析剩余响应：生图响应可达数 MB base64，
                # 错误分支的返回值不依赖后续对象，继续拼接是纯浪费。
                if api_error:
                    break
                events = list(extract_events(obj))
                # 与流式路径同口径：空分片/纯错误分片不计入「收到首个上游分片」。
                if any(et != "error" for et, _ in events):
                    upstream_chunk()
                for et, data in events:
                    if et == "text":
                        full_text += data
                    elif et == "thought":
                        reasoning_text += data
                    elif et == "image":
                        full_text += data
                    elif et == "finish":
                        finish_raw = data
                    elif et == "safety":
                        safety_html = _safety_html_if_enabled(data)
                    elif et == "blocked":
                        blocked_msg = data
                    elif et == "usage":
                        usage_meta = merge_usage_metadata(usage_meta, data)
                    elif et == "error":
                        api_error_status, api_error = api_error_details(data)
                        break

            if (
                api_error
                and is_retryable_status(api_error_status)
                and attempt < retry_max
            ):
                delay = retry_delay(
                    None, backoff_sec, attempt, jitter_sec, error=api_error
                )
                print(
                    f"⚠️ [Studio] 生图 429/限流（尝试 {attempt + 1}）: {api_error[:300]} | {delay:.1f}s 后重试..."
                )
                if not await wait_for_retry(fastapi_request, delay):
                    return {"kind": "error", "message": "客户端已断开连接。"}
                continue
            if api_error:
                return {"kind": "error", "code": api_error_status, "message": api_error}

            if not full_text.strip():
                # 无正文：给出可见的诊断信息，而不是静默空回复
                detail = _no_body_detail(finish_raw, blocked_msg)
                print(
                    f"🔎 [Studio 诊断] 生图无正文（{detail}）。原始响应样本：\n{sampler.dump()}"
                )
                return {
                    "kind": "error",
                    "message": f"上游未返回图片/正文（{detail}）。已在日志记录原始响应样本。",
                }

            require_finish_reason(finish_raw)
            return {
                "kind": "ok",
                "full_text": full_text + safety_html,
                "reasoning_text": reasoning_text,
                "finish_reason": map_finish_reason_to_openai(finish_raw),
                "usage_meta": usage_meta,
            }
        except Exception as e:
            em = str(e)
            if is_retryable_exception(e) and attempt < retry_max:
                delay = retry_delay(None, backoff_sec, attempt, jitter_sec, error=em)
                print(
                    f"⚠️ [Studio] 生图异常（尝试 {attempt + 1}）：{em[:300]} | {delay:.1f}s 后重试..."
                )
                if not await wait_for_retry(fastapi_request, delay):
                    return {"kind": "error", "message": "客户端已断开连接。"}
                continue
            code, _ = extract_upstream_error(e)
            return {
                "kind": "error",
                "code": code,
                "message": f"batchGraphql proxy error: {em}",
            }
    return {"kind": "error", "message": "重试多次仍失败。"}


@dataclass(kw_only=True)
class CookieGeneration:
    """One request's credential snapshot and explicit delivery state.

    No state is shared across requests. Each delivery method owns and closes
    its upstream iterator/task; retries retain the same credentials and body cache.
    """

    request_obj: OpenAIRequest
    fastapi_request: Request
    record: dict | None
    account_id: str
    cookie_str: str
    project_id: str
    base_model_name: str
    model_display: str
    response_id: str
    client_kwargs: dict
    retry_max: int
    backoff_sec: float
    jitter_sec: float
    start_time: float
    prefill_text: str
    prefill_active: bool
    force_builtin_search: bool
    native_custom_tools: bool
    text_transport: TextTransport | None
    strip_thoughts: bool
    want_usage: bool
    cookie_debug: bool
    is_stream: bool
    is_image: bool

    async def response(self):
        if self.is_stream and self.is_image:
            return StreamingResponse(
                self.image_frames(), media_type="text/event-stream"
            )
        if self.is_stream:
            return StreamingResponse(
                self.stream_frames(), media_type="text/event-stream"
            )
        if self.request_obj.stream:
            return StreamingResponse(
                self.buffered_frames(), media_type="text/event-stream"
            )
        return await self.collect_response()

    async def image_frames(self):
        if await self.fastapi_request.is_disconnected():
            return
        print(
            f"🖼️ [生图保护] 图片模型 {self.base_model_name} 已自动切换为假流式输出（Cookie 通道），避免分块 base64 卡死前端。"
        )
        # 生图和重试等待期间也持续发心跳，避免前端或中间层按空闲超时断开。
        # 首心跳行为与 stream_frames / buffered_frames 保持一致。
        yield HEARTBEAT_FRAME
        collect_task = asyncio.create_task(
            _collect_full_response(
                self.project_id,
                self.base_model_name,
                self.request_obj,
                self.cookie_str,
                self.client_kwargs,
                self.retry_max,
                self.backoff_sec,
                self.jitter_sec,
                self.fastapi_request,
                prefill_active=self.prefill_active,
                force_search=self.force_builtin_search,
            )
        )
        try:
            while True:
                done, _pending = await asyncio.wait({collect_task}, timeout=10.0)
                if done:
                    break
                if await self.fastapi_request.is_disconnected():
                    print("ℹ️ [Studio] 客户端已断开连接，取消生图收集。")
                    return
                yield HEARTBEAT_FRAME
            try:
                res = collect_task.result()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                res = {
                    "kind": "error",
                    "code": 502,
                    "message": f"batchGraphql proxy error: {type(e).__name__}: {e}",
                }
            yield make_openai_chunk(
                self.response_id, self.model_display, role="assistant"
            )
            if res.get("kind") != "ok":
                stats.add_error()
                print(f"❌ [Studio] 生图失败 | {res.get('message', '')[:300]}")
                error = create_openai_error_response(
                    res.get("code", 502),
                    res.get("message", "生图失败"),
                    "upstream_error",
                )
                yield "data: " + json.dumps(error, ensure_ascii=False) + "\n\n"
                yield "data: [DONE]\n\n"
                return
            full_text = res.get("full_text") or ""
            if self.prefill_text:
                full_text = self.prefill_text + strip_prefill_overlap(
                    self.prefill_text, full_text
                )
            if res.get("reasoning_text") and not self.strip_thoughts:
                yield make_openai_chunk(
                    self.response_id,
                    self.model_display,
                    reasoning_content=res["reasoning_text"],
                )
            # 关键：整张图作为单个 chunk 发出，绝不分块
            yield make_openai_chunk(
                self.response_id, self.model_display, content=full_text or " "
            )
            stats.add_success()
            credential_manager.record_cookie_success(
                self.account_id, credential=self.record
            )
            # 生图正文是 base64 data URL，字符估算失真极大，不做 usage 估算
            usage = map_usage(res.get("usage_meta"))
            if self.want_usage:
                yield make_usage_chunk(self.response_id, self.model_display, usage)
            yield make_openai_chunk(
                self.response_id,
                self.model_display,
                finish_reason=res.get("finish_reason", "stop"),
            )
            yield "data: [DONE]\n\n"
            print(f"✅ [Studio] {self.base_model_name} | 生图假流式完成")
        finally:
            # 生成器被提前关闭（断连/停机/缓冲淘汰都会 aclose 本生成器，
            # GeneratorExit 打进 asyncio.wait 的挂起点）时必须收尸：
            # 孤儿任务会带着完整重试循环（retry_max × 退避）继续打上游，
            # 浪费配额且此时并发名额已归还，完全绕过 upstream_max_concurrency。
            if not collect_task.done():
                collect_task.cancel()
            await asyncio.gather(collect_task, return_exceptions=True)

    async def _stream_events(self):

        body_cache = _RequestBodyCache(
            self.project_id,
            self.base_model_name,
            self.request_obj,
            prefill_active=self.prefill_active,
            force_search=self.force_builtin_search,
        )

        for attempt in range(self.retry_max + 1):
            # 客户端已断开则立即停止，避免无谓的上游调用与重试
            if await self.fastapi_request.is_disconnected():
                print("ℹ️ [Studio] 客户端已断开连接，停止流式重试。")
                return
            if attempt:
                stats.add_retry()

            use_native_tools = not body_cache.native_latched
            body = await body_cache.get(use_native_tools)
            # 降级 body 惰性构建（原生 Schema 首次被拒才建）；
            # degraded() 会把降级模式锁定到后续重试。
            fallback_body = (
                body_cache.degraded
                if self.native_custom_tools
                and use_native_tools
                and not self.text_transport
                else None
            )
            req_headers = build_headers(self.cookie_str)
            if self.cookie_debug:
                print(
                    f"🔎 [Studio 调试] 出站 generationConfig: "
                    f"{json.dumps(body['variables'].get('generationConfig', {}), ensure_ascii=False)}"
                )

            async with open_upstream_client(self.client_kwargs) as client:
                has_yielded_to_client = False
                got_text = False  # 是否收到过“正文”（text/image）
                got_thought = False  # 是否收到过思考
                got_tool_call = False
                tool_call_index = 0
                stream_ordinary_metadata = []
                stream_part_order = []
                should_retry = False
                error_to_raise = None
                usage_meta = None
                finish_raw = None
                blocked_msg = None
                sampler = RawSampler()
                deduper = PrefillDeduper(self.prefill_text)
                pending_safety_html = ""
                stream_tool_headers = set()
                stream_tool_ids = set()

                # 消费实时生成器（原始事件 → 此处格式化为 OpenAI chunk）
                events = stream_events(
                    client,
                    req_headers,
                    body,
                    sampler=sampler,
                    fallback_body=fallback_body,
                    model=self.base_model_name,
                    emit_tool_deltas=self.native_custom_tools and not self.text_transport,
                )
                if self.text_transport:
                    events = self.text_transport.decoder().cookie_stream(events)
                try:
                    async for status, data in events:
                        if status == "transport_error":
                            stats.add_error()
                            print(f"❌ [抗截断] {data}")
                            yield (
                                "data: "
                                + json.dumps(
                                    {
                                        "error": {
                                            "code": 502,
                                            "message": data,
                                            "type": "upstream_error",
                                        }
                                    },
                                    ensure_ascii=False,
                                )
                                + "\n\n"
                            )
                            yield "data: [DONE]\n\n"
                            return
                        # 思考剥离：batchGraphql 忽略 includeThoughts，这里按解析出的配置主动丢弃思考块
                        if status == "thought":
                            got_thought = True
                            if self.strip_thoughts:
                                continue  # 不建连、不输出，等真正的正文

                        if status in ("tool_delta", "tool_end"):
                            if not has_yielded_to_client:
                                print(f"⚡ [Studio] {self.base_model_name} | 连接建立，正在实时流式输出...")
                                yield make_openai_chunk(self.response_id, self.model_display, role="assistant")
                                has_yielded_to_client = True
                            index = data["index"]
                            if index not in stream_tool_headers:
                                stream_tool_headers.add(index)
                                stream_part_order.append({"type": "tool_call", "index": index})
                                tool_call_index = max(tool_call_index, index + 1)
                            tool_payload = {"index": index}
                            if status == "tool_delta":
                                tool_payload["function"] = {"arguments": data.get("arguments", "")}
                                if data.get("name"):
                                    tool_payload["type"] = "function"
                                    tool_payload["function"]["name"] = data["name"]
                                if data.get("id"):
                                    tool_payload["id"] = data["id"]
                                    stream_tool_ids.add(index)
                            else:
                                complete = openai_tool_call(self.response_id, index, data, include_arguments=False)
                                if index not in stream_tool_ids:
                                    tool_payload["id"] = complete["id"]
                                    stream_tool_ids.add(index)
                                if complete.get("extra_content"):
                                    tool_payload["extra_content"] = complete["extra_content"]
                                got_tool_call = True
                            if len(tool_payload) > 1:
                                yield make_openai_chunk(self.response_id, self.model_display, tool_calls=[tool_payload])

                        elif status == "function_call":
                            if not has_yielded_to_client:
                                print(
                                    f"⚡ [Studio] {self.base_model_name} | 连接建立，正在实时流式输出..."
                                )
                                yield make_openai_chunk(
                                    self.response_id,
                                    self.model_display,
                                    role="assistant",
                                )
                                has_yielded_to_client = True
                            tool_payload = openai_tool_call(
                                self.response_id, tool_call_index, data
                            )
                            tool_payload["index"] = tool_call_index
                            stream_part_order.append(
                                {"type": "tool_call", "index": tool_call_index}
                            )
                            yield make_openai_chunk(
                                self.response_id,
                                self.model_display,
                                tool_calls=[tool_payload],
                            )
                            tool_call_index += 1
                            got_tool_call = True

                        elif status == "thought_signature":
                            signature = apply_wire_signature(
                                data, stream_ordinary_metadata, stream_part_order
                            )
                            extra = thought_signature_extra(
                                signature, data.get("part_kind")
                            )
                            if extra:
                                google = extra.setdefault("google", {})
                                if stream_ordinary_metadata:
                                    google["ordinary_parts"] = list(
                                        stream_ordinary_metadata
                                    )
                                if got_tool_call:
                                    google["part_order"] = list(stream_part_order)
                                if not has_yielded_to_client:
                                    yield make_openai_chunk(
                                        self.response_id,
                                        self.model_display,
                                        role="assistant",
                                    )
                                    has_yielded_to_client = True
                                yield make_openai_chunk(
                                    self.response_id,
                                    self.model_display,
                                    extra_content=extra,
                                )

                        elif status in ("text", "thought", "image", "api_error_text"):
                            if not has_yielded_to_client:
                                print(
                                    f"⚡ [Studio] {self.base_model_name} | 连接建立，正在实时流式输出..."
                                )
                                yield make_openai_chunk(
                                    self.response_id,
                                    self.model_display,
                                    role="assistant",
                                )
                                has_yielded_to_client = True
                                # 预填充智能兼容：先把预填充文本作为回复开头发出
                                if self.prefill_text:
                                    yield make_openai_chunk(
                                        self.response_id,
                                        self.model_display,
                                        content=self.prefill_text,
                                    )

                            if status == "thought":
                                stream_part_order.append(
                                    {
                                        "type": "ordinary",
                                        "index": len(stream_ordinary_metadata),
                                    }
                                )
                                stream_ordinary_metadata.append(
                                    ordinary_part_metadata("thought", data, None)
                                )
                                yield make_openai_chunk(
                                    self.response_id,
                                    self.model_display,
                                    reasoning_content=data,
                                )
                            elif status == "api_error_text":
                                stats.add_error()
                                yield make_openai_chunk(
                                    self.response_id,
                                    self.model_display,
                                    content=f"\n[Studio API 错误] {data}",
                                )
                                yield (
                                    "data: "
                                    + json.dumps(
                                        {
                                            "error": {
                                                "code": 502,
                                                "message": data,
                                                "type": "upstream_error",
                                            }
                                        },
                                        ensure_ascii=False,
                                    )
                                    + "\n\n"
                                )
                                yield "data: [DONE]\n\n"
                                return
                            elif status == "image":
                                got_text = True
                                stream_part_order.append(
                                    {
                                        "type": "ordinary",
                                        "index": len(stream_ordinary_metadata),
                                    }
                                )
                                stream_ordinary_metadata.append(
                                    ordinary_part_metadata("image", "", None)
                                )
                                yield make_openai_chunk(
                                    self.response_id, self.model_display, content=data
                                )
                            else:  # text（经过预填充去重器）
                                got_text = True
                                out = deduper.feed(data)
                                if out:
                                    stream_part_order.append(
                                        {
                                            "type": "ordinary",
                                            "index": len(stream_ordinary_metadata),
                                        }
                                    )
                                    stream_ordinary_metadata.append(
                                        ordinary_part_metadata("text", out, None)
                                    )
                                    yield make_openai_chunk(
                                        self.response_id,
                                        self.model_display,
                                        content=out,
                                    )

                        elif status == "finish":
                            finish_raw = data
                        elif status == "safety":
                            # 安全分与 finish 同块到达（最后一块）：先暂存，
                            # 等去重器 flush 完尾段再输出。旧实现在循环内即时
                            # yield，预填充 + 短回复场景下会插在去重器攒住的
                            # 正文尾段**之前**，把安全块嵌进正文中间（2026-10 修复）。
                            pending_safety_html = _safety_html_if_enabled(data)
                        elif status == "usage":
                            usage_meta = merge_usage_metadata(usage_meta, data)
                        elif status == "blocked":
                            blocked_msg = data

                        elif status == "cookie_error":
                            stats.add_error()
                            credential_manager.record_cookie_auth_failure(
                                self.account_id, credential=self.record
                            )
                            code = getattr(data, "status_code", None) or 403
                            error = create_openai_error_response(
                                code, str(data)[:1024], "upstream_error"
                            )
                            yield (
                                "data: "
                                + json.dumps(error, ensure_ascii=False)
                                + "\n\n"
                            )
                            yield "data: [DONE]\n\n"
                            return

                        elif status in ("retryable_error", "fatal_error"):
                            if (
                                not has_yielded_to_client
                                and status == "retryable_error"
                                and attempt < self.retry_max
                            ):
                                should_retry = True
                                error_to_raise = data
                                break
                            stats.add_error()
                            default_code = 429 if status == "retryable_error" else 502
                            code = getattr(data, "status_code", None) or default_code
                            error = create_openai_error_response(
                                code, str(data)[:1024], "upstream_error"
                            )
                            print(f"❌ [Studio] HTTP {code} | {str(data)[:300]}")
                            yield (
                                "data: "
                                + json.dumps(error, ensure_ascii=False)
                                + "\n\n"
                            )
                            yield "data: [DONE]\n\n"
                            return
                finally:
                    await events.aclose()

                # 如果标记了需要重试，在当前 attempt 结束时等待并开启下一次循环
                if should_retry:
                    # 重试前再次确认客户端仍在
                    if await self.fastapi_request.is_disconnected():
                        print("ℹ️ [Studio] 客户端已断开连接，取消后续重试。")
                        return
                    wait_sec = retry_delay(
                        getattr(error_to_raise, "retry_after", None),
                        self.backoff_sec,
                        attempt,
                        self.jitter_sec,
                        error=str(error_to_raise),
                    )
                    print(
                        f"⚠️ [Studio] 遇到可重试拥堵/限流: {str(error_to_raise)[:300]} | {wait_sec}s 后进行第 {attempt + 2} 次退避重试"
                    )
                    # 退避等待期间持续吐心跳，保活前端连接（issue4：3.1-pro 频繁 429 长等待易被前端断开）
                    async for _hb in retry_heartbeats(
                        wait_sec, request=self.fastapi_request
                    ):
                        if await self.fastapi_request.is_disconnected():
                            print("ℹ️ [Studio] 客户端已断开连接，取消后续重试。")
                            return
                        yield _hb
                    self.start_time = time.time()
                    continue

                # ===== 正常收尾（修复旧版“空响应静默关流”，并显式暴露“只有思考没有正文”）=====
                if not has_yielded_to_client:
                    yield make_openai_chunk(
                        self.response_id, self.model_display, role="assistant"
                    )
                    has_yielded_to_client = True
                    if self.prefill_text:
                        yield make_openai_chunk(
                            self.response_id,
                            self.model_display,
                            content=self.prefill_text,
                        )

                # 预填充去重器里可能还攒着开头的一小段（短回复场景）
                tail = deduper.flush()
                if tail:
                    stream_part_order.append(
                        {"type": "ordinary", "index": len(stream_ordinary_metadata)}
                    )
                    stream_ordinary_metadata.append(
                        ordinary_part_metadata("text", tail, None)
                    )
                    yield make_openai_chunk(
                        self.response_id, self.model_display, content=tail
                    )

                # 安全分在正文（含去重尾段）之后、结束帧之前输出，
                # 与非流式路径 content_value = full_text + safety_html 的顺序一致。
                if pending_safety_html:
                    yield make_openai_chunk(
                        self.response_id,
                        self.model_display,
                        content=pending_safety_html,
                    )

                if got_text or got_tool_call:
                    stats.add_success()
                    credential_manager.record_cookie_success(
                        self.account_id, credential=self.record
                    )
                    if got_tool_call and stream_ordinary_metadata:
                        google = {
                            "ordinary_parts": list(stream_ordinary_metadata),
                            "part_order": list(stream_part_order),
                        }
                        last_signed = next(
                            (
                                item
                                for item in reversed(stream_ordinary_metadata)
                                if item.get("thought_signature")
                            ),
                            None,
                        )
                        if last_signed:
                            google["thought_signature"] = last_signed[
                                "thought_signature"
                            ]
                            google["thought_signature_part"] = last_signed["kind"]
                        yield make_openai_chunk(
                            self.response_id,
                            self.model_display,
                            extra_content={"google": google},
                        )
                    yield make_openai_chunk(
                        self.response_id,
                        self.model_display,
                        finish_reason=map_finish_reason_to_openai(
                            finish_raw, has_tool_calls=got_tool_call
                        ),
                    )
                else:
                    # 无正文：明确告知客户端 + 落诊断日志（旧版此处一个字节都不发就关流）
                    desc, detail, notice, no_body_finish = _no_body_outcome(
                        finish_raw, blocked_msg, got_thought, self.strip_thoughts
                    )
                    stats.add_error()
                    print(
                        f"❌ [Studio] {self.base_model_name} | 上游{desc}（{detail}）"
                    )
                    print(f"🔎 [Studio 诊断] 原始响应样本：\n{sampler.dump()}")
                    yield make_openai_chunk(
                        self.response_id, self.model_display, content=f"\n{notice}"
                    )
                    yield make_openai_chunk(
                        self.response_id,
                        self.model_display,
                        finish_reason=no_body_finish,
                    )

                usage = _final_usage(
                    usage_meta,
                    self.request_obj,
                    "\n".join(
                        part.get("text", "") for part in stream_ordinary_metadata
                    ),
                )
                if self.want_usage:
                    yield make_usage_chunk(self.response_id, self.model_display, usage)

                yield "data: [DONE]\n\n"

                elapsed = time.time() - self.start_time
                if got_text or got_tool_call:
                    print(
                        f"✅ [Studio] {self.base_model_name} | 流式传输顺利完毕 | 耗时 {elapsed:.1f}s"
                    )
                return

    async def stream_frames(self):
        # 立即吐一个心跳，尽快建立连接（避免首个上游调用较慢/429 时前端久等无字节）
        yield HEARTBEAT_FRAME
        try:
            async for frame in self._stream_events():
                yield frame
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # 准备阶段（请求体构建 / 客户端池获取）的异常旧实现直接抛出
            # 生成器：SSE 已 200 建流后连接被拦腰掐断，客户端拿不到任何
            # 错误帧只能干等到超时。这里统一以错误帧 + [DONE] 收尾，
            # 与「半截答案不当成功、异常必须显式返回」的设计原则一致。
            stats.add_error()
            code, message = extract_upstream_error(e)
            print(
                f"❌ [Studio] {self.base_model_name} | 流式响应意外中断 | HTTP {code} | {str(e)[:150]}"
            )
            error = create_openai_error_response(code, message[:1024], "upstream_error")
            yield "data: " + json.dumps(error, ensure_ascii=False) + "\n\n"
            yield "data: [DONE]\n\n"

    async def collect_response(self):
        # 请求体两形态各自只构建一次（同流式路径：防重试反复下载图片）
        body_cache = _RequestBodyCache(
            self.project_id,
            self.base_model_name,
            self.request_obj,
            prefill_active=self.prefill_active,
            force_search=self.force_builtin_search,
        )

        # 非流式整取：首回复窗口按整调用预算放宽（生图/长思考）。
        fr_seconds = timeout_seconds(self.base_model_name)
        for attempt in range(self.retry_max + 1):
            # 客户端已断开则立即停止，避免无谓的上游调用与重试
            if await self.fastapi_request.is_disconnected():
                print("ℹ️ [Studio] 客户端已断开连接，停止非流式重试。")
                return JSONResponse(
                    status_code=499,
                    content=create_openai_error_response(
                        499, "客户端已断开连接，请求已取消。", "client_closed_request"
                    ),
                )
            if attempt:
                stats.add_retry()
            try:
                use_native_tools = not body_cache.native_latched
                body = await body_cache.get(use_native_tools)
                # 降级 body 惰性构建（原生 Schema 首次被拒才建）；
                # degraded() 会把降级模式锁定到后续重试。
                fallback_body = (
                    body_cache.degraded
                    if self.native_custom_tools
                    and use_native_tools
                    and not self.text_transport
                    else None
                )
                req_headers = build_headers(self.cookie_str)
                if self.cookie_debug:
                    print(
                        f"🔎 [Studio 调试] 出站 generationConfig: "
                        f"{json.dumps(body['variables'].get('generationConfig', {}), ensure_ascii=False)}"
                    )

                async with open_upstream_client(self.client_kwargs) as client:
                    upstream_attempt()
                    response = await timed_call(
                        self.base_model_name,
                        client.post(BATCH_GRAPHQL_URL, headers=req_headers, json=body),
                        whole_call=True,
                        seconds=fr_seconds,
                    )
                    if fallback_body is not None and is_native_tool_error_response(
                        response.status_code, response.text
                    ):
                        resolved_fallback = (
                            await fallback_body()
                        )  # degraded() 同时锁定后续重试
                        print(
                            "⚠️ [Studio] 当前模型/协议拒绝原生函数 Schema；本次及后续重试固定降级为文本工具观测。"
                        )
                        upstream_attempt()
                        response = await timed_call(
                            self.base_model_name,
                            client.post(
                                BATCH_GRAPHQL_URL,
                                headers=req_headers,
                                json=resolved_fallback,
                            ),
                            whole_call=True,
                            seconds=fr_seconds,
                        )
                        fallback_body = None

                if is_retryable_status(response.status_code):
                    if attempt < self.retry_max:
                        delay = retry_delay(
                            getattr(response, "headers", {}).get("retry-after"),
                            self.backoff_sec,
                            attempt,
                            self.jitter_sec,
                            error=f"HTTP {response.status_code}: {response.text[:200]}",
                        )
                        print(
                            f"⚠️ [Studio] HTTP {response.status_code} (尝试 {attempt + 1}): {response.text[:300]} | {delay:.1f}s 后重试..."
                        )
                        if not await wait_for_retry(self.fastapi_request, delay):
                            return JSONResponse(
                                status_code=499,
                                content=create_openai_error_response(
                                    499, "客户端已断开连接。", "client_closed_request"
                                ),
                            )
                        continue

                if response.status_code != 200:
                    elapsed = time.time() - self.start_time
                    print(
                        f"❌ [Studio] {self.base_model_name} | HTTP {response.status_code} | {elapsed:.1f}s"
                    )
                    return JSONResponse(
                        status_code=response.status_code,
                        content=create_openai_error_response(
                            response.status_code, response.text[:500], "upstream_error"
                        ),
                    )

                full_text = ""
                reasoning_text = ""
                safety_html = ""
                tool_calls = []
                ordinary_metadata = []
                part_order = []
                message_extra = None
                api_error = None
                api_error_status = 502
                usage_meta = None
                finish_raw = None
                blocked_msg = None
                sampler = RawSampler()

                fake_resp = TextSource(response.text)
                text_decoder = (
                    self.text_transport.decoder() if self.text_transport else None
                )
                async for obj in iter_json_objects(fake_resp):
                    sampler.add(obj)
                    # error 即停：后续对象不再解析（生图响应可达数 MB，
                    # 白烧事件循环 CPU），且首个 error 不被后续 error 覆盖。
                    # 生图路径（_collect_full_response）同构修复已验证。
                    if api_error:
                        break
                    events = extract_events(obj)
                    if text_decoder:
                        events = text_decoder.cookie_events(events)
                    # list() 物化：extract_events / cookie_events 是生成器，
                    # 下方 any() 与 for 各消费一次，不物化后者会拿到空。
                    events = list(events)
                    # 与流式路径同口径：空分片/纯错误分片不计入「收到首个上游分片」。
                    if any(et != "error" for et, _ in events):
                        upstream_chunk()
                    for event_type, data in events:
                        if event_type == "text":
                            full_text += data
                            part_order.append(
                                {"type": "ordinary", "index": len(ordinary_metadata)}
                            )
                            ordinary_metadata.append(
                                ordinary_part_metadata("text", data, None)
                            )
                        elif event_type == "thought":
                            reasoning_text += data  # 先收集（供诊断判断），输出时按 strip 决定是否附带
                            part_order.append(
                                {"type": "ordinary", "index": len(ordinary_metadata)}
                            )
                            ordinary_metadata.append(
                                ordinary_part_metadata("thought", data, None)
                            )
                        elif event_type == "image":
                            full_text += data
                            part_order.append(
                                {"type": "ordinary", "index": len(ordinary_metadata)}
                            )
                            ordinary_metadata.append(
                                ordinary_part_metadata("image", "", None)
                            )
                        elif event_type == "function_call":
                            part_order.append(
                                {"type": "tool_call", "index": len(tool_calls)}
                            )
                            tool_calls.append(
                                openai_tool_call(
                                    self.response_id, len(tool_calls), data
                                )
                            )
                        elif event_type == "thought_signature":
                            sig = apply_wire_signature(
                                data, ordinary_metadata, part_order
                            )
                            if sig:
                                message_extra = thought_signature_extra(
                                    sig, data.get("part_kind")
                                )
                        elif event_type == "finish":
                            finish_raw = data
                        elif event_type == "safety":
                            safety_html = _safety_html_if_enabled(data)
                        elif event_type == "blocked":
                            blocked_msg = data
                        elif event_type == "usage":
                            usage_meta = merge_usage_metadata(usage_meta, data)
                        elif event_type == "error":
                            api_error_status, api_error = api_error_details(data)
                            break

                if (
                    api_error
                    and is_retryable_status(api_error_status)
                    and attempt < self.retry_max
                ):
                    wait_sec = retry_delay(
                        None,
                        self.backoff_sec,
                        attempt,
                        self.jitter_sec,
                        error=api_error,
                    )
                    print(
                        f"⚠️ [Studio] 429/限流 (尝试 {attempt + 1}): {api_error[:300]} | {wait_sec:.1f}s 后重试..."
                    )
                    if not await wait_for_retry(self.fastapi_request, wait_sec):
                        return JSONResponse(
                            status_code=499,
                            content=create_openai_error_response(
                                499, "客户端已断开连接。", "client_closed_request"
                            ),
                        )
                    continue
                if api_error:
                    if api_error_status in (401, 403):
                        credential_manager.record_cookie_auth_failure(
                            self.account_id, credential=self.record
                        )
                    return JSONResponse(
                        status_code=api_error_status,
                        content=create_openai_error_response(
                            api_error_status, api_error, "upstream_error"
                        ),
                    )

                if text_decoder:
                    text_decoder.validate_end()

                got_text = bool(full_text.strip())
                got_output = got_text or bool(tool_calls)
                elapsed = time.time() - self.start_time

                if got_output:
                    require_finish_reason(finish_raw)
                    credential_manager.record_cookie_success(
                        self.account_id, credential=self.record
                    )
                    # 预填充智能兼容：去重后把预填充文本拼回输出开头
                    if self.prefill_text and got_text:
                        full_text = self.prefill_text + strip_prefill_overlap(
                            self.prefill_text, full_text
                        )
                    finish_reason = map_finish_reason_to_openai(
                        finish_raw, has_tool_calls=bool(tool_calls)
                    )
                    stats.add_success()
                    credential_manager.record_cookie_success(
                        self.account_id, credential=self.record
                    )
                    print(
                        f"✅ [Studio] {self.base_model_name} | {len(full_text)} 字符 | {elapsed:.1f}s"
                    )
                else:
                    # 无正文（只有思考 / 完全空）：返回明确诊断而不是一个空格
                    desc, detail, notice, finish_reason = _no_body_outcome(
                        finish_raw,
                        blocked_msg,
                        bool(reasoning_text),
                        self.strip_thoughts,
                    )
                    stats.add_error()
                    print(
                        f"❌ [Studio] {self.base_model_name} | 上游{desc}（{detail}）| {elapsed:.1f}s"
                    )
                    print(f"🔎 [Studio 诊断] 原始响应样本：\n{sampler.dump()}")
                    full_text = notice

                usage = _final_usage(
                    usage_meta, self.request_obj, full_text, reasoning_text
                )

                content_value = full_text + safety_html
                message_obj = {
                    "role": "assistant",
                    "content": content_value if content_value else None,
                }
                if (
                    reasoning_text and not self.strip_thoughts
                ):  # strip：batchGraphql 忽略 includeThoughts，输出侧剥离
                    message_obj["reasoning_content"] = reasoning_text
                if tool_calls:
                    message_obj["tool_calls"] = tool_calls
                if ordinary_metadata:
                    if message_extra is None:
                        message_extra = {"google": {}}
                    google = message_extra.setdefault("google", {})
                    google["ordinary_parts"] = ordinary_metadata
                    if tool_calls:
                        google["part_order"] = part_order
                if message_extra:
                    message_obj["extra_content"] = message_extra

                payload = {
                    "id": self.response_id,
                    "object": "chat.completion",
                    "created": int(time.time()),
                    "model": self.model_display,
                    "choices": [
                        {
                            "index": 0,
                            "message": message_obj,
                            "finish_reason": finish_reason,
                        }
                    ],
                    "usage": usage,
                }
                return JSONResponse(content=payload)

            except Exception as e:
                err_msg = str(e)
                is_retryable = is_retryable_exception(e)

                if is_retryable and attempt < self.retry_max:
                    wait_sec = retry_delay(
                        None, self.backoff_sec, attempt, self.jitter_sec, error=err_msg
                    )
                    print(
                        f"⚠️ [Studio] 异常 (尝试 {attempt + 1}): {err_msg[:300]} | {wait_sec:.1f}s 后重试..."
                    )
                    if not await wait_for_retry(self.fastapi_request, wait_sec):
                        return JSONResponse(
                            status_code=499,
                            content=create_openai_error_response(
                                499, "客户端已断开连接。", "client_closed_request"
                            ),
                        )
                    continue

                elapsed = time.time() - self.start_time
                print(
                    f"❌ [Studio] {self.base_model_name} | 异常 | {elapsed:.1f}s: {err_msg[:300]}"
                )
                if not isinstance(e, (AntiTruncationError, IncompleteResponseError)):
                    traceback.print_exc()
                code, _ = extract_upstream_error(e)
                return JSONResponse(
                    status_code=code,
                    content=create_openai_error_response(
                        code, f"batchGraphql proxy error: {err_msg}", "proxy_error"
                    ),
                )

        elapsed = time.time() - self.start_time
        print(
            f"❌ [Studio] {self.base_model_name} | 重试 {self.retry_max} 次后仍失败 | {elapsed:.1f}s"
        )
        return JSONResponse(
            status_code=429,
            content=create_openai_error_response(
                429, "请求被限流，已重试多次仍失败。请稍后再试。", "rate_limit_error"
            ),
        )

    async def _result_once(self):
        yield await self.collect_response()

    async def buffered_frames(self):
        events = with_heartbeats(self._result_once(), self.fastapi_request)
        try:
            yield HEARTBEAT_FRAME
            async for result in events:
                if result is HEARTBEAT:
                    yield HEARTBEAT_FRAME
                    continue
                payload = json.loads(result.body)
                if result.status_code >= 400:
                    stats.add_error()
                    error = payload.get("error")
                    if isinstance(error, dict):
                        error.setdefault("code", result.status_code)
                    yield "data: " + json.dumps(payload, ensure_ascii=False) + "\n\n"
                    yield "data: [DONE]\n\n"
                    return
                async for frame in buffered_sse(
                    payload, include_usage=self.want_usage
                ).body_iterator:
                    yield frame
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            stats.add_error()
            code, message = extract_upstream_error(exc)
            error = create_openai_error_response(code, message[:1024], "upstream_error")
            yield "data: " + json.dumps(error, ensure_ascii=False) + "\n\n"
            yield "data: [DONE]\n\n"
        finally:
            await events.aclose()


def _prepare_cookie_generation(
    request_obj: OpenAIRequest, fastapi_request: Request, credential_record: dict | None
) -> CookieGeneration | JSONResponse:
    """Validate credentials and snapshot model/delivery settings before streaming."""
    track_configure(mode="stream" if request_obj.stream else "json", channel="cookie")
    try:
        validate_openai_request(request_obj)
        error = cookie_feature_error(request_obj, structured_output=app_state.get_setting("cookie_structured_output", False))
        if error:
            raise SchemaValidationError(error)
    except SchemaValidationError as exc:
        # 统一错误形状：message/type/code/param 四字段，与 Express 通道及
        # OpenAI SDK 重试逻辑的读取口径一致（旧实现缺 code/param）。
        return JSONResponse(
            status_code=400,
            content=create_openai_error_response(
                400, str(exc), "invalid_request_error"
            ),
        )

    # ===== 1. 验证认证 =====
    record = credential_record or credential_manager.get_cookie_record()
    cookie_str = record["cookie"] if record else _get_cookie_string()
    if not cookie_str:
        return JSONResponse(
            status_code=401,
            content=create_openai_error_response(
                401,
                (
                    "未配置 Google Cookie。\n"
                    "请在大盘控制台中粘贴 Cookie 和 Project ID，\n"
                    "或设置环境变量 GOOGLE_COOKIE 和 GOOGLE_PROJECT_ID。"
                ),
                "auth_error",
            ),
        )

    project_id = record["project_id"] if record else _get_project_id()
    track_configure(project_id=project_id)
    if not project_id:
        return JSONResponse(
            status_code=400,
            content=create_openai_error_response(
                400,
                (
                    "未配置 Google Cloud Project ID。\n"
                    "请在大盘中填写，或设置环境变量 GOOGLE_PROJECT_ID。\n"
                    "可从 Studio URL 中获取：...?project=YOUR_PROJECT_ID"
                ),
                "config_error",
            ),
        )

    # ===== 2. 校验认证字段 =====
    # 认证头包含实时时间戳、每次出站都重算；这里只做一次字段校验。
    if not build_headers(cookie_str):
        return JSONResponse(
            status_code=401,
            content=create_openai_error_response(
                401,
                (
                    "Cookie 中未找到 SAPISID，无法计算认证头。\n"
                    "请确保 Cookie 来自已登录的 console.cloud.google.com 页面。"
                ),
                "auth_error",
            ),
        )

    # 账号健康隔离钩子：鉴权类失败累计冷却、成功清零（credential_manager）。
    account_id = (record or {}).get("id", "")

    # ===== 2.5 工具流量处理：原生函数调用 =====
    _tool_info = classify_tool_traffic(request_obj.messages, request_obj.tools)
    force_builtin_search = bool(_tool_info["builtin_search"])
    _forced_search = is_forced_builtin_search(request_obj)
    search_selected = _forced_search or search_suffix_has_priority(request_obj)
    native_custom_tools = bool(_tool_info["custom_names"] or _tool_info["history"]) and not search_selected
    if (force_builtin_search or search_selected) and _tool_info["custom_names"]:
        if search_selected:
            print(
                "🔎 [Studio 搜索] batchGraphql 不支持内建搜索与自定义函数混用；"
                "本轮按搜索后缀或显式选择优先搜索，仅启用 googleSearch，跳过可选自定义函数。"
            )
        else:
            print(
                "⚠️ [Studio] batchGraphql 不支持内建搜索与自定义函数混用；本轮优先启用自定义函数，忽略搜索声明。"
            )
    elif force_builtin_search:
        print("🔎 [Studio] 请求声明了搜索类工具，已映射为 Studio 内建 googleSearch。")
    if _tool_info["custom_names"] and not search_selected:
        print(
            f"🛠️ [Studio] 原生函数调用：{'、'.join(_tool_info['custom_names'][:5])}"
            f"{' 等' if len(_tool_info['custom_names']) > 5 else ''}。"
        )

    # ===== 3. 解析模型名 =====
    model_display = request_obj.model
    base_model_name, _ = split_model_alias(model_display)
    is_grounded_search = base_model_name.endswith("-search")
    if is_grounded_search:
        base_model_name = base_model_name[: -len("-search")]
    # Persist the parsed selection for every delivery/retry path. Inspecting the
    # raw request model later misses whitespace and combined transport aliases.
    force_builtin_search = force_builtin_search or is_grounded_search or _forced_search

    _profile = mc.get_profile(base_model_name)
    _inj_settings = app_state.get_effective_settings(base_model_name)
    request_obj, prefill_text, prefill_active = prepare_messages(
        request_obj,
        _profile,
        _inj_settings,
        has_tools=bool(_tool_info["declared"] or _tool_info["history"]),
    )

    request_obj, text_transport = prepare_transport(
        request_obj,
        base_model_name,
        _inj_settings,
        search=force_builtin_search,
        prefill=prefill_active or bool(prefill_text),
    )

    # ===== 4. HTTP 客户端配置 =====
    # One account snapshot binds Cookie, project and proxy chain for this
    # generation. The transport tries dedicated candidates before globals.
    client_kwargs = {
        "timeout": httpx.Timeout(connect=30.0, read=180.0, write=30.0, pool=10.0),
        "follow_redirects": False,
        "trust_env": False,
    }
    if app_config.SSL_CERT_FILE:
        client_kwargs["verify"] = app_config.SSL_CERT_FILE
    proxy_chain = ProxyChain(record)
    # 与 Express 通道同款出站可观测性：本次请求的实际首选出口（账号级优先、
    # 全局兑底）；直连（无候选）静默，与 express_sdk 的行为一致。
    if proxy_chain.current:
        from proxy_resolver import mask_proxy_uri

        print(f"🌐 [出站代理] 当前出口：{mask_proxy_uri(proxy_chain.current)}")
    client_kwargs["_proxy_chain"] = proxy_chain.bind_to_generation()

    # 重试配置（控制台可调）；语义与 Express 通道统一：总尝试次数 = retry_max + 1
    # 间隔为固定值 + 抖动（backoff ± jitter/2，不随次数放大）
    retry_max, backoff_sec, jitter_sec = get_retry_settings()

    buffer_text = not _profile["is_image"] and (
        (text_transport and text_transport.mode == "nonstream")
        or app_state.get_setting("fake_streaming", app_config.FAKE_STREAMING_ENABLED)
    )
    is_stream = request_obj.stream and not buffer_text
    response_id = f"chatcmpl-studio-{int(time.time())}-{uuid.uuid4().hex[:8]}"
    start_time = time.time()
    want_usage = wants_usage(request_obj)
    cookie_debug = bool(
        app_state.get_setting("debug_outbound", False)
        or app_state.get_setting("cookie_debug", False)
    )

    # batchGraphql(Studio) 会忽略 includeThoughts=false（真机验证），因此当解析出的思考配置
    # 要求不回传思考时，由本通道在响应侧主动剥离思考块，生图同样适用。
    # 复用 3. 段已取的 _inj_settings：同参数同模型，两次 get_effective_settings
    # 之间无任何设置写入点，旧实现白做一次全量深拷贝+合并（热路径，2026-10 优化）。
    _eff_settings = _inj_settings
    _tk = mc.resolve_thinking(
        base_model_name, request_obj, _eff_settings, prefill_active=prefill_active
    )
    strip_thoughts = bool(_tk.get("mode") and not _tk.get("include_thoughts", True))
    if strip_thoughts and cookie_debug:
        print(
            "🔎 [Studio 调试] 已启用响应侧思考剥离（batchGraphql 不支持 includeThoughts=false）。"
        )

    # 打印请求日志
    msg_count = len(request_obj.messages)
    print(
        f"→ [Studio] {base_model_name} | {msg_count} 条消息 | {'流式' if is_stream else '非流式'}"
    )

    is_image = _profile["is_image"]
    track_configure(
        mode="fake_stream"
        if request_obj.stream and (buffer_text or is_image)
        else "stream"
        if request_obj.stream
        else "json",
        thinking_level=_tk.get("level"),
    )

    return CookieGeneration(
        request_obj=request_obj,
        fastapi_request=fastapi_request,
        record=record,
        account_id=account_id,
        cookie_str=cookie_str,
        project_id=project_id,
        base_model_name=base_model_name,
        model_display=model_display,
        response_id=response_id,
        client_kwargs=client_kwargs,
        retry_max=retry_max,
        backoff_sec=backoff_sec,
        jitter_sec=jitter_sec,
        start_time=start_time,
        prefill_text=prefill_text,
        prefill_active=prefill_active,
        force_builtin_search=force_builtin_search,
        native_custom_tools=native_custom_tools,
        text_transport=text_transport,
        strip_thoughts=strip_thoughts,
        want_usage=want_usage,
        cookie_debug=cookie_debug,
        is_stream=is_stream,
        is_image=is_image,
    )


class CookieProxyUpstream(BaseUpstream):
    """OpenAI adapter for Cookie batchGraphql generation."""

    async def chat_completions(
        self,
        request_obj: OpenAIRequest,
        fastapi_request: Request,
        *,
        credential_record=None,
    ):
        prepared = _prepare_cookie_generation(
            request_obj, fastapi_request, credential_record
        )
        if isinstance(prepared, JSONResponse):
            return prepared
        return await prepared.response()

"""SDK 调用、响应转换与 OpenAI 流式输出编排。"""

import asyncio
import inspect
import json
import time
import uuid
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

import config as app_config
import model_capabilities as mc
from anti_truncation import TextTransport, supports_argument_stream
from express_endpoint import is_location_pin_failure
from fastapi.responses import JSONResponse, StreamingResponse
from first_reply import (
    FirstReplyGuard,
    FirstReplyTimeout,
    sdk_has_reply,
    timed_call,
    timeout_seconds,
)
from google.genai import types

# 引入报错重试统计器
from logger import stats
from media_tasks import run_media_task
from message_processing import (
    convert_to_openai_format,
)
from models import OpenAIMessage, OpenAIRequest
from prefill import PrefillDeduper
from request_tracking import (
    attempt_started_at,
    record_usage,
    upstream_attempt,
    upstream_chunk,
)
from request_tracking import (
    configure as track_configure,
)
from response_validation import (
    IncompleteResponseError,
    PendingMetadataGate,
    has_finish_reason,
    require_complete_response,
    require_unblocked_response,
)
from retry_policy import (
    exception_retry_delay,
    get_retry_settings,
    is_auth_or_billing_error,
    is_retryable_exception,
    wait_for_retry,
)
from runtime_state import app_state
from tool_stream import ToolStreamAssembler
from streaming import disconnected as _request_disconnected
from upstream_errors import (
    create_openai_error_response,
    error_status,
    extract_upstream_error,
)
from upstreams.express_response import (
    ToolCallIndexer as ToolCallIndexer,
)
from upstreams.express_response import (
    _build_chunk_payload as _build_chunk_payload,
)
from upstreams.express_response import (
    _chunk_openai_response_dict_for_sse as _chunk_openai_response_dict_for_sse,
)
from upstreams.express_response import (
    _dedup_chunk_payload as _dedup_chunk_payload,
)
from upstreams.express_response import (
    _prepend_prefill as _prepend_prefill,
)
from upstreams.express_response import (
    convert_chunk_to_openai as convert_chunk_to_openai,
)
from upstreams.express_response import (
    is_gemini_response_valid as is_gemini_response_valid,
)
from usage import make_usage_chunk, map_usage, merge_usage_metadata, wants_usage


def _keepalive_frame(model_name: str) -> str:
    """假流式等待期间的心跳帧（空 delta chunk，前端按普通 chunk 忽略）。"""
    payload = {
        "id": "chatcmpl-keepalive",
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": model_name,
        "choices": [{"delta": {"content": ""}, "index": 0, "finish_reason": None}],
    }
    return f"data: {json.dumps(payload)}\n\n"


def _pin_failure_fallback(fallback_model: str, error: Exception, get_fallback_client):
    """location 钉定路径失败的统一退回（真流式与非流式共用同一句提示）。

    返回 (裸模型名, 普通客户端)；调用方需自行保证仅在尚未输出内容时才走到这里。
    """
    print(
        f"↩️ [上游端点] 钉定路径调用失败（{str(error)[:80]}），"
        f"已退回默认路由 {fallback_model} 重试一次。"
        "如持续出现，请确认「通道与凭证」里的 Project ID 属于该 API Key 且已开启计费，"
        "或把「标准模式 location」设为“默认（后端自选）”。"
    )
    print("ℹ️ [流量等级] 回退默认路由已使用普通请求。")
    return fallback_model, get_fallback_client()


def _record_usage(resp: Any) -> dict:
    """记录 token 用量，并打印上游实际返回的流量等级。

    返回完整 map_usage 结果（含 prompt_tokens_details / completion_tokens_details
    细分字段，供响应体透传——Claude 侧映射 cache_read_input_tokens 与
    output_tokens_details.thinking_tokens 依赖这两个细节）。
    """
    usage_metadata = getattr(resp, "usage_metadata", None)
    traffic_type = (
        getattr(usage_metadata, "traffic_type", None) if usage_metadata else None
    )
    if traffic_type:
        value = getattr(traffic_type, "value", None) or str(traffic_type)
        print(f"🚦 [流量等级] 上游实际 traffic_type={value}")

    usage = map_usage(usage_metadata)
    record_usage(usage)
    p_tk, c_tk = usage["prompt_tokens"], usage["completion_tokens"]
    stats.add_tokens(p_tk, c_tk)
    if p_tk or c_tk:
        print(
            f"💰 [算力消耗统计] 提示词: {p_tk} | 思考与生成: {c_tk} | 总计: {usage['total_tokens']} Tokens"
        )
    return usage


def _resolve_retry_target(current_client, current_model, current_fallback, target):
    client, model, fallback = target
    # 池化 SDK 客户端对应一把 Key；单 Key 重试保留本轮已采用的裸模型兜底，
    # 避免每次 429 都先重打已知无效的 projects/locations 路径。
    if client is current_client and current_model == current_fallback == fallback:
        model = current_model
    return client, model, fallback


def _call_retry_factory(factory: Callable, error: Optional[Exception]):
    """调用重试目标工厂；兼容旧的无参工厂（测试桩等），仅在工厂声明接受 error 时传递。

    坏凭证（鉴权/欠费）需要随错误一起交给工厂做隔离标记；用签名探测而非
    try/except TypeError，避免把工厂内部的真实 TypeError 误当参数不匹配。
    """
    if error is not None:
        try:
            params = inspect.signature(factory).parameters
        except (TypeError, ValueError):
            params = {}
        if "error" in params or any(
            p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()
        ):
            return factory(error=error)
    return factory()


def _warn_bad_credential(
    describer: Optional[Callable[[], str]], error: Exception, warned: set
) -> bool:
    """坏凭证红色醒目告警（每凭证只告警一次，对齐上游 _emit_credential_invalid_warning）。

    返回是否命中坏凭证分类，便于调用方据此决定隔离与立即切换。
    """
    if not is_auth_or_billing_error(error):
        return False
    if describer is not None:
        label = describer()
        if label not in warned:
            warned.add(label)
            code = error_status(error)
            print(
                f"🔴 [密钥失效提醒] {label} 鉴权或计费失败（HTTP {code or '?'}），"
                f"本轮请求已自动隔离并跳过该凭证！"
                f"👉 请及时前往控制台「通道与凭证」页检查、删除或更换。原因：{str(error)[:300]}"
            )
    return True


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
    retry_target_factory: Optional[Callable] = None,
    text_transport: Optional[TextTransport] = None,
    credential_describer: Optional[Callable[[], str]] = None,
):
    print(
        f"🌊 [假流式] 已开始调用 Gemini 模型 {model_for_api_call}，客户端请求模型名为 {request_obj.model}。"
    )
    fake_stream_started = time.monotonic()
    warned_credentials: set = set()

    # P1-6：不再使用 tenacity 的硬编码 20 次，改为与真流式/非流式一致的手写退避，
    # 读取控制台的 retry_max / retry_backoff_seconds / retry_jitter_seconds
    # （固定间隔 + 抖动，不随次数放大），并在等待期间检测客户端断开。
    max_retries, backoff_sec, jitter_sec = get_retry_settings()
    # 首回复窗口每请求只读一次设置：重试循环里每次 timed_call 都重复
    # 深拷贝整棵设置树（含全部 model_overrides），重试风暴下放大浪费。
    fr_seconds = timeout_seconds(request_obj.model)

    outer_keep_alive_interval = app_state.get_setting(
        "fake_streaming_interval", app_config.FAKE_STREAMING_INTERVAL_SECONDS
    )

    api_call_task = None
    raw_gemini_response = None
    last_error = None
    retry_pending = False
    heartbeat_due = 0.0

    try:
        attempt = 0
        while attempt <= max_retries:
            if await _request_disconnected(fastapi_request):
                print(
                    f"ℹ️ [客户端断开] 假流式请求前检测到客户端已断开，停止调用模型 {model_for_api_call}。"
                )
                return

            if retry_pending:
                if retry_target_factory:
                    target = _call_retry_factory(retry_target_factory, last_error)
                    if inspect.isawaitable(target):
                        target = await target
                    if target is None:
                        raise last_error or RuntimeError("没有可用的 Express API Key。")
                    gemini_client_instance, model_for_api_call, fallback_model = (
                        _resolve_retry_target(
                            gemini_client_instance,
                            model_for_api_call,
                            fallback_model,
                            target,
                        )
                    )
                stats.add_retry()
                retry_pending = False

            upstream_attempt()
            api_call_task = asyncio.create_task(
                timed_call(
                    request_obj.model,
                    gemini_client_instance.aio.models.generate_content(
                        model=model_for_api_call,
                        contents=prompt_for_api_call,
                        config=gen_config_dict_for_api_call,
                    ),
                    whole_call=True,
                    seconds=fr_seconds,
                )
            )

            # 等待期间持续吐 keep-alive，避免前端因长时间无字节而超时
            while not api_call_task.done():
                if outer_keep_alive_interval > 0 and time.monotonic() >= heartbeat_due:
                    yield _keepalive_frame(request_obj.model)
                    heartbeat_due = time.monotonic() + outer_keep_alive_interval
                await asyncio.wait({api_call_task}, timeout=0.5)
                if await _request_disconnected(fastapi_request):
                    print(
                        "ℹ️ [客户端断开] 假流式等待期间客户端已断开，正在取消上游任务。"
                    )
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
                if (
                    fallback_model
                    and model_for_api_call != fallback_model
                    and is_location_pin_failure(e_call)
                ):
                    model_for_api_call = fallback_model
                    if fallback_client_factory:
                        gemini_client_instance = fallback_client_factory()
                    print(
                        f"↩️ [上游端点] 假流式钉定失败，退回裸模型名 {fallback_model} 重试。"
                    )
                    continue
                # 坏凭证（鉴权失效/欠费）：告警 + 隔离 + 立即切换下一把 Key，
                # 同一把凭证重试只会得到相同结果（对齐上游坏凭证隔离体系）。
                bad_credential = _warn_bad_credential(
                    credential_describer, e_call, warned_credentials
                )
                if (
                    is_retryable_exception(e_call) or bad_credential
                ) and attempt < max_retries:
                    if bad_credential:
                        print(
                            f"⚠️ [凭证隔离] 假流式检测到坏凭证（{e_call.__class__.__name__}），跳过退避等待，立即切换下一个 Express Key。"
                        )
                    else:
                        wait_time = exception_retry_delay(
                            e_call, backoff_sec, attempt, jitter_sec
                        )
                        print(
                            f"⚠️ [自动重试] 假流式上游繁忙（{e_call.__class__.__name__}），"
                            f"第 {attempt + 1} 次退避重试，等待 {wait_time} 秒。"
                        )
                        waited = 0.0
                        while waited < wait_time:
                            step = min(0.5, wait_time - waited)
                            await asyncio.sleep(step)
                            waited += step
                            if await _request_disconnected(fastapi_request):
                                print(
                                    "ℹ️ [客户端断开] 假流式退避期间客户端已断开，停止重试。"
                                )
                                return
                            if (
                                outer_keep_alive_interval > 0
                                and time.monotonic() >= heartbeat_due
                            ):
                                yield _keepalive_frame(request_obj.model)
                                heartbeat_due = (
                                    time.monotonic() + outer_keep_alive_interval
                                )
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
        openai_response_dict = convert_to_openai_format(
            raw_gemini_response, request_obj.model
        )
        _prepend_prefill(openai_response_dict, prefill_text)
        _record_usage(raw_gemini_response)
        async for chunk_sse in _chunk_openai_response_dict_for_sse(
            openai_response_dict=openai_response_dict
        ):
            if chunk_sse.strip() == "data: [DONE]" and wants_usage(request_obj):
                yield make_usage_chunk(
                    openai_response_dict["id"],
                    request_obj.model,
                    openai_response_dict.get("usage", {}),
                )
            yield chunk_sse
        print(
            f"✅ [Express] {request_obj.model} | 假流式传输完成 | 耗时 {time.monotonic() - fake_stream_started:.1f}s"
        )

    except asyncio.CancelledError:
        print(
            f"ℹ️ [客户端断开] 假流式响应期间客户端已断开，正在清理模型 {request_obj.model} 的后台任务。"
        )
        # api_call_task 在循环前已无条件初始化为 None；真正完备的清理在
        # finally（cancel + await），这里只做断连路径的快速取消。
        if api_call_task is not None and not api_call_task.done():
            api_call_task.cancel()
        raise
    except Exception as e_outer_gemini:
        err_msg_detail = f"Gemini 假流式生成器异常（模型：{request_obj.model}）：{type(e_outer_gemini).__name__} - {str(e_outer_gemini)}"
        print(
            f"❌ [API 错误响应] 假流发生器运行崩溃 (Model: {request_obj.model})。错误详情: {err_msg_detail}"
        )
        sse_err_msg_display = str(e_outer_gemini)
        if len(sse_err_msg_display) > 512:
            sse_err_msg_display = sse_err_msg_display[:512] + "..."
        code, _ = extract_upstream_error(e_outer_gemini)
        err_resp_sse = create_openai_error_response(
            code, sse_err_msg_display, "server_error"
        )
        json_payload_error = json.dumps(err_resp_sse)
        if not is_auto_attempt:
            # 流式响应已是 200，中间件按状态码计不到这次失败，这里补记
            # （auto 模式抛给外层通道接管，由外层统一计数，避免双计）
            stats.add_error()
            yield f"data: {json_payload_error}\n\n"
            yield "data: [DONE]\n\n"
        if is_auto_attempt:
            raise
    finally:
        if api_call_task is not None and not api_call_task.done():
            api_call_task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await api_call_task


@dataclass(kw_only=True)
class SDKGeneration:
    """Per-request SDK selection, retry state and response delivery.

    Client/model changes during fallback are explicit instance state, so the
    streaming and nonstreaming paths share selection rules without closures.
    """

    current_client: Any
    model_to_call: str
    gen_config_dict: Dict[str, Any]
    request_obj: OpenAIRequest
    actual_prompt_for_call: List[types.Content]
    is_auto_attempt: bool
    fastapi_request: Any
    prefill_text: str
    fallback_model: Optional[str]
    fallback_client_factory: Optional[Callable[[], Any]]
    retry_target_factory: Optional[Callable]
    text_transport: Optional[TextTransport]
    credential_describer: Optional[Callable[[], str]]
    warned_credentials: set = field(default_factory=set)
    response_id_for_stream: str = ""

    async def response(self):
        if not self.request_obj.stream:
            return await self.collect_response()
        is_image = mc.get_profile(self.request_obj.model)["is_image"]
        buffered = (
            app_state.get_setting("fake_streaming", app_config.FAKE_STREAMING_ENABLED)
            or is_image
            or (self.text_transport and self.text_transport.mode == "nonstream")
        )
        thinking = self.gen_config_dict.get("thinking_config") or {}
        tracking = {
            "mode": "fake_stream" if buffered else "stream",
            "upstream_model": self.model_to_call,
            "thinking_level": thinking.get("thinking_level"),
            "max_output_tokens": self.gen_config_dict.get("max_output_tokens"),
        }
        if not buffered:
            tracking["include_thoughts"] = thinking.get("include_thoughts")
        track_configure(**tracking)
        if buffered:
            if is_image:
                print(
                    "🖼️ [生图保护] 图片模型请求已自动切换为假流式输出，以避免上游流式限制。"
                )
            frames = gemini_fake_stream_generator(
                self.current_client,
                self.model_to_call,
                self.actual_prompt_for_call,
                self.gen_config_dict,
                self.request_obj,
                self.is_auto_attempt,
                fastapi_request=self.fastapi_request,
                prefill_text=self.prefill_text,
                fallback_model=self.fallback_model,
                fallback_client_factory=self.fallback_client_factory,
                retry_target_factory=self.retry_target_factory,
                text_transport=self.text_transport,
                credential_describer=self.credential_describer,
            )
        else:
            # Ask for real generation progress even when a preset puts the
            # entire reply in a required tool call. Keep the first-reply window:
            # silent queueing still times out, native tool fragments do not.
            if (
                self.text_transport is None
                and supports_argument_stream(self.request_obj.model)
                and any(tool.get("function_declarations") for tool in self.gen_config_dict.get("tools", []))
                and self.gen_config_dict.get("tool_config", {}).get("function_calling_config", {}).get("mode") != "NONE"
            ):
                fcc = self.gen_config_dict.setdefault("tool_config", {}).setdefault("function_calling_config", {})
                fcc["stream_function_call_arguments"] = True
                print("🔧 [工具参数流] 已启用原生参数分片；真实工具输出算首回复，完整调用合并后交付。")
            if self.gen_config_dict.get("tool_config", {}).get("function_calling_config", {}).get("stream_function_call_arguments"):
                # Native partial arguments and SDK automatic tool execution are
                # mutually exclusive. Clients own execution in both protocols.
                self.gen_config_dict["automatic_function_calling"] = {"disable": True}
            self.response_id_for_stream = (
                f"chatcmpl-realstream-{int(time.time())}-{uuid.uuid4().hex[:12]}"
            )
            frames = self.stream_frames()
        return StreamingResponse(frames, media_type="text/event-stream")

    def fallback_client(self):
        if self.fallback_client_factory is None:
            return self.current_client
        # 工厂本身走池化；不能缓存上一次 Key 的客户端供换 Key 后兜底使用。
        return self.fallback_client_factory()

    async def select_retry_target(self, error: Optional[Exception] = None) -> bool:
        if self.retry_target_factory is None:
            return True
        target = _call_retry_factory(self.retry_target_factory, error)
        if inspect.isawaitable(target):
            target = await target
        if target is None:
            return False
        self.current_client, self.model_to_call, self.fallback_model = (
            _resolve_retry_target(
                self.current_client, self.model_to_call, self.fallback_model, target
            )
        )
        return True

    async def stream_frames(self):
        # 路径兜底和换 Key 都更新本次请求持有的模型与客户端。
        max_retries, backoff_sec, jitter_sec = get_retry_settings()
        fr_seconds = timeout_seconds(self.request_obj.model)
        has_yielded = False  # 是否已向客户端输出过内容
        stream_started = time.monotonic()  # 完成日志耗时统计（对齐上游）
        thinking = self.gen_config_dict.get("thinking_config") or {}
        print(
            f"🌊 [流式模式] {self.request_obj.model}：真流式，"
            f"思考档位={thinking.get('thinking_level', '默认')}，"
            f"回传思考={thinking.get('include_thoughts', '默认')}，"
            f"输出上限={self.gen_config_dict.get('max_output_tokens', '上游默认')}。"
        )
        # 立即吐一个 SSE 心跳，尽快建立连接（429 重试期间也保活，防前端超时中断）
        yield ": keep-alive\n\n"
        # 总尝试次数 = retry_max + 1，retry_max=0 时仍会请求一次
        attempt = 0
        retry_pending = False
        last_error = None
        while attempt <= max_retries:
            # 客户端断开则停止重试，避免无谓的上游调用
            if await _request_disconnected(self.fastapi_request):
                print(
                    f"ℹ️ [客户端断开] 真流式请求前检测到客户端已断开，停止调用模型 {self.model_to_call}。"
                )
                return
            stream_gen_obj = None
            target_unavailable = False
            attempt_started = time.monotonic()
            try:
                if retry_pending:
                    if not await self.select_retry_target(last_error):
                        target_unavailable = True
                        raise last_error or RuntimeError("没有可用的 Express API Key。")
                    stats.add_retry()
                    retry_pending = False
                upstream_attempt()
                first_reply = FirstReplyGuard(
                    self.request_obj.model, seconds=fr_seconds
                )
                stream_gen_obj = await first_reply.wait(
                    self.current_client.aio.models.generate_content_stream(
                        model=self.model_to_call,
                        contents=self.actual_prompt_for_call,
                        config=self.gen_config_dict,
                    )
                )

                # 预填充智能兼容：把预填充文本作为回复开头先发出（仅一次）；
                # 同时启用流式去重器，模型若复述预填充开头会被自动裁掉（n>1 时不启用）。
                # 每次 attempt 都重置 tool_calls 序号（重试会重发整轮）
                tool_indexer = ToolCallIndexer(
                    keep_unsigned_parts=self.text_transport is None
                    or len(self.request_obj.tools or []) > 1
                )
                text_decoder = (
                    self.text_transport.decoder() if self.text_transport else None
                )
                tool_assembler = (
                    ToolStreamAssembler()
                    if self.text_transport is None
                    and self.gen_config_dict.get("tool_config", {}).get("function_calling_config", {}).get("stream_function_call_arguments")
                    else None
                )
                deduper = (
                    PrefillDeduper(self.prefill_text)
                    if (self.prefill_text and (self.request_obj.n or 1) == 1)
                    else None
                )
                # 预填充先行帧：n>1 时为每个候选都发一份，与非流式
                # _prepend_prefill 的全 choice 行为一致（旧实现只发
                # choice 0，stream 与非 stream 同请求输出分叉）。
                _pf_frames = [
                    {
                        "id": self.response_id_for_stream,
                        "object": "chat.completion.chunk",
                        "created": int(time.time()),
                        "model": self.request_obj.model,
                        "choices": [
                            {
                                "index": ci,
                                "delta": {
                                    "role": "assistant",
                                    "content": self.prefill_text,
                                },
                                "finish_reason": None,
                            }
                        ],
                    }
                    for ci in range(max(1, self.request_obj.n or 1))
                ]

                final_p_tk, final_c_tk, final_t_tk = 0, 0, 0
                # 完整 usage dict（含 prompt_tokens_details.cached_tokens /
                # completion_tokens_details.reasoning_tokens 细分）：尾块
                # 直接透传，与假流式/非流式口径一致；旧实现只传三个标量，
                # Claude 生态的 cache_read_input_tokens 与 thinking 细分
                # 在真流式下永远缺失（2026-10 修复）。
                final_usage: dict = {}
                usage_metadata: dict = {}
                seen_candidates, finished_candidates = set(), set()
                received_chunks = 0
                content_chars, reasoning_chars, tool_deltas = 0, 0, 0
                finish_reasons = set()
                deferred_finishes = []
                pending_metadata = PendingMetadataGate()
                # 按 choice index 追踪首帧：OpenAI 官方帧序的首个 delta
                # 携带 role:"assistant"（Cookie 通道同口径；旧实现缺失，
                # 严格按官方 SDK 消费流的客户端会识别不出角色）。
                roles_sent = set()

                async for chunk_item_call in first_reply.iterate(
                    stream_gen_obj, sdk_has_reply
                ):
                    upstream_chunk()
                    received_chunks += 1
                    if received_chunks == 1:
                        # 归因到当前出口的尝试起点：多出口切换后
                        # attempt_started_at() 会重置，不把已失败出口
                        # 的等待时间累加进来（外层 attempt_started 只随
                        # 换 key 重试重置，口径是整次尝试总耗时）。
                        elapsed = time.monotonic() - (
                            attempt_started_at() or attempt_started
                        )
                        print(
                            f"📥 [首个上游分片] {self.request_obj.model}：第 {attempt + 1} 次尝试等待 {elapsed:.1f} 秒收到分片；分片可能仅含思考或用量。"
                        )
                    if getattr(chunk_item_call, "usage_metadata", None):
                        usage_metadata = merge_usage_metadata(usage_metadata, chunk_item_call.usage_metadata)
                        final_usage = map_usage(usage_metadata)
                        record_usage(
                            final_usage
                        )  # 与旧 _extract_usage 一致：trace 用量口径
                        final_p_tk, final_c_tk, final_t_tk = (
                            final_usage["prompt_tokens"],
                            final_usage["completion_tokens"],
                            final_usage["total_tokens"],
                        )

                    # 支持 n>1：按候选序号逐个输出
                    require_unblocked_response(chunk_item_call)
                    for ci, candidate in enumerate(chunk_item_call.candidates or []):
                        index = candidate.index if candidate.index is not None else ci
                        seen_candidates.add(index)
                        if has_finish_reason(candidate.finish_reason):
                            finished_candidates.add(index)
                    if text_decoder:
                        chunk_item_call = text_decoder.restore_sdk(chunk_item_call)
                    if tool_assembler:
                        chunk_item_call = tool_assembler.restore_sdk(chunk_item_call)
                    num_candidates = len(
                        getattr(chunk_item_call, "candidates", None) or []
                    )
                    for ci in range(num_candidates):
                        chunk_payload = _build_chunk_payload(
                            chunk_item_call,
                            self.request_obj.model,
                            self.response_id_for_stream,
                            ci,
                            indexer=tool_indexer,
                        )
                        if deduper is not None:
                            chunk_payload = _dedup_chunk_payload(chunk_payload, deduper)
                            if chunk_payload is None:
                                continue  # 正文暂存于去重器，跳过空 chunk
                        choice = (chunk_payload.get("choices") or [{}])[0]
                        delta = choice.get("delta") or {}
                        # 首帧补 role（预填充先行帧已自带，不重复注入）。
                        if (
                            not self.prefill_text
                            and choice.get("index") not in roles_sent
                        ):
                            choice["delta"] = {"role": "assistant", **delta}
                            delta = choice["delta"]
                            roles_sent.add(choice.get("index"))
                        if delta.get("content"):
                            if not content_chars:
                                elapsed = time.monotonic() - (
                                    attempt_started_at() or attempt_started
                                )
                                print(
                                    f"✍️ [首段正文] {self.request_obj.model}：第 {attempt + 1} 次尝试等待 {elapsed:.1f} 秒开始输出正文。"
                                )
                            content_chars += len(delta["content"])
                        reasoning_chars += len(delta.get("reasoning_content") or "")
                        tool_deltas += len(delta.get("tool_calls") or [])
                        if choice.get("finish_reason"):
                            finish_reasons.add(choice["finish_reason"])
                        signature = (
                            (delta.get("extra_content") or {}).get("google") or {}
                        ).get("thought_signature")
                        if not (
                            delta.get("content")
                            or delta.get("reasoning_content")
                            or delta.get("tool_calls")
                            or signature
                            or choice.get("finish_reason")
                        ):
                            continue
                        if choice.get("finish_reason") and not (
                            content_chars or reasoning_chars or tool_deltas
                        ):
                            # 有些前端收到 finish_reason 就结束读取；先校验空响应，
                            # 避免成功结束帧遮住后续的明确错误。
                            deferred_finishes.append(chunk_payload)
                            continue
                        if not first_reply.ready:
                            # 序列化一次复用：测量与最终下发共用同一份字符串
                            # （旧实现每个元数据 chunk 序列化两次）。
                            frame_text = json.dumps(chunk_payload, ensure_ascii=False)
                            pending_metadata.add(
                                frame_text, len(frame_text.encode("utf-8"))
                            )
                            continue
                        for metadata in pending_metadata.drain():
                            yield f"data: {metadata}\n\n"
                        if self.prefill_text and not has_yielded:
                            for _pf_chunk in _pf_frames:
                                yield f"data: {json.dumps(_pf_chunk, ensure_ascii=False)}\n\n"
                        if not has_yielded:
                            # 对齐上游：首个真实输出帧时明确告知流式已建立
                            print(
                                f"⚡ [Express] {self.request_obj.model} | 连接建立，正在实时流式输出…"
                            )
                        has_yielded = True
                        yield f"data: {json.dumps(chunk_payload, ensure_ascii=False)}\n\n"

                if tool_assembler:
                    tool_assembler.validate_end()
                if text_decoder:
                    text_decoder.validate_end()
                if seen_candidates - finished_candidates:
                    raise IncompleteResponseError(
                        "上游流式回复缺少候选结束标记，内容可能不完整。"
                    )
                # 去重器可能还攒着开头文本（上游没发 finish chunk 的场景）
                if deduper is not None and not deduper.done:
                    tail = deduper.flush()
                    if tail:
                        if self.prefill_text and not has_yielded:
                            for _pf_chunk in _pf_frames:
                                yield f"data: {json.dumps(_pf_chunk, ensure_ascii=False)}\n\n"
                        has_yielded = True
                        content_chars += len(tail)
                        _tail = {
                            "id": self.response_id_for_stream,
                            "object": "chat.completion.chunk",
                            "created": int(time.time()),
                            "model": self.request_obj.model,
                            "choices": [
                                {
                                    "index": 0,
                                    "delta": {"content": tail},
                                    "finish_reason": None,
                                }
                            ],
                        }
                        yield f"data: {json.dumps(_tail, ensure_ascii=False)}\n\n"

                if final_p_tk > 0 or final_c_tk > 0:
                    print(
                        f"💰 [算力消耗统计] 提示词: {final_p_tk} | 思考与生成: {final_c_tk} | 总计: {final_t_tk} Tokens"
                    )
                reasons = ",".join(sorted(finish_reasons)) or "无"
                print(
                    f"📊 [流式完成] {self.request_obj.model}：正文 {content_chars} 字符，思考 {reasoning_chars} 字符，"
                    f"工具增量 {tool_deltas}，结束原因={reasons}。"
                )
                # 结束标记或签名单独到达也会设置 has_yielded，但它们不是回复内容。
                # 安全拦截允许空正文；只有思考的合法响应保留，同时明确提示诊断。
                if not (
                    content_chars
                    or reasoning_chars
                    or tool_deltas
                    or "content_filter" in finish_reasons
                ):
                    hint = (
                        "请降低思考档位或提高输出 token 上限。"
                        if "length" in finish_reasons
                        else "请检查上游响应与生成参数。"
                    )
                    raise IncompleteResponseError(
                        f"上游流式响应没有正文、思考或工具调用（结束原因：{reasons}）。{hint}"
                    )
                if reasoning_chars and not (content_chars or tool_deltas):
                    print(
                        f"⚠️ [空正文] {self.request_obj.model}：上游仅返回思考，客户端可能显示为空。"
                        f"结束原因={reasons}；请检查思考档位和输出 token 上限。"
                    )
                stats.add_tokens(final_p_tk, final_c_tk)
                for payload in deferred_finishes:
                    yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

                # P1-8：Express 真流式此前从不发 usage 块，客户端只能显示 0；
                # 上游没给 usage 时退三个标量（全 0）。
                if wants_usage(self.request_obj):
                    yield make_usage_chunk(
                        self.response_id_for_stream,
                        self.request_obj.model,
                        final_usage
                        or {
                            "prompt_tokens": final_p_tk,
                            "completion_tokens": final_c_tk,
                            "total_tokens": final_t_tk,
                        },
                    )

                yield "data: [DONE]\n\n"
                print(
                    f"✅ [Express] {self.request_obj.model} | 流式传输顺利完毕 | 耗时 {time.monotonic() - stream_started:.1f}s"
                )
                return

            except asyncio.CancelledError:
                print(
                    f"ℹ️ [客户端断开] 真流式响应期间客户端已断开，模型 {self.model_to_call} 的请求已安全终止。"
                )
                raise
            except Exception as e_stream_call:
                last_error = e_stream_call
                close = getattr(stream_gen_obj, "aclose", None)
                if close:
                    with suppress(Exception):
                        await close()
                stream_gen_obj = None
                is_retryable = is_retryable_exception(e_stream_call)
                # 坏凭证（鉴权失效/欠费）：告警 + 隔离 + 立即切换下一把 Key
                bad_credential = _warn_bad_credential(
                    self.credential_describer, e_stream_call, self.warned_credentials
                )

                # Older endpoints can reject the preview argument-stream
                # field. Remove only that field, once, before any output.
                fcc = self.gen_config_dict.get("tool_config", {}).get(
                    "function_calling_config", {}
                )
                if (
                    not has_yielded
                    and fcc.get("stream_function_call_arguments")
                    and error_status(e_stream_call) == 400
                    and any(
                        term in str(e_stream_call).lower().replace("_", "")
                        for term in ("streamfunctioncallarguments", "partialargs")
                    )
                ):
                    fcc.pop("stream_function_call_arguments", None)
                    print("↩️ [工具参数流] 上游不支持增量参数流，退回完整参数；首回复超时仍按配置执行。")
                    continue

                # location 钉定路径不对 → 退回裸模型名再试一次（仅在还没输出内容时）
                if (
                    self.fallback_model
                    and self.model_to_call != self.fallback_model
                    and not has_yielded
                    and is_location_pin_failure(e_stream_call)
                ):
                    self.model_to_call, self.current_client = _pin_failure_fallback(
                        self.fallback_model, e_stream_call, self.fallback_client
                    )
                    continue

                # 关键修复：只有在“尚未向客户端输出任何内容”时才重试；
                # 否则重试会导致整段答案重复输出（前半段 + 完整重发）。
                # 坏凭证同样必须未输出内容才允许切换（半截答案绝不重发）。
                if (
                    (is_retryable or bad_credential)
                    and not has_yielded
                    and not target_unavailable
                    and attempt < max_retries
                ):
                    if bad_credential:
                        # 坏凭证立即切换，退避等待没有意义（对齐上游隔离语义）
                        wait_time = 0.0
                        print(
                            f"⚠️ [凭证隔离] 检测到坏凭证（{type(e_stream_call).__name__}），"
                            f"跳过退避等待，立即切换下一个 Express Key。"
                        )
                    else:
                        # F-5：退避时长改读控制台的 retry_backoff_seconds，
                        # 原先硬编码 2**(attempt%4)，控制台设置对真流式完全没作用。
                        wait_time = exception_retry_delay(
                            e_stream_call, backoff_sec, attempt, jitter_sec
                        )
                        code, _ = extract_upstream_error(e_stream_call)
                        elapsed = time.monotonic() - attempt_started
                        # 首回复超时等有完整中文说明的异常直接展示 message，
                        # 与 429 重试日志一致可读，不再只报英文类名。
                        detail = (
                            str(e_stream_call).rstrip("。")
                            if isinstance(e_stream_call, FirstReplyTimeout)
                            else type(e_stream_call).__name__
                        )
                        print(
                            f"⚠️ [自动重试] Agent Platform Express Mode 流式请求失败（HTTP {code}，"
                            f"{detail}，本次已等待 {elapsed:.1f} 秒）。"
                            f"第 {attempt + 1} 次退避重试，等待 {wait_time:.1f} 秒。"
                        )
                    if await _request_disconnected(self.fastapi_request):
                        print(
                            f"ℹ️ [客户端断开] 重试前检测到客户端已断开，停止调用模型 {self.model_to_call}。"
                        )
                        return
                    # 退避等待期间持续吐 SSE 心跳，保活前端连接
                    _waited = 0.0
                    while _waited < wait_time:
                        step = min(3.0, wait_time - _waited)
                        if not await wait_for_retry(self.fastapi_request, step):
                            print(
                                f"ℹ️ [客户端断开] 重试等待期间检测到客户端已断开，停止调用模型 {self.model_to_call}。"
                            )
                            return
                        _waited += step
                        yield ": keep-alive\n\n"
                    attempt += 1
                    retry_pending = True
                    continue

                err_msg_detail_stream = f"Gemini 流式请求异常（模型：{self.model_to_call}）：{type(e_stream_call).__name__} - {str(e_stream_call)}"
                print(
                    f"❌ [API 错误响应] 流式连接异常中断 (Model: {self.model_to_call})。错误详情: {err_msg_detail_stream}"
                )
                s_err = str(e_stream_call)
                s_err = s_err[:1024] + "..." if len(s_err) > 1024 else s_err
                if self.is_auto_attempt:
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

    async def collect_response(self):
        track_configure(mode="json", upstream_model=self.model_to_call)
        # 手动退避重试循环（替代 tenacity），以便在每次重试前检测客户端断开
        max_retries, backoff_sec, jitter_sec = get_retry_settings()
        fr_seconds = timeout_seconds(self.request_obj.model)
        response_obj_call = None
        # 总尝试次数 = retry_max + 1，retry_max=0 时仍会请求一次
        attempt = 0
        retry_pending = False
        last_error = None
        while attempt <= max_retries:
            if await _request_disconnected(self.fastapi_request):
                print(
                    f"ℹ️ [客户端断开] 非流式请求前检测到客户端已断开，停止调用模型 {self.model_to_call}。"
                )
                return JSONResponse(
                    status_code=499,
                    content=create_openai_error_response(
                        499, "客户端已断开连接，请求已取消。", "client_closed_request"
                    ),
                )
            target_unavailable = False
            try:
                if retry_pending:
                    if not await self.select_retry_target(last_error):
                        target_unavailable = True
                        raise last_error or RuntimeError("没有可用的 Express API Key。")
                    stats.add_retry()
                    retry_pending = False
                upstream_attempt()
                response_obj_call = await timed_call(
                    self.request_obj.model,
                    self.current_client.aio.models.generate_content(
                        model=self.model_to_call,
                        contents=self.actual_prompt_for_call,
                        config=self.gen_config_dict,
                    ),
                    whole_call=True,
                    seconds=fr_seconds,
                )
                upstream_chunk()
                break
            except asyncio.CancelledError:
                print(
                    f"ℹ️ [客户端断开] 非流式响应期间客户端已断开，模型 {self.model_to_call} 的请求已安全终止。"
                )
                raise
            except Exception as e_call:
                last_error = e_call
                if (
                    self.fallback_model
                    and self.model_to_call != self.fallback_model
                    and is_location_pin_failure(e_call)
                ):
                    self.model_to_call, self.current_client = _pin_failure_fallback(
                        self.fallback_model, e_call, self.fallback_client
                    )
                    continue
                # 坏凭证（鉴权失效/欠费）：告警 + 隔离 + 立即切换下一把 Key
                bad_credential = _warn_bad_credential(
                    self.credential_describer, e_call, self.warned_credentials
                )
                if (
                    (is_retryable_exception(e_call) or bad_credential)
                    and not target_unavailable
                    and attempt < max_retries
                ):
                    if bad_credential:
                        print(
                            f"⚠️ [凭证隔离] 非流式检测到坏凭证（{e_call.__class__.__name__}），"
                            f"跳过退避等待，立即切换下一个 Express Key。"
                        )
                    else:
                        wait_time = exception_retry_delay(
                            e_call, backoff_sec, attempt, jitter_sec
                        )
                        detail = (
                            str(e_call).rstrip("。")
                            if isinstance(e_call, FirstReplyTimeout)
                            else e_call.__class__.__name__
                        )
                        print(
                            f"⚠️ [自动重试] 上游繁忙或触发配额限制（{detail}）。第 {attempt + 1} 次退避重试，等待 {wait_time} 秒。"
                        )
                        if not await wait_for_retry(self.fastapi_request, wait_time):
                            return JSONResponse(
                                status_code=499,
                                content=create_openai_error_response(
                                    499,
                                    "客户端已断开连接，请求已取消。",
                                    "client_closed_request",
                                ),
                            )
                    attempt += 1
                    retry_pending = True
                    continue
                raise

        # 兜底：绝不让 None 流到下游的有效性检查里变成一条误导性的“无有效内容”
        if response_obj_call is None:
            print(
                f"❌ [上游无响应] 模型 {self.model_to_call} 在 {max_retries + 1} 次尝试后仍未返回任何响应。"
            )
            return JSONResponse(
                status_code=502,
                content=create_openai_error_response(
                    502, "上游未返回任何响应（重试已耗尽）。", "upstream_error"
                ),
            )

        require_unblocked_response(response_obj_call)
        require_complete_response(response_obj_call)
        if self.text_transport:
            decoder = self.text_transport.decoder()
            response_obj_call = decoder.restore_sdk(response_obj_call)
            decoder.validate_end()
        if not is_gemini_response_valid(response_obj_call):
            error_details = (
                f"Agent Platform 非流式响应无有效内容，模型：{self.model_to_call}。"
            )
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
                                    error_details += (
                                        f"First part text: '{text_preview}'"
                                    )
            else:
                error_details += f"Response type: {type(response_obj_call).__name__}"
            raise ValueError(error_details)

        _record_usage(response_obj_call)

        openai_response_content = convert_to_openai_format(
            response_obj_call, self.request_obj.model
        )
        _prepend_prefill(openai_response_content, self.prefill_text)
        print(f"✅ [Express] {self.request_obj.model} | 非流式响应完成")
        return JSONResponse(content=openai_response_content)


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
    retry_target_factory: Optional[Callable] = None,
    text_transport: Optional[TextTransport] = None,
    credential_describer: Optional[Callable[[], str]] = None,
):
    """Prepare the prompt once, then delegate delivery to a request-local context."""
    actual_prompt_for_call = await run_media_task(prompt_func, request_obj.messages)
    print(
        f"🚀 [上游请求] 正在调用 Agent Platform Express Mode 模型 {model_to_call}，客户端请求模型名为 {request_obj.model}。"
    )
    generation = SDKGeneration(
        current_client=current_client,
        model_to_call=model_to_call,
        gen_config_dict=gen_config_dict,
        request_obj=request_obj,
        actual_prompt_for_call=actual_prompt_for_call,
        is_auto_attempt=is_auto_attempt,
        fastapi_request=fastapi_request,
        prefill_text=prefill_text,
        fallback_model=fallback_model,
        fallback_client_factory=fallback_client_factory,
        retry_target_factory=retry_target_factory,
        text_transport=text_transport,
        credential_describer=credential_describer,
    )
    return await generation.response()

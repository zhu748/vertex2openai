"""Express（SDK）通道管线：调用编排、响应转换与 OpenAI 流式输出。

原 app/api_helpers.py 本体（2026-11 P2-3 归位）：与 cookie 家族对称地住在
upstreams/ 下；app/api_helpers.py 保留为兼容 re-export 层。历史 patch seam
（stats / wait_for_retry / timeout_seconds / track_configure 等）以本模块
为准——patch 兼容层属性不再影响管线内部绑定。
"""

import asyncio
import gc
import inspect
import json
import time
import uuid
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Callable, Dict, List, Optional

import config as app_config
import model_capabilities as mc
from anti_truncation import TextTransport, supports_argument_stream
from express_endpoint import is_location_pin_failure
from fastapi.responses import JSONResponse, Response, StreamingResponse
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
from json_encoding import LargeJSONResponse
from media_tasks import run_media_task
from message_processing import (
    GIANT_CONTENT_THRESHOLD,
    convert_to_openai_format,
)
from models import OpenAIMessage, OpenAIRequest
from prefill import PrefillDeduper, first_frames
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
from streaming import DONE_FRAME, HEARTBEAT_FRAME, MAX_ERROR_MESSAGE_CHARS
from streaming import disconnected as _request_disconnected
from upstream_errors import (
    create_openai_error_response,
    error_status,
    extract_upstream_error,
)
# express_response 的转换件在此合并导入：convert_chunk_to_openai /
# ToolCallIndexer / _chunk_openai_response_dict_for_sse 同时是既有测试经
# api_helpers 访问的再导出面（tests/test_model_retry.py 等），其余为本
# 文件自用。2026-11 审查收敛：原 8 条独立的 `import X as X` 语句合一。
from upstreams.express_response import (  # noqa: F401
    ToolCallIndexer,
    _build_chunk_payload,
    _chunk_openai_response_dict_for_sse,
    _dedup_chunk_payload,
    _prepend_prefill,
    convert_chunk_to_openai,
    is_gemini_response_valid,
    split_giant_chunk_payload,
)
from usage import make_usage_chunk, map_usage, merge_usage_metadata, usage_snapshot_unchanged, wants_usage


async def _cancel_attribution(request: Any) -> str:
    """CancelledError 的归因文案（REQ-P3-4）：区分 ASGI 断连与服务端取消。

    取消来源有三类：客户端断开、服务停机排空（lifespan 取消后台泵）、
    续传超窗 / 请求超时取消（resumable cancel_generation）。旧实现
    一律打印「客户端已断开」，停机与超窗场景的归因会误导排障——
    本项目日志是主要排障手段。
    """
    if await _request_disconnected(request):
        return "客户端已断开"
    return "任务被服务端取消（非客户端断开：可能为服务停机排空或续传超窗/请求超时取消）"


def _pin_failure_fallback(fallback_model: str, error: Exception, get_fallback_client: Optional[Callable]):
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


def _resolve_retry_target(current_client: Any, current_model: str, current_fallback: Optional[str], target: Any):
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


def _response_has_giant_content(openai_response_dict: Optional[Dict[str, Any]]) -> bool:
    """响应任一 choice 的 content 超过巨帧阈值（4K 生图 base64）。"""
    try:
        for choice in (openai_response_dict or {}).get("choices") or []:
            content = ((choice.get("message") or {}).get("content"))
            if isinstance(content, str) and len(content) > GIANT_CONTENT_THRESHOLD:
                return True
    except Exception:
        pass
    return False


async def _fake_stream_retry_target(
    retry_target_factory: Callable,
    last_error: Optional[Exception],
    client: Any,
    model: str,
    fallback_model: Optional[str],
    failed_pin_paths: set,
) -> tuple:
    """假流式重试的目标重选与失败路径记忆（拆分自 gemini_fake_stream_generator）。

    REQ-P1-1：这把 Key 的钉定路径本轮已确认失败，直接改用裸模型名，
    不再重打已知会失败的 projects/locations 路径。返回
    (client, model, fallback_model)；无可选目标时抛 last_error。
    """
    target = _call_retry_factory(retry_target_factory, last_error)
    if inspect.isawaitable(target):
        target = await target
    if target is None:
        raise last_error or RuntimeError("没有可用的 Express API Key。")
    client, model, fallback_model = _resolve_retry_target(
        client, model, fallback_model, target
    )
    if (
        fallback_model
        and model in failed_pin_paths
        and model != fallback_model
    ):
        model = fallback_model
    return client, model, fallback_model


def _fake_stream_error_payload(e: Exception) -> str:
    """外层异常 → SSE 错误帧 JSON（拆分自 gemini_fake_stream_generator）。"""
    msg = str(e)
    if len(msg) > MAX_ERROR_MESSAGE_CHARS:
        msg = msg[:MAX_ERROR_MESSAGE_CHARS] + "..."
    code, _ = extract_upstream_error(e)
    return json.dumps(create_openai_error_response(code, msg, "server_error"))


async def _fake_stream_response_frames(
    raw_gemini_response: Any,
    request_obj: OpenAIRequest,
    *,
    prefill_text: str,
    text_transport: Optional[TextTransport],
    fake_stream_started: float,
) -> AsyncIterator[str]:
    """假流式的响应校验、转换与 SSE 发射（拆分自 gemini_fake_stream_generator）。

    转换链（含生图落盘）搬媒体线程；转换完成后立即释放 SDK 响应引用
    （4K 生图内存关键点），发射完毕对巨帧触发针对性 GC 回收引用环。
    """
    require_unblocked_response(raw_gemini_response)
    require_complete_response(raw_gemini_response)
    if text_transport:
        decoder = text_transport.decoder()
        raw_gemini_response = decoder.restore_sdk(raw_gemini_response)
        decoder.validate_end()
    if not is_gemini_response_valid(raw_gemini_response):
        raise ValueError("上游假流式响应无有效内容。")
    # PRF-P1-1：同非流式路径——转换链含生图落盘，搬进媒体线程。
    openai_response_dict = await run_media_task(
        convert_to_openai_format, raw_gemini_response, request_obj.model,
        heavy=True,
    )
    _prepend_prefill(openai_response_dict, prefill_text)
    _record_usage(raw_gemini_response)
    # 4K 生图内存关键点：响应对象持有整张图的原始 bytes（0.75× base64
    # 体积）；转换完成后本参数引用立即断开（调用方的同名局部与
    # api_call_task 已在进入本函数前置空），避免与后续 SSE 分帧的全量
    # 字符串拷贝叠加驻留（实测 25MB 图旧链路峰值 RSS +345MB，Render
    # 512MB 容器被 OOMKill，进程级 SIGKILL 无任何 Python 异常）。
    raw_gemini_response = None
    # 巨帧标记：发射完成后据此触发针对性 GC（见下）。
    _giant = _response_has_giant_content(openai_response_dict)
    async for chunk_sse in _chunk_openai_response_dict_for_sse(
        openai_response_dict=openai_response_dict
    ):
        if chunk_sse.strip() == DONE_FRAME.strip() and wants_usage(request_obj):
            yield make_usage_chunk(
                openai_response_dict["id"],
                request_obj.model,
                openai_response_dict.get("usage", {}),
            )
        yield chunk_sse
    # 发射完毕先断开本帧对 25MB+ content 的最后一处本地引用（走引用
    # 计数即时释放主体），再触发 GC 回收引用环。
    openai_response_dict = None
    if _giant:
        # 巨帧（4K 生图 base64）发射完毕后主动回收：大字符串/bytes 靠
        # 引用计数即时释放，但 SDK 响应树与转换链上的 pydantic 对象
        # 可能成环拖住 arenas 不还给 OS；一次针对性 gc.collect（毫秒
        # 级）让 RSS 尽快回落，给下一张图/别的请求腾出余量。
        gc.collect()
    print(
        f"✅ [Express] {request_obj.model} | 假流式传输完成 | 耗时 {time.monotonic() - fake_stream_started:.1f}s"
    )


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
) -> AsyncIterator[str]:
    print(
        f"🌊 [假流式] 已开始调用 Gemini 模型 {model_for_api_call}，客户端请求模型名为 {request_obj.model}。"
    )
    fake_stream_started = time.monotonic()
    warned_credentials: set = set()
    # REQ-P1-1：本轮已确认失败的 location 钉定路径集合（含 Project ID，
    # 天然按键区分）。换 Key 重试会把 model_for_api_call 重新置回钉定路径，
    # 不记忆的话同一逻辑请求的配额烧毁最坏放大 3 倍。
    failed_pin_paths: set = set()

    # P1-6：不再使用 tenacity 的硬编码 20 次，改为与真流式/非流式一致的手写退避，
    # 读取控制台的 retry_max / retry_backoff_seconds / retry_jitter_seconds
    # （固定间隔 + 抖动，不随次数放大），并在等待期间检测客户端断开。
    # 注：整取路径不参与首回复计时（2026-11 语义，见 first_reply.timed_call），
    # 此处无需预读首回复窗口。
    max_retries, backoff_sec, jitter_sec = get_retry_settings()

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
                    gemini_client_instance, model_for_api_call, fallback_model = (
                        await _fake_stream_retry_target(
                            retry_target_factory,
                            last_error,
                            gemini_client_instance,
                            model_for_api_call,
                            fallback_model,
                            failed_pin_paths,
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
                    # 整取（假流式）不参与首回复计时。
                    whole_call=True,
                )
            )

            # 等待期间持续吐 keep-alive，避免前端因长时间无字节而超时
            # （REQ-P3-7：改注释帧，与真流式同口径——data 帧会进续传缓冲
            # 占用事件序号，断线重放时客户端收到大量空 delta；注释帧
            # 由 pump_stream 过滤，不入库不占序号）
            while not api_call_task.done():
                if outer_keep_alive_interval > 0 and time.monotonic() >= heartbeat_due:
                    yield HEARTBEAT_FRAME
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
                    # REQ-P1-1：记录失败路径，重选到同一把 Key 时上方
                    # retry_pending 分支会直接改用裸模型名，避免重复白烧。
                    failed_pin_paths.add(model_for_api_call)
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
                        # 退避计时改单调时钟 deadline（2026-11 P2-6 修复）：旧
                        # 写法按「名义步长」累加 waited，事件循环繁忙（生图 GC、
                        # 巨帧序列化）时 asyncio.sleep 的实际耗时远超 0.5s，
                        # 名义累加却不变——设定 5 秒的退避在负载下可能实际拖到
                        # 10-20 秒。与同文件 stream_frames / collect_response
                        # 及 retry_policy.wait_for_retry 的 deadline 口径对齐。
                        deadline = time.monotonic() + wait_time
                        while True:
                            remaining = deadline - time.monotonic()
                            if remaining <= 0:
                                break
                            await asyncio.sleep(min(0.5, remaining))
                            if await _request_disconnected(fastapi_request):
                                print(
                                    "ℹ️ [客户端断开] 假流式退避期间客户端已断开，停止重试。"
                                )
                                return
                            if (
                                outer_keep_alive_interval > 0
                                and time.monotonic() >= heartbeat_due
                            ):
                                # REQ-P3-7：注释帧心跳（不入续传缓冲、不占序号）
                                yield HEARTBEAT_FRAME
                                heartbeat_due = (
                                    time.monotonic() + outer_keep_alive_interval
                                )
                    attempt += 1
                    retry_pending = True
                    continue
                raise

        if raw_gemini_response is None:
            raise last_error or ValueError("上游未返回任何响应（重试已耗尽）。")

        # 4K 生图内存关键点：发射前断开本层对 SDK 响应树的最后两处引用
        # （api_call_task 的结果仍引用着整张图的原始 bytes）。
        response_arg = raw_gemini_response
        raw_gemini_response = None
        api_call_task = None
        async for frame in _fake_stream_response_frames(
            response_arg,
            request_obj,
            prefill_text=prefill_text,
            text_transport=text_transport,
            fake_stream_started=fake_stream_started,
        ):
            yield frame

    except asyncio.CancelledError:
        print(
            f"ℹ️ [取消归因] {await _cancel_attribution(fastapi_request)}，"
            f"假流式生成器正在清理模型 {request_obj.model} 的后台任务。"
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
        json_payload_error = _fake_stream_error_payload(e_outer_gemini)
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


class _StreamMeters:
    """单次流式尝试的用量与内容计量（QUA-P1-1 拆分，仿 PendingMetadataGate
    先例：旧实现是 stream_frames 循环体内的 13 个零散局部变量）。

    has_yielded 不在此类：它是跨尝试不变量（已向客户端输出过内容就
    绝不重试/降级重发），由编排壳持有。
    """

    __slots__ = (
        "received_chunks", "content_chars", "reasoning_chars", "tool_deltas",
        "usage_metadata", "final_usage", "final_p_tk", "final_c_tk",
        "final_t_tk", "seen_candidates", "finished_candidates",
        "finish_reasons", "deferred_finishes",
    )

    def __init__(self):
        self.received_chunks = 0
        self.content_chars = 0
        self.reasoning_chars = 0
        self.tool_deltas = 0
        # 完整 usage dict（含 prompt_tokens_details.cached_tokens /
        # completion_tokens_details.reasoning_tokens 细分）：尾块直接透传，
        # 与假流式/非流式口径一致；旧实现只传三个标量，Claude 生态的
        # cache_read_input_tokens 与 thinking 细分在真流式下永远缺失
        # （2026-10 修复）。
        self.final_usage: Dict[str, Any] = {}
        self.usage_metadata: Dict[str, Any] = {}
        self.final_p_tk = 0
        self.final_c_tk = 0
        self.final_t_tk = 0
        self.seen_candidates: set = set()
        self.finished_candidates: set = set()
        self.finish_reasons: set = set()
        self.deferred_finishes: list = []

    def note_usage(self, usage_meta: Optional[dict]) -> None:
        """累计上游 usage 快照（trace 用量口径与旧 _extract_usage 一致）。

        计数器未变化的累计快照直接短路（2026-11 O-5）：逐帧
        pydantic model_dump 是长流式的隐性热点。
        """
        if usage_snapshot_unchanged(self.usage_metadata, usage_meta):
            return
        self.usage_metadata = merge_usage_metadata(self.usage_metadata, usage_meta)
        self.final_usage = map_usage(self.usage_metadata)
        record_usage(self.final_usage)
        self.final_p_tk = self.final_usage["prompt_tokens"]
        self.final_c_tk = self.final_usage["completion_tokens"]
        self.final_t_tk = self.final_usage["total_tokens"]

    def note_candidates(self, chunk: Any) -> None:
        """n>1：按候选序号登记出现与结束标记。"""
        for ci, candidate in enumerate(chunk.candidates or []):
            index = candidate.index if candidate.index is not None else ci
            self.seen_candidates.add(index)
            if has_finish_reason(candidate.finish_reason):
                self.finished_candidates.add(index)

    def note_delta(self, delta: dict) -> None:
        """正文/思考/工具增量计量（完成日志与空响应校验的数据源）。"""
        if delta.get("content"):
            self.content_chars += len(delta["content"])
        self.reasoning_chars += len(delta.get("reasoning_content") or "")
        self.tool_deltas += len(delta.get("tool_calls") or [])

    def has_any_output(self) -> bool:
        return bool(self.content_chars or self.reasoning_chars or self.tool_deltas)


def _arg_stream_rejected(exc: Exception, gen_config_dict: Dict[str, Any]) -> bool:
    """上游拒绝预览版参数流字段（旧端点）：仅此场景允许降级重试一次。

    判定与旧实现一致：请求携带 stream_function_call_arguments、上游
    返回 400 且错误文案点名 streamFunctionCallArguments / partialArgs
    （下划线归一后匹配）。字段由调用方摘除，只降级一次。
    """
    if error_status(exc) != 400:
        return False
    fcc = gen_config_dict.get("tool_config", {}).get(
        "function_calling_config", {}
    )
    if not fcc.get("stream_function_call_arguments"):
        return False
    return any(
        term in str(exc).lower().replace("_", "")
        for term in ("streamfunctioncallarguments", "partialargs")
    )


def _retry_wait_time(
    exc: Exception,
    *,
    bad_credential: bool,
    backoff_sec: float,
    attempt: int,
    jitter_sec: float,
    attempt_started: float,
) -> float:
    """可重试失败的退避时长（0.0 = 坏凭证立即切换，对齐上游隔离语义）。"""
    if bad_credential:
        print(
            f"⚠️ [凭证隔离] 检测到坏凭证（{type(exc).__name__}），"
            f"跳过退避等待，立即切换下一个 Express Key。"
        )
        return 0.0
    # F-5：退避时长改读控制台的 retry_backoff_seconds，原先硬编码
    # 2**(attempt%4)，控制台设置对真流式完全没作用。
    wait_time = exception_retry_delay(exc, backoff_sec, attempt, jitter_sec)
    code, _ = extract_upstream_error(exc)
    elapsed = time.monotonic() - attempt_started
    # 首回复超时等有完整中文说明的异常直接展示 message，与 429 重试
    # 日志一致可读，不再只报英文类名。
    detail = (
        str(exc).rstrip("。")
        if isinstance(exc, FirstReplyTimeout)
        else type(exc).__name__
    )
    print(
        f"⚠️ [自动重试] Agent Platform Express Mode 流式请求失败（HTTP {code}，"
        f"{detail}，本次已等待 {elapsed:.1f} 秒）。"
        f"第 {attempt + 1} 次退避重试，等待 {wait_time:.1f} 秒。"
    )
    return wait_time


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
    # REQ-P1-1（2026-11 审查）：本轮已确认失败的 location 钉定路径集合。
    # 换 Key 重试会把 model_to_call 重新置回该 Key 的钉定路径，若不记忆，
    # 同一逻辑请求最坏可打 (retry_max+1)×N 次上游、配额烧毁最多放大 3 倍。
    # 用路径字符串而非客户端实例做身份：FailoverSDKClient 包装每次换 Key
    # 都新建，id() 不稳定；而钉定路径含 Project ID，天然按键区分。
    failed_pin_paths: set = field(default_factory=set)
    response_id_for_stream: str = ""

    async def response(self) -> Response:
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

    def fallback_client(self) -> Any:
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
        # REQ-P1-1：这把 Key 的钉定路径本轮已确认失败（404 模型不在该区域 /
        # 403 项目未开计费）——直接改用裸模型名，不再重打已知会失败的
        # projects/locations 路径白烧一次上游调用。
        if (
            self.fallback_model
            and self.model_to_call in self.failed_pin_paths
            and self.model_to_call != self.fallback_model
        ):
            self.model_to_call = self.fallback_model
        return True

    async def stream_frames(self) -> AsyncIterator[str]:
        """Express 真流式交付的编排壳（QUA-P1-1 拆分后只负责重试与错误分类）。

        单次尝试的帧发射在 _emit_attempt_frames（计量经 _StreamMeters）；
        本层保留：断连检测、换 Key 重选、参数流降级 / 钉定路径回退 /
        坏凭证立即切换 / 可重试退避、以及已输出内容后的显式错误收尾。
        """
        # 路径兜底和换 Key 都更新本次请求持有的模型与客户端。
        max_retries, backoff_sec, jitter_sec = get_retry_settings()
        fr_seconds = timeout_seconds(self.request_obj.model)
        # 是否已向客户端输出过内容：跨尝试不变量（半截答案绝不重发）。
        # 编排壳按「是否有任何帧经过本生成器」判定，比旧实现（首个真实
        # 输出帧后置位）略保守一拍，只影响亚毫秒窗口内的重试决策，方向
        # 更安全（宁可不重试，不可重复输出）。
        has_yielded = False
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
                async for frame in self._emit_attempt_frames(
                    first_reply, stream_gen_obj, attempt, attempt_started,
                    stream_started,
                ):
                    has_yielded = True
                    yield frame
                return

            except asyncio.CancelledError:
                print(
                    f"ℹ️ [取消归因] {await _cancel_attribution(self.fastapi_request)}，"
                    f"真流式请求模型 {self.model_to_call} 已安全终止。"
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
                if (
                    not has_yielded
                    and _arg_stream_rejected(e_stream_call, self.gen_config_dict)
                ):
                    fcc = self.gen_config_dict["tool_config"][
                        "function_calling_config"
                    ]
                    fcc.pop("stream_function_call_arguments", None)
                    print("↩️ [工具参数流] 上游不支持增量参数流，退回完整参数；首回复超时仍按配置执行。")
                    continue

                # location 钉定路径不对 → 退回裸模型名再试一次（仅在还没输出内容时）。
                # REQ-P1-1：记录失败路径；重选到同一把 Key 时 select_retry_target
                # 会直接用裸模型名，不会再次走到这里。
                if (
                    self.fallback_model
                    and self.model_to_call != self.fallback_model
                    and not has_yielded
                    and is_location_pin_failure(e_stream_call)
                ):
                    self.failed_pin_paths.add(self.model_to_call)
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
                    wait_time = _retry_wait_time(
                        e_stream_call,
                        bad_credential=bad_credential,
                        backoff_sec=backoff_sec,
                        attempt=attempt,
                        jitter_sec=jitter_sec,
                        attempt_started=attempt_started,
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
                s_err = (s_err[:MAX_ERROR_MESSAGE_CHARS] + "..."
                         if len(s_err) > MAX_ERROR_MESSAGE_CHARS else s_err)
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

    async def _emit_attempt_frames(
        self, first_reply: FirstReplyGuard, stream_gen_obj: Any, attempt: int, attempt_started: float,
        stream_started: float,
    ):
        """单次尝试的帧发射（QUA-P1-1 拆分自 stream_frames）。

        逐尝试装配（工具索引 / 参数流合并器 / 去重器 / 预填充先行帧 /
        元数据暂存门）在此重建；计量经 _StreamMeters；正常路径以 usage
        块 + [DONE] 收尾；异常原样抛给编排壳做错误分类与重试决策。
        """
        meters = _StreamMeters()
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
        _pf_frames = first_frames(
            self.response_id_for_stream,
            self.request_obj.model,
            self.prefill_text,
            self.request_obj.n or 1,
        )
        # 按 choice index 追踪首帧：OpenAI 官方帧序的首个 delta
        # 携带 role:"assistant"（Cookie 通道同口径；旧实现缺失，
        # 严格按官方 SDK 消费流的客户端会识别不出角色）。
        roles_sent = set()
        pending_metadata = PendingMetadataGate()
        has_yielded = False

        async for chunk_item_call in first_reply.iterate(
            stream_gen_obj, sdk_has_reply
        ):
            upstream_chunk()
            meters.received_chunks += 1
            if meters.received_chunks == 1:
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
                meters.note_usage(chunk_item_call.usage_metadata)

            # 支持 n>1：按候选序号逐个输出
            require_unblocked_response(chunk_item_call)
            meters.note_candidates(chunk_item_call)
            if text_decoder:
                chunk_item_call = text_decoder.restore_sdk(chunk_item_call)
            if tool_assembler:
                chunk_item_call = tool_assembler.restore_sdk(chunk_item_call)
            num_candidates = len(
                getattr(chunk_item_call, "candidates", None) or []
            )
            for ci in range(num_candidates):
                payload = self._prepare_candidate_payload(
                    chunk_item_call, ci,
                    tool_indexer=tool_indexer, deduper=deduper,
                    roles_sent=roles_sent,
                )
                if payload is None:
                    continue  # 正文暂存于去重器，跳过空 chunk
                choice = (payload.get("choices") or [{}])[0]
                delta = choice.get("delta") or {}
                if delta.get("content") and not meters.content_chars:
                    elapsed = time.monotonic() - (
                        attempt_started_at() or attempt_started
                    )
                    print(
                        f"✍️ [首段正文] {self.request_obj.model}：第 {attempt + 1} 次尝试等待 {elapsed:.1f} 秒开始输出正文。"
                    )
                meters.note_delta(delta)
                if choice.get("finish_reason"):
                    meters.finish_reasons.add(choice["finish_reason"])
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
                if choice.get("finish_reason") and not meters.has_any_output():
                    # 有些前端收到 finish_reason 就结束读取；先校验空响应，
                    # 避免成功结束帧遮住后续的明确错误。
                    meters.deferred_finishes.append(payload)
                    continue
                if not first_reply.ready:
                    # 序列化一次复用：测量与最终下发共用同一份字符串
                    # （旧实现每个元数据 chunk 序列化两次）；巨帧
                    # （>1MiB delta.content，生图/未来直出路径）先拆
                    # 成 256KiB 增量帧再落缓冲，避免单帧三份全量拷贝。
                    for _sub in split_giant_chunk_payload(payload):
                        frame_text = json.dumps(_sub, ensure_ascii=False)
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
                # 巨帧拆分（同上）：单帧 json.dumps + f-string + ASGI
                # encode 瞬间三份全量拷贝，512MB 容器叠加即 OOMKill。
                for _sub in split_giant_chunk_payload(payload):
                    yield f"data: {json.dumps(_sub, ensure_ascii=False)}\n\n"

        if tool_assembler:
            tool_assembler.validate_end()
        if text_decoder:
            text_decoder.validate_end()
        if meters.seen_candidates - meters.finished_candidates:
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
                meters.content_chars += len(tail)
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

        if meters.final_p_tk > 0 or meters.final_c_tk > 0:
            print(
                f"💰 [算力消耗统计] 提示词: {meters.final_p_tk} | 思考与生成: {meters.final_c_tk} | 总计: {meters.final_t_tk} Tokens"
            )
        reasons = ",".join(sorted(meters.finish_reasons)) or "无"
        print(
            f"📊 [流式完成] {self.request_obj.model}：正文 {meters.content_chars} 字符，思考 {meters.reasoning_chars} 字符，"
            f"工具增量 {meters.tool_deltas}，结束原因={reasons}。"
        )
        # 结束标记或签名单独到达也会设置 has_yielded，但它们不是回复内容。
        # 安全拦截允许空正文；只有思考的合法响应保留，同时明确提示诊断。
        if not (
            meters.content_chars
            or meters.reasoning_chars
            or meters.tool_deltas
            or "content_filter" in meters.finish_reasons
        ):
            hint = (
                "请降低思考档位或提高输出 token 上限。"
                if "length" in meters.finish_reasons
                else "请检查上游响应与生成参数。"
            )
            raise IncompleteResponseError(
                f"上游流式响应没有正文、思考或工具调用（结束原因：{reasons}）。{hint}"
            )
        if meters.reasoning_chars and not (
            meters.content_chars or meters.tool_deltas
        ):
            print(
                f"⚠️ [空正文] {self.request_obj.model}：上游仅返回思考，客户端可能显示为空。"
                f"结束原因={reasons}；请检查思考档位和输出 token 上限。"
            )
        stats.add_tokens(meters.final_p_tk, meters.final_c_tk)
        # REQ-P2-1（2026-11 收敛）：content_filter 等空响应路径首回复从未
        # 就绪，pending_metadata 里攒的思考签名/元数据帧此前随流结束静默
        # 丢弃——Gemini 3.x 多轮工具对话强校验签名，丢签名会让下一轮
        # 请求直接 400。与首回复分支同口径：先按原顺序冲刷暂存元数据，
        # 再下发延迟的结束帧（此处不补发预填充先行帧：其后没有真实
        # 正文跟随，正常流此 drain 恒为空，见 first_reply.iterate 的
        # ready 先于 yield 置位语义）。
        for metadata in pending_metadata.drain():
            yield f"data: {metadata}\n\n"
        for payload in meters.deferred_finishes:
            yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

        # P1-8：Express 真流式此前从不发 usage 块，客户端只能显示 0；
        # 上游没给 usage 时退三个标量（全 0）。
        if wants_usage(self.request_obj):
            yield make_usage_chunk(
                self.response_id_for_stream,
                self.request_obj.model,
                meters.final_usage
                or {
                    "prompt_tokens": meters.final_p_tk,
                    "completion_tokens": meters.final_c_tk,
                    "total_tokens": meters.final_t_tk,
                },
            )

        yield "data: [DONE]\n\n"
        print(
            f"✅ [Express] {self.request_obj.model} | 流式传输顺利完毕 | 耗时 {time.monotonic() - stream_started:.1f}s"
        )
        return

    def _prepare_candidate_payload(
        self, chunk_item_call: Any, ci: int, *, tool_indexer: ToolCallIndexer, deduper: Optional[PrefillDeduper], roles_sent: set
    ):
        """单候选帧的载荷准备（QUA-P1-1 拆分）：构建 → 去重 → role 注入。

        返回 payload；None 表示去重器暂存正文或空 chunk，调用方跳过。
        （空响应先行的结束帧延迟逻辑在调用方：需要结合整轮计量判定。）
        """
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
                return None
        choice = (chunk_payload.get("choices") or [{}])[0]
        delta = choice.get("delta") or {}
        # 首帧补 role（预填充先行帧已自带，不重复注入）。
        if (
            not self.prefill_text
            and choice.get("index") not in roles_sent
        ):
            choice["delta"] = {"role": "assistant", **delta}
            roles_sent.add(choice.get("index"))
        return chunk_payload

    async def collect_response(self) -> Response:
        # 与流式路径（response()）及 Cookie 通道（cookie_proxy 两模式都上报
        # thinking_level）对齐：非流式整取同样上报思考档位与输出上限。旧实现
        # 只报 mode/upstream_model，请求监控对 json 模式永远显示「默认 / 默认」，
        # 让人误以为控制台强制档位（native_thinking_mode=console）没生效——
        # 实际 gen_config_dict 在 chat_completions 里就已带上 thinking_config，
        # 流式与非流式共用同一份配置，仅监控字段缺失。
        _thinking = self.gen_config_dict.get("thinking_config") or {}
        track_configure(
            mode="json",
            upstream_model=self.model_to_call,
            thinking_level=_thinking.get("thinking_level"),
            max_output_tokens=self.gen_config_dict.get("max_output_tokens"),
        )
        # 手动退避重试循环（替代 tenacity），以便在每次重试前检测客户端断开
        max_retries, backoff_sec, jitter_sec = get_retry_settings()
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
                    # 整取（非流式）不参与首回复计时。
                    whole_call=True,
                )
                upstream_chunk()
                break
            except asyncio.CancelledError:
                print(
                    f"ℹ️ [取消归因] {await _cancel_attribution(self.fastapi_request)}，"
                    f"非流式请求模型 {self.model_to_call} 已安全终止。"
                )
                raise
            except Exception as e_call:
                last_error = e_call
                if (
                    self.fallback_model
                    and self.model_to_call != self.fallback_model
                    and is_location_pin_failure(e_call)
                ):
                    # REQ-P1-1：记录失败路径；重选到同一把 Key 时
                    # select_retry_target 会直接改用裸模型名，避免重复白烧。
                    self.failed_pin_paths.add(self.model_to_call)
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

        # PRF-P1-1：转换链内含生图 base64 解码 + 落盘（4K 可达数十 MB，
        # 100-500ms 同步停顿），整体搬进媒体线程，事件循环只等结果。
        openai_response_content = await run_media_task(
            convert_to_openai_format, response_obj_call, self.request_obj.model,
            heavy=True,
        )
        # 与假流式路径同因：释放 SDK 响应（持有图片原始 bytes），
        # 避免与 JSON 序列化拷贝叠加驻留。
        response_obj_call = None
        _prepend_prefill(openai_response_content, self.prefill_text)
        # 巨帧（4K 生图）主动回收 SDK/pydantic 引用环（同假流式路径）。
        # Encode giant strings in bounded slices into the final byte buffer;
        # unlike pre-serializing with dumps(), this also avoids its accumulator
        # and the complete JSON-text -> UTF-8 copy.
        if _response_has_giant_content(openai_response_content):
            gc.collect()
            print(f"✅ [Express] {self.request_obj.model} | 非流式响应完成")
            return LargeJSONResponse(content=openai_response_content)
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
) -> Response:
    """Prepare the prompt once, then delegate delivery to a request-local context."""
    # PRF-P1-5：prompt 构建是每请求必经的轻载路径，走 prep 池，
    # 绝不排在重图片解码（media 池）后面。
    actual_prompt_for_call = await run_media_task(prompt_func, request_obj.messages, heavy=False)
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
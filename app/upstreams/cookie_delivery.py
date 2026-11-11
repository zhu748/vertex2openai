"""Cookie 通道的四条交付路径（2026-11 P2-2 自 cookie_proxy.py 拆出）。

真流式（deliver_stream_frames / deliver_stream_events）、非流式整取
（deliver_collect_response）、生图假流式（deliver_image_frames）与缓冲
假流式（deliver_buffered_frames）。CookieGeneration 是每请求的凭证快照
与交付配置（上下文对象 gen）；本模块函数不共享 self，全部状态经 gen
传递或函数局部持有。请求构造在 cookie_request，事件解析在
cookie_response / cookie_transport。
"""

import asyncio
import json
import time
import traceback

from anti_truncation import buffered_sse
from fastapi.responses import JSONResponse
import image_delivery
from media_tasks import run_media_task
from upstreams.cookie_request import build_request_body_async
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    # 仅供类型注解：cookie_proxy 导入本模块，运行期反向导入会成环
    from upstreams.cookie_proxy import CookieGeneration
    from fastapi import Request
    from fastapi.responses import Response
    from models import OpenAIRequest

import config as app_config
from cookie_auth import BATCH_GRAPHQL_URL, build_headers
from credential_manager import credential_manager
from first_reply import timed_call
from impersonate import open_upstream_client
from logger import stats
from json_encoding import LargeJSONResponse
from message_processing import _create_safety_ratings_html
from prefill import PrefillDeduper, strip_prefill_overlap
from request_tracking import (
    upstream_attempt,
    upstream_chunk,
)
from response_validation import (
    FINISH_CONTENT_FILTER,
    AntiTruncationError,
    IncompleteResponseError,
    map_finish_reason_to_openai,
    require_finish_reason,
)
from retry_policy import is_retryable_exception, is_retryable_status
from runtime_state import app_state
from streaming import HEARTBEAT, HEARTBEAT_FRAME, with_heartbeats
from thought_signatures import (
    ordinary_part_metadata,
    thought_signature_extra,
)
from upstream_errors import create_openai_error_response, extract_upstream_error
from upstreams.cookie_response import (
    BODY_EVENT_TYPES,
    CompleteTextObjectCursor,
    RawSampler,
    apply_wire_signature,
    extract_events,
    make_openai_chunk,
    iter_openai_content_chunks,
    openai_tool_call,
)
from upstreams.cookie_transport import (
    api_error_details,
    cookie_retry_backoff,
    cookie_retry_backoff_heartbeats,
    is_native_tool_error_response,
    stream_events,
)
from usage import (
    estimate_usage,
    make_usage_chunk,
    map_usage,
    merge_usage_metadata,
    messages_prompt_text,
)

_THINKING_RUNAWAY_HINT = (
    "这通常是原生思考在高强度下跑飞或被截断（前端如 SillyTavern 常发 reasoning_effort=xhigh，"
    "会覆盖控制台档位）。解决：控制台“模型参数→思考强度→原生思考控制”选“关闭原生思考”"
    "（可用“保存为该模型专属”只对本模型生效），即忽略前端强度、压到 minimal 并剥离原生思考。"
)


_NO_BODY_HINT = "可能被安全策略拦截或接口行为变化；原始响应样本已写入运行日志，可到大盘“运行日志”页查看。"




def _no_body_detail(finish_raw: Optional[str], blocked_msg: Optional[str]) -> str:
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
    finish_raw: Optional[str], blocked_msg: Optional[str], has_thought: bool, strip_thoughts: bool
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
        project_id: str,
        base_model_name: str,
        request_obj: "OpenAIRequest",
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


def _body_cache_for(gen: "CookieGeneration") -> _RequestBodyCache:
    """四条交付路径共用的请求体缓存构造（QUA-P1-2：流式/非流式两处
    五参构造逐字重复收敛；生图整取走 _collect_full_response 的单形态
    构建不带降级）。"""
    return _RequestBodyCache(
        gen.project_id,
        gen.base_model_name,
        gen.request_obj,
        prefill_active=gen.prefill_active,
        force_search=gen.force_builtin_search,
    )


async def _attempt_context(gen: "CookieGeneration", body_cache: _RequestBodyCache) -> tuple:
    """单次尝试的 (body, fallback_body, req_headers) 装配（QUA-P1-2 收敛）。

    body 按「原生未锁定」取形态；降级 body 惰性构建（原生 Schema 首次
    被拒才建），degraded() 会把降级模式锁定到后续重试；请求头构建与
    调试日志同构。流式/非流式两条交付路径逐字共享此前是三段复制。
    """
    use_native_tools = not body_cache.native_latched
    body = await body_cache.get(use_native_tools)
    # 降级 body 惰性构建（原生 Schema 首次被拒才建）；
    # degraded() 会把降级模式锁定到后续重试。
    fallback_body = (
        body_cache.degraded
        if gen.native_custom_tools
        and use_native_tools
        and not gen.text_transport
        else None
    )
    req_headers = build_headers(gen.cookie_str)
    if gen.cookie_debug:
        print(
            f"🔎 [Studio 调试] 出站 generationConfig: "
            f"{json.dumps(body['variables'].get('generationConfig', {}), ensure_ascii=False)}"
        )
    return body, fallback_body, req_headers


class _BackoffPolicy:
    """cookie_retry_backoff 六处调用的参数包（QUA-P1-2 收敛）。

    退避窗口（backoff/jitter）与断连检测句柄在一次交付内不变，只有
    attempt / error / label / retry_after 逐次变化；生图整取路径
    （_collect_full_response）不经 gen，显式传参构造。
    """

    def __init__(self, fastapi_request: "Request", backoff_sec: float, jitter_sec: float):
        self.fastapi_request = fastapi_request
        self.backoff_sec = backoff_sec
        self.jitter_sec = jitter_sec

    @classmethod
    def from_gen(cls, gen: "CookieGeneration") -> "_BackoffPolicy":
        return cls(gen.fastapi_request, gen.backoff_sec, gen.jitter_sec)

    async def wait(self, attempt: int, *, error: str, label: str, retry_after: Optional[str]=None) -> bool:
        """退避等待：True=继续下一次尝试，False=客户端已断开（调用方
        按各自交付形态收尾——错误 dict / 499 响应）。"""
        return await cookie_retry_backoff(
            self.fastapi_request,
            retry_after=retry_after,
            backoff_sec=self.backoff_sec,
            attempt=attempt,
            jitter_sec=self.jitter_sec,
            error=error,
            label=label,
        )


def _safety_html_if_enabled(ratings: Any) -> str:
    """开关打开时把 batchGraphql 的 safetyRatings 渲染成附加块，否则空串。"""
    if not ratings:
        return ""
    if not app_state.get_setting("safety_score", app_config.SAFETY_SCORE):
        return ""
    return _create_safety_ratings_html(ratings)


def _client_gone(message: str = "客户端已断开连接。") -> JSONResponse:
    """499 断连响应（非流式/流式/生图/缓冲交付路径共用同一形态；
    2026-11 审查收敛：此前四处逐字重复同一 JSONResponse 块）。"""
    return JSONResponse(
        status_code=499,
        content=create_openai_error_response(499, message, "client_closed_request"),
    )


# REQ-P3-8：生图收集内部的断连错误字典统一带 code 499——消费方
# （image_frames）按 res.get("code", 502) 取状态码，缺 code 字段时
# 客户端断开被呈现为 502 上游错误，污染错误归因与大盘计数。
_CLIENT_GONE_RESULT = {"kind": "error", "code": 499, "message": "客户端已断开连接。"}



def _final_usage(
    usage_meta: Optional[dict], request_obj: "OpenAIRequest", completion_text: str, reasoning_text: str = ""
) -> dict:
    """上游真实计量优先；未回传且 usage_estimation 开启时给字符级粗估。

    粗估附带 "estimated": true；生图（正文为 base64 data URL）不适用，调用方不走本函数。
    思考文本恰好计一次：流式路径把 thought 项并入 completion_text 传入，
    非流式路径经 reasoning_text 传入——两口径必须给出相同结果（2026-11
    对齐：reasoning 为空时不再追加换行，否则流式比非流式多计一个字符）。
    """
    usage = map_usage(usage_meta)
    if usage_meta is None and app_state.get_setting("usage_estimation", False):
        prompt = messages_prompt_text(getattr(request_obj, "messages", None) or [])
        combined = completion_text or ""
        if reasoning_text:
            combined = f"{combined}\n{reasoning_text}"
        estimated = estimate_usage(prompt, combined)
        if estimated["total_tokens"]:
            return estimated
    return usage


async def _collect_full_response(
    project_id: str,
    base_model_name: str,
    request_obj: "OpenAIRequest",
    cookie_str: str,
    client_kwargs: dict,
    retry_max: int,
    backoff_sec: float,
    jitter_sec: float,
    fastapi_request: "Request",
    *,
    prefill_active: bool=False,
    force_search: bool=False,
    account_id: str | None = None,
    record: Optional[dict]=None,
) -> dict:
    """非流式地完整取回一次响应（供生图假流式复用）。返回 dict。

    account_id / record：凭据记账句柄（由 image_frames 传入）。鉴权失败
    （HTTP 401/403 或 GraphQL 权限类错误）时向 credential_manager 记账，
    触发坏账号冷却隔离（2026-11 P1 修复：旧实现两个鉴权失败出口都直接
    return，生图流量下的失效账号会被反复选中烧满全部重试预算，完全
    绕过健康隔离——与同通道流式/非流式路径行为分叉）。
    """
    # 请求体只构建一次：构建内部含远程图片下载 + PIL 压缩，旧实现每次
    # 重试都重建一遍 = 反复下载同一批图片。重试发同一个 body 本就是重试语义。
    if await fastapi_request.is_disconnected():
        return dict(_CLIENT_GONE_RESULT)
    body = await build_request_body_async(
        project_id,
        base_model_name,
        request_obj,
        prefill_active=prefill_active,
        force_search=force_search,
    )
    backoff = _BackoffPolicy(fastapi_request, backoff_sec, jitter_sec)
    for attempt in range(retry_max + 1):
        if await fastapi_request.is_disconnected():
            return dict(_CLIENT_GONE_RESULT)
        if attempt:
            stats.add_retry()
        try:
            # cookie_str 一律是完整 Cookie 字符串（2026-11 审查收敛：曾保留
            # 兼容旧测试桩直传 headers dict 的分支，测试桩已改为传字符串，
            # 拒绝整包 headers 直传的防御逻辑随之并入下方统一的 None 判定）。
            req_headers = build_headers(cookie_str)
            if not req_headers:
                return {
                    "kind": "error",
                    "code": 401,
                    "message": "Cookie 中未找到 SAPISID，无法计算认证头。",
                }
            async with open_upstream_client(client_kwargs) as client:
                upstream_attempt()
                # 整取响应（生图假流式路径）：不参与首回复计时（2026-11
                # 语义，见 first_reply.timed_call）；挂起由 httpx read 超时
                # （180s）与整轮期限兑底。
                response = await timed_call(
                    base_model_name,
                    client.post(BATCH_GRAPHQL_URL, headers=req_headers, json=body),
                    whole_call=True,
                )

            if is_retryable_status(response.status_code) and attempt < retry_max:
                if not await backoff.wait(
                    attempt,
                    retry_after=getattr(response, "headers", {}).get("retry-after"),
                    error=f"HTTP {response.status_code}: {response.text[:200]}",
                    label=f"HTTP {response.status_code}（生图）",
                ):
                    return dict(_CLIENT_GONE_RESULT)
                continue
            if response.status_code != 200:
                # 鉴权失败记账（2026-11 P1）：与流式/非流式路径同口径，
                # 401/403 要进账号健康隔离，不能只返回错误字典。
                if response.status_code in (401, 403) and account_id is not None:
                    credential_manager.record_cookie_auth_failure(
                        account_id, credential=record
                    )
                return {
                    "kind": "error",
                    "code": response.status_code,
                    "message": f"HTTP {response.status_code}: {response.text[:300]}",
                }

            # 生图内存（2026-11 第三轮）：状态码/重试判断完成后立即释放
            # 响应对象——httpx 整取会同时持有 content bytes 与 .text 解码
            # 出的 str（4K 单图各 ~30-50MB），旧实现解析全程两者并存，
            # 叠加 JSON 解出的 data 串、markdown 与 full_text 后收集期
            # 4-5 份全量。text 提前取本地引用，bytes 缓冲即可回收。
            response_text = response.text
            response = None

            full_text, reasoning_text, api_error, usage_meta = "", "", None, None
            api_error_status = 502
            finish_raw, blocked_msg = None, None
            safety_html = ""
            sampler = RawSampler()
            # 正文/思考收集改「片段表 + 一次 join」（2026-11 第五轮）：生图响应
            # 常见「导语文本 + 巨型图片 + 尾文本」，旧 `full_text += data` 对
            # 每个事件都重建整串（尾文本一次就多拷一份 40MB 级全量；多图场景
            # O(n²)）；join 恰好一次拷贝，片段表只持引用。
            text_pieces: list[str] = []
            thought_pieces: list[str] = []

            # 拉取式游标：生成器形态在 yield 挂起期间其帧局部会把解析出的
            # 巨型 dict（含整份 base64）钉到本对象消费完之后，del 也无济于事；
            # 游标返回后不持引用，配合 del obj 让解析副本在事件消费前释放。
            cursor = CompleteTextObjectCursor(response_text)
            while True:
                obj = cursor.next_object()
                if obj is None:
                    break
                sampler.add(obj)
                # 一旦拿到错误事件就停止解析剩余响应：生图响应可达数 MB base64，
                # 错误分支的返回值不依赖后续对象，继续拼接是纯浪费。
                if api_error:
                    break
                # PRF-P1-1：含内联图片的对象在媒体线程解析（base64 解码 +
                # 落盘 100-500ms 不能停在事件循环上，多用户生图会停摆全部
                # 并发 SSE 流）；纯文本对象保持原地零开销解析。
                if image_delivery.has_inline_image(obj):
                    events = await run_media_task(list, extract_events(obj), heavy=True)
                else:
                    events = list(extract_events(obj))
                # 正文已到即停止留存诊断样本（样本只服务「无正文」日志）：
                # 留存会把含巨型 inlineData 的本对象钉到函数返回，收集期
                # 恒多一份全量驻留（2026-11 第五轮）。
                if any(et in BODY_EVENT_TYPES for et, _ in events):
                    sampler.stop_retaining()
                del obj
                # 与流式路径同口径：空分片/纯错误分片不计入「收到首个上游分片」。
                if any(et != "error" for et, _ in events):
                    upstream_chunk()
                for et, data in events:
                    if et == "text" or et == "image":
                        text_pieces.append(data)
                    elif et == "thought":
                        thought_pieces.append(data)
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
                events = None
            full_text = "".join(text_pieces)
            reasoning_text = "".join(thought_pieces)
            # 片段表与游标立即释放：join 后它们只是全量拷贝的额外钉子。
            text_pieces = None
            thought_pieces = None
            cursor = None

            if (
                api_error
                and is_retryable_status(api_error_status)
                and attempt < retry_max
            ):
                if not await backoff.wait(
                    attempt, error=api_error, label="生图 429/限流",
                ):
                    return dict(_CLIENT_GONE_RESULT)
                continue
            if api_error:
                # 鉴权失败记账：api_error_details 已含文本兜底（权限类文案
                # → 403），无 code 字段的 GraphQL 错误块同样进健康隔离。
                if api_error_status in (401, 403) and account_id is not None:
                    credential_manager.record_cookie_auth_failure(
                        account_id, credential=record
                    )
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
                if not await backoff.wait(
                    attempt, error=em, label="生图异常",
                ):
                    return dict(_CLIENT_GONE_RESULT)
                continue
            code, _ = extract_upstream_error(e)
            return {
                "kind": "error",
                "code": code,
                "message": f"batchGraphql proxy error: {em}",
            }
    return {"kind": "error", "message": "重试多次仍失败。"}


async def deliver_image_frames(gen: "CookieGeneration") -> AsyncIterator[str]:

    if await gen.fastapi_request.is_disconnected():
        return
    print(
        f"🖼️ [生图保护] 图片模型 {gen.base_model_name} 已自动切换为假流式输出（Cookie 通道），生成期间持续保活。"
    )
    # 生图和重试等待期间也持续发心跳，避免前端或中间层按空闲超时断开。
    # 首心跳行为与 stream_frames / buffered_frames 保持一致。
    yield HEARTBEAT_FRAME
    collect_task = asyncio.create_task(
        _collect_full_response(
            gen.project_id,
            gen.base_model_name,
            gen.request_obj,
            gen.cookie_str,
            gen.client_kwargs,
            gen.retry_max,
            gen.backoff_sec,
            gen.jitter_sec,
            gen.fastapi_request,
            prefill_active=gen.prefill_active,
            force_search=gen.force_builtin_search,
            account_id=gen.account_id,
            record=gen.record,
        )
    )
    try:
        while True:
            done, _pending = await asyncio.wait({collect_task}, timeout=10.0)
            if done:
                break
            if await gen.fastapi_request.is_disconnected():
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
            gen.response_id, gen.model_display, role="assistant"
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
        if gen.prefill_text:
            full_text = gen.prefill_text + strip_prefill_overlap(
                gen.prefill_text, full_text
            )
        if res.get("reasoning_text") and not gen.strip_thoughts:
            yield make_openai_chunk(
                gen.response_id,
                gen.model_display,
                reasoning_content=res["reasoning_text"],
            )
        # 整张图作为单个 chunk 发出（≤1MiB 小图）；超大 4K base64
        # 改为 256KiB 增量帧：单帧会在序列化/编码瞬间再造三份全量拷贝，
        # 512MB 容器（Render 免费档）叠加解析链会被 OOMKill。切片拼接
        # 结果与单帧逐字节一致，客户端按 delta 拼接语义无感。
        # 惰性分帧：不物化整张帧表（4K base64 三份全量拷贝叠加会
        # OOMKill 512MB 容器），逐帧按需产出。
        for _frame in iter_openai_content_chunks(
            gen.response_id, gen.model_display, content=full_text or " "
        ):
            yield _frame
        stats.add_success()
        credential_manager.record_cookie_success(
            gen.account_id, credential=gen.record
        )
        # 生图正文是 base64 data URL，字符估算失真极大，不做 usage 估算
        usage = map_usage(res.get("usage_meta"))
        if gen.want_usage:
            yield make_usage_chunk(gen.response_id, gen.model_display, usage)
        yield make_openai_chunk(
            gen.response_id,
            gen.model_display,
            finish_reason=res.get("finish_reason", "stop"),
        )
        yield "data: [DONE]\n\n"
        print(f"✅ [Studio] {gen.base_model_name} | 生图假流式完成")
    finally:
        # 生成器被提前关闭（断连/停机/缓冲淘汰都会 aclose 本生成器，
        # GeneratorExit 打进 asyncio.wait 的挂起点）时必须收尸：
        # 孤儿任务会带着完整重试循环（retry_max × 退避）继续打上游，
        # 浪费配额且此时并发名额已归还，完全绕过 upstream_max_concurrency。
        if not collect_task.done():
            collect_task.cancel()
        await asyncio.gather(collect_task, return_exceptions=True)



class _StreamAttemptState:
    """单次流式尝试的事件计量（QUA-P1-1：自 deliver_stream_events 拆出）。

    每次尝试全新构造，与旧实现的循环内局部变量一一对应；重试决策
    （retry_error）与终态标记（terminated）由事件循环子生成器写入，
    编排壳 deliver_stream_events 读取。
    """

    def __init__(self, gen: "CookieGeneration"):
        self.has_yielded = False  # 是否已向客户端输出过帧
        self.prefill_sent = False  # 预填充先行帧独立闸（REQ-P3-9）
        self.got_text = False  # 是否收到过“正文”（text/image）
        self.got_thought = False  # 是否收到过思考
        self.got_tool_call = False
        self.got_wire_signature = False  # 是否收到过 thoughtSignature 事件
        self.tool_call_index = 0
        self.ordinary_metadata = []
        self.part_order = []
        self.usage_meta = None
        self.finish_raw = None
        self.blocked_msg = None
        self.sampler = RawSampler()
        self.deduper = PrefillDeduper(gen.prefill_text)
        self.pending_safety_html = ""
        self.retry_error = None  # 非 None = 本次尝试应重试（携错误对象）
        self.terminated = False  # 已发出错误帧 + [DONE]，交付就此结束


def _role_frames(gen: "CookieGeneration", *, announce: bool) -> list:
    """连接建立首帧（role chunk）与 ⚡ 日志。

    function_call / thought_signature / 正文三类事件与正常收尾共用形态；
    announce 保留旧口径——签名先到与纯收尾补帧不打「连接建立」日志。
    预填充先行帧不在此处（REQ-P3-9 拆出，见 _ensure_prefill_frames）。
    """
    if announce:
        print(
            f"⚡ [Studio] {gen.base_model_name} | 连接建立，正在实时流式输出..."
        )
    return [make_openai_chunk(gen.response_id, gen.model_display, role="assistant")]


def _ensure_prefill_frames(gen: "CookieGeneration", st: "_StreamAttemptState") -> list:
    """预填充先行帧的一次性闸门（REQ-P3-9，2026-11 收敛）。

    旧实现把「是否已发 prefill」隐式折叠进 has_yielded：签名 / 工具
    事件先到时置真但从未发过 prefill，后续正文分支被 has_yielded 挡
    住，预填充文本整段丢失——与 Express 通道（签名暂存 pending_gate，
    首个可见内容前冲刷后发 prefill）行为分叉。提取独立闸门：
    - 签名先到：先出签名帧（不动 prefill 闸）；
    - 首个可见内容（正文 / 思考 / 图片 / 工具调用 / 纯收尾补帧）前：
      先发 prefill 再发内容，恰好一次，跨分支防回归。
    """
    if not gen.prefill_text or st.prefill_sent:
        return []
    st.prefill_sent = True
    # 预填充智能兼容：先把预填充文本作为回复开头发出
    return [
        make_openai_chunk(
            gen.response_id, gen.model_display, content=gen.prefill_text
        )
    ]


def _error_done_frames(code: int, message: str) -> list:
    """错误帧 + [DONE] 两连帧（transport / api_error_text / cookie_error /
    retryable-fatal 四个终点路径共用；统一走 create_openai_error_response
    拿到凭据脱敏与标准形状，消息按 1024 截断防巨型帧）。"""
    error = create_openai_error_response(code, str(message)[:1024], "upstream_error")
    return [
        "data: " + json.dumps(error, ensure_ascii=False) + "\n\n",
        "data: [DONE]\n\n",
    ]


def _safety_before_error_frames(gen: "CookieGeneration", st: "_StreamAttemptState") -> list:
    """错误终点前冲刷已暂存的安全分块（REQ-P3-6，2026-11 收敛）。

    安全分与 finish 同块到达、先暂存等去重尾段；若上游随后直接报错
    （transport / api_error / cookie_error / retryable-fatal），旧实现
    把暂存的安全分静默丢弃——安全拦截诊断线索就此丢失，且重放/监控
    都看不到该次拦截的评分。与正常收尾同口径：正文 → 安全分 → 结束/
    错误帧；安全分需内容帧形态，未建连时先补 role 帧（同 _finalize）。
    """
    if not st.pending_safety_html:
        return []
    frames = []
    if not st.has_yielded:
        frames.extend(_role_frames(gen, announce=False))
        st.has_yielded = True
    # 终点路径：预填充若还没发也一并冲刷（流到此结束，不再有机会）。
    frames.extend(_ensure_prefill_frames(gen, st))
    frames.append(make_openai_chunk(
        gen.response_id, gen.model_display, content=st.pending_safety_html,
    ))
    st.pending_safety_html = ""
    return frames


async def _stream_attempt_frames(gen: "CookieGeneration", events: AsyncIterator, st: _StreamAttemptState, attempt: int):
    """单次尝试的事件消费循环：原始事件 → OpenAI 帧（QUA-P1-1 拆分）。

    只负责「事件→帧」与计量；重试与收尾决策经 st 交回编排壳：
    - st.retry_error 非空：应重试（此处已判定未输出且 attempt < retry_max）；
    - st.terminated：已发错误帧 + [DONE]，整个交付就此结束；
    - 两者皆否：流自然结束，调用方进入正常收尾。
    """
    async for status, data in events:
        if status == "transport_error":
            stats.add_error()
            print(f"❌ [抗截断] {data}")
            for frame in _safety_before_error_frames(gen, st):
                yield frame
            for frame in _error_done_frames(502, data):
                yield frame
            st.terminated = True
            return
        # 思考剥离：batchGraphql 忽略 includeThoughts，这里按解析出的配置主动丢弃思考块
        if status == "thought":
            st.got_thought = True
            if gen.strip_thoughts:
                continue  # 不建连、不输出，等真正的正文

        if status == "function_call":
            if not st.has_yielded:
                for frame in _role_frames(gen, announce=True):
                    yield frame
                st.has_yielded = True
            for frame in _ensure_prefill_frames(gen, st):
                yield frame
            tool_payload = openai_tool_call(
                gen.response_id, st.tool_call_index, data
            )
            tool_payload["index"] = st.tool_call_index
            st.part_order.append(
                {"type": "tool_call", "index": st.tool_call_index}
            )
            yield make_openai_chunk(
                gen.response_id,
                gen.model_display,
                tool_calls=[tool_payload],
            )
            st.tool_call_index += 1
            st.got_tool_call = True

        elif status == "thought_signature":
            signature = apply_wire_signature(
                data, st.ordinary_metadata, st.part_order
            )
            if signature:
                st.got_wire_signature = True
            extra = thought_signature_extra(signature, data.get("part_kind"))
            if extra:
                # 签名事件只携带签名与 part 类型（2026-11 P2-2 修复）：
                # 旧实现每个签名事件都内嵌「至今全部普通 part（含全部
                # 思考全文）」快照，P 个带签名 part 的响应线上传输 O(P²)
                # （Express 通道已修复过同一问题，见 express_response.py
                # 的终帧快照注释）；完整拓扑改由收尾帧一次性携带。
                if not st.has_yielded:
                    for frame in _role_frames(
                        gen, announce=False
                    ):
                        yield frame
                    st.has_yielded = True
                yield make_openai_chunk(
                    gen.response_id,
                    gen.model_display,
                    extra_content=extra,
                )

        elif status in ("text", "thought", "image", "api_error_text"):
            if not st.has_yielded:
                for frame in _role_frames(gen, announce=True):
                    yield frame
                st.has_yielded = True
            for frame in _ensure_prefill_frames(gen, st):
                yield frame

            if status == "thought":
                st.part_order.append(
                    {
                        "type": "ordinary",
                        "index": len(st.ordinary_metadata),
                    }
                )
                st.ordinary_metadata.append(
                    ordinary_part_metadata("thought", data, None)
                )
                yield make_openai_chunk(
                    gen.response_id,
                    gen.model_display,
                    reasoning_content=data,
                )
            elif status == "api_error_text":
                stats.add_error()
                yield make_openai_chunk(
                    gen.response_id,
                    gen.model_display,
                    content=f"\n[Studio API 错误] {data}",
                )
                for frame in _safety_before_error_frames(gen, st):
                    yield frame
                for frame in _error_done_frames(502, data):
                    yield frame
                st.terminated = True
                return
            elif status == "image":
                st.got_text = True
                st.part_order.append(
                    {
                        "type": "ordinary",
                        "index": len(st.ordinary_metadata),
                    }
                )
                st.ordinary_metadata.append(
                    ordinary_part_metadata("image", "", None)
                )
                # 超大生图 base64 分帧发射（同整取路径，见
                # iter_openai_content_chunks 注释）；小图保持单帧。
                for _frame in iter_openai_content_chunks(
                    gen.response_id, gen.model_display, content=data
                ):
                    yield _frame
            else:  # text（经过预填充去重器）
                st.got_text = True
                out = st.deduper.feed(data)
                if out:
                    st.part_order.append(
                        {
                            "type": "ordinary",
                            "index": len(st.ordinary_metadata),
                        }
                    )
                    st.ordinary_metadata.append(
                        ordinary_part_metadata("text", out, None)
                    )
                    yield make_openai_chunk(
                        gen.response_id,
                        gen.model_display,
                        content=out,
                    )

        elif status == "finish":
            st.finish_raw = data
        elif status == "safety":
            # 安全分与 finish 同块到达（最后一块）：先暂存，等去重器
            # flush 完尾段再输出。旧实现在循环内即时 yield，预填充 +
            # 短回复场景下会插在去重器攒住的正文尾段**之前**，把安全块
            # 嵌进正文中间（2026-10 修复）。
            st.pending_safety_html = _safety_html_if_enabled(data)
        elif status == "usage":
            st.usage_meta = merge_usage_metadata(st.usage_meta, data)
        elif status == "blocked":
            st.blocked_msg = data

        elif status == "cookie_error":
            stats.add_error()
            credential_manager.record_cookie_auth_failure(
                gen.account_id, credential=gen.record
            )
            code = getattr(data, "status_code", None) or 403
            for frame in _safety_before_error_frames(gen, st):
                yield frame
            for frame in _error_done_frames(code, data):
                yield frame
            st.terminated = True
            return

        elif status in ("retryable_error", "fatal_error"):
            if (
                not st.has_yielded
                and status == "retryable_error"
                and attempt < gen.retry_max
            ):
                st.retry_error = data
                return
            stats.add_error()
            default_code = 429 if status == "retryable_error" else 502
            code = getattr(data, "status_code", None) or default_code
            print(f"❌ [Studio] HTTP {code} | {str(data)[:300]}")
            for frame in _safety_before_error_frames(gen, st):
                yield frame
            for frame in _error_done_frames(code, data):
                yield frame
            st.terminated = True
            return


async def _finalize_cookie_stream(gen: "CookieGeneration", st: _StreamAttemptState):
    """流式正常收尾（QUA-P1-1 拆分）：role 补帧 / 去重尾段 / 安全分 /
    拓扑快照终帧 / 结束帧 / usage / [DONE] / 完成日志。

    无正文分支给出可见诊断而非静默关流；成功分支在存在工具调用或
    签名元数据时一次性携带完整 ordinary_parts + part_order 终帧拓扑
    （2026-11 P2-2：签名事件瘦身，拓扑流量回到线性）。
    """
    if not st.has_yielded:
        for frame in _role_frames(gen, announce=False):
            yield frame
        st.has_yielded = True
    # 纯收尾补帧（无任何可见内容）：预填充仍需发出（REQ-P3-9 独立闸，
    # 与旧 with_prefill=True 行为一致；签名先到场景此前会整段丢失）。
    for frame in _ensure_prefill_frames(gen, st):
        yield frame

    # 预填充去重器里可能还攒着开头的一小段（短回复场景）
    tail = st.deduper.flush()
    if tail:
        st.part_order.append(
            {"type": "ordinary", "index": len(st.ordinary_metadata)}
        )
        st.ordinary_metadata.append(ordinary_part_metadata("text", tail, None))
        yield make_openai_chunk(gen.response_id, gen.model_display, content=tail)

    # 安全分在正文（含去重尾段）之后、结束帧之前输出，
    # 与非流式路径 content_value = full_text + safety_html 的顺序一致。
    if st.pending_safety_html:
        yield make_openai_chunk(
            gen.response_id,
            gen.model_display,
            content=st.pending_safety_html,
        )

    if st.got_text or st.got_tool_call:
        stats.add_success()
        credential_manager.record_cookie_success(
            gen.account_id, credential=gen.record
        )
        if st.ordinary_metadata and (st.got_tool_call or st.got_wire_signature):
            google = {
                "ordinary_parts": list(st.ordinary_metadata),
                "part_order": list(st.part_order),
            }
            last_signed = next(
                (
                    item
                    for item in reversed(st.ordinary_metadata)
                    if item.get("thought_signature")
                ),
                None,
            )
            if last_signed:
                google["thought_signature"] = last_signed["thought_signature"]
                google["thought_signature_part"] = last_signed["kind"]
            yield make_openai_chunk(
                gen.response_id,
                gen.model_display,
                extra_content={"google": google},
            )
        yield make_openai_chunk(
            gen.response_id,
            gen.model_display,
            finish_reason=map_finish_reason_to_openai(
                st.finish_raw, has_tool_calls=st.got_tool_call
            ),
        )
    else:
        # 无正文：明确告知客户端 + 落诊断日志（旧版此处一个字节都不发就关流）
        desc, detail, notice, no_body_finish = _no_body_outcome(
            st.finish_raw, st.blocked_msg, st.got_thought, gen.strip_thoughts
        )
        stats.add_error()
        # 收到过思考即证明 Cookie 有效（2026-11 C-2 修复）：无正文
        # 分支不记成功，但必须清零该账号的失败计数——否则上游正常
        # 返回、仅正文被剥离/安全拦截的账号会因历史失败累计被
        # 误冷却 600 秒，多 Cookie 池下可用账号被静默摘除。
        if st.got_thought:
            credential_manager.record_cookie_success(
                gen.account_id, credential=gen.record
            )
        print(
            f"❌ [Studio] {gen.base_model_name} | 上游{desc}（{detail}）"
        )
        print(f"🔎 [Studio 诊断] 原始响应样本：\n{st.sampler.dump()}")
        yield make_openai_chunk(
            gen.response_id, gen.model_display, content=f"\n{notice}"
        )
        yield make_openai_chunk(
            gen.response_id,
            gen.model_display,
            finish_reason=no_body_finish,
        )

    usage = _final_usage(
        st.usage_meta,
        gen.request_obj,
        "\n".join(part.get("text", "") for part in st.ordinary_metadata),
    )
    if gen.want_usage:
        yield make_usage_chunk(gen.response_id, gen.model_display, usage)

    yield "data: [DONE]\n\n"

    elapsed = time.time() - gen.start_time
    if st.got_text or st.got_tool_call:
        print(
            f"✅ [Studio] {gen.base_model_name} | 流式传输顺利完毕 | 耗时 {elapsed:.1f}s"
        )


async def deliver_stream_events(gen: "CookieGeneration") -> AsyncIterator[str]:
    """Cookie 真流式交付的编排壳（QUA-P1-1 拆分后仅负责重试循环）。

    事件→帧在 _stream_attempt_frames，正常收尾在 _finalize_cookie_stream；
    单次尝试的状态经 _StreamAttemptState 传递。
    """
    body_cache = _body_cache_for(gen)

    for attempt in range(gen.retry_max + 1):
        # 客户端已断开则立即停止，避免无谓的上游调用与重试
        if await gen.fastapi_request.is_disconnected():
            print("ℹ️ [Studio] 客户端已断开连接，停止流式重试。")
            return
        if attempt:
            stats.add_retry()

        body, fallback_body, req_headers = await _attempt_context(gen, body_cache)
        st = _StreamAttemptState(gen)

        async with open_upstream_client(gen.client_kwargs) as client:
            # 消费实时生成器（原始事件 → OpenAI chunk）
            events = stream_events(
                client,
                req_headers,
                body,
                sampler=st.sampler,
                fallback_body=fallback_body,
                model=gen.base_model_name,
            )
            if gen.text_transport:
                events = gen.text_transport.decoder().cookie_stream(events)
            try:
                async for frame in _stream_attempt_frames(gen, events, st, attempt):
                    yield frame
            finally:
                await events.aclose()

            # 如果标记了需要重试，在当前 attempt 结束时等待并开启下一次循环
            if st.retry_error is not None:
                # 重试前再次确认客户端仍在
                if await gen.fastapi_request.is_disconnected():
                    print("ℹ️ [Studio] 客户端已断开连接，取消后续重试。")
                    return
                # 退避等待期间持续吐心跳，保活前端连接（issue4：3.1-pro 频繁 429 长等待易被前端断开）
                async for hb in cookie_retry_backoff_heartbeats(
                    gen.fastapi_request,
                    retry_after=getattr(st.retry_error, "retry_after", None),
                    backoff_sec=gen.backoff_sec, attempt=attempt,
                    jitter_sec=gen.jitter_sec,
                    error=str(st.retry_error), label="可重试拥堵/限流",
                ):
                    yield hb
                if await gen.fastapi_request.is_disconnected():
                    print("ℹ️ [Studio] 客户端已断开连接，取消后续重试。")
                    return
                gen.start_time = time.time()
                continue

            # 错误帧已发完（transport / api_error_text / cookie_error / retryable-fatal）
            if st.terminated:
                return
            async for frame in _finalize_cookie_stream(gen, st):
                yield frame
            return

async def deliver_stream_frames(gen: "CookieGeneration") -> AsyncIterator[str]:

    # 立即吐一个心跳，尽快建立连接（避免首个上游调用较慢/429 时前端久等无字节）
    yield HEARTBEAT_FRAME
    try:
        async for frame in deliver_stream_events(gen):
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
            f"❌ [Studio] {gen.base_model_name} | 流式响应意外中断 | HTTP {code} | {str(e)[:150]}"
        )
        error = create_openai_error_response(code, message[:1024], "upstream_error")
        yield "data: " + json.dumps(error, ensure_ascii=False) + "\n\n"
        yield "data: [DONE]\n\n"



@dataclass
class _WholeBodyState:
    """非流式整取的事件收集结果（deliver_collect_response 拆分用）。

    片段表（text_pieces/thought_pieces）与游标在 _collect_whole_body
    内部即用即释，这里只持 join 后的成品与元数据槽位。
    """

    # 片段表 + 一次 join（2026-11 第五轮）：生图响应「导语 + 巨型图片 +
    # 尾文本」逐事件 += 是 O(n²) 全量拷贝；join 恰好一次。
    text_pieces: list = field(default_factory=list)
    thought_pieces: list = field(default_factory=list)
    full_text: str = ""
    reasoning_text: str = ""
    safety_html: str = ""
    tool_calls: list = field(default_factory=list)
    ordinary_metadata: list = field(default_factory=list)
    part_order: list = field(default_factory=list)
    message_extra: Optional[dict] = None
    api_error: Optional[str] = None
    api_error_status: int = 502
    usage_meta: Optional[dict] = None
    finish_raw: Optional[str] = None
    blocked_msg: Optional[str] = None
    sampler: RawSampler = field(default_factory=RawSampler)


def _apply_whole_body_event(gen: "CookieGeneration", st: _WholeBodyState, event_type: str, data: Any) -> None:
    """单个解析事件 → 整取状态（deliver_collect_response 拆分）。

    与流式路径的 _stream_attempt_frames 同口径的事件族；差异在整取
    只积累不输出（除错误外），结束时由 _assemble_whole_response 统一装配。
    """
    if event_type == "text":
        st.text_pieces.append(data)
        st.part_order.append({"type": "ordinary", "index": len(st.ordinary_metadata)})
        st.ordinary_metadata.append(ordinary_part_metadata("text", data, None))
    elif event_type == "thought":
        # 先收集（供诊断判断），输出时按 strip 决定是否附带
        st.thought_pieces.append(data)
        st.part_order.append({"type": "ordinary", "index": len(st.ordinary_metadata)})
        st.ordinary_metadata.append(ordinary_part_metadata("thought", data, None))
    elif event_type == "image":
        st.text_pieces.append(data)
        st.part_order.append({"type": "ordinary", "index": len(st.ordinary_metadata)})
        st.ordinary_metadata.append(ordinary_part_metadata("image", "", None))
    elif event_type == "function_call":
        st.part_order.append({"type": "tool_call", "index": len(st.tool_calls)})
        st.tool_calls.append(openai_tool_call(gen.response_id, len(st.tool_calls), data))
    elif event_type == "thought_signature":
        sig = apply_wire_signature(data, st.ordinary_metadata, st.part_order)
        if sig:
            st.message_extra = thought_signature_extra(sig, data.get("part_kind"))
    elif event_type == "finish":
        st.finish_raw = data
    elif event_type == "safety":
        st.safety_html = _safety_html_if_enabled(data)
    elif event_type == "blocked":
        st.blocked_msg = data
    elif event_type == "usage":
        st.usage_meta = merge_usage_metadata(st.usage_meta, data)
    elif event_type == "error":
        st.api_error_status, st.api_error = api_error_details(data)


async def _collect_whole_body(gen: "CookieGeneration", response_text: str, text_decoder: Any) -> _WholeBodyState:
    """整取响应体 → 事件状态（deliver_collect_response 拆分）。

    拉取式游标 + 片段表 join + 媒体线程分流（含内联图片对象的解析搬
    线程）+ 诊断样本「正文即停留存」，与 _collect_full_response 同构但
    覆盖全事件族（工具/签名/用量）。
    """
    st = _WholeBodyState()
    cursor = CompleteTextObjectCursor(response_text)
    while True:
        obj = cursor.next_object()
        if obj is None:
            break
        st.sampler.add(obj)
        # error 即停：后续对象不再解析（生图响应可达数 MB，
        # 白烧事件循环 CPU），且首个 error 不被后续 error 覆盖。
        if st.api_error:
            break
        # PRF-P1-1：含内联图片的对象整段解析搬媒体线程（解码 +
        # 落盘不在事件循环上）；纯文本对象保持原地零开销。
        def _materialize_cookie_events(obj: Any = obj):  # B023：默认参数绑定循环变量（同迭代调用）
            events = extract_events(obj)
            if text_decoder:
                events = text_decoder.cookie_events(events)
            # list() 物化：extract_events / cookie_events 是生成器，
            # 下方 any() 与 for 各消费一次，不物化后者会拿到空。
            return list(events)

        if image_delivery.has_inline_image(obj):
            events = await run_media_task(_materialize_cookie_events, heavy=True)
        else:
            events = _materialize_cookie_events()
        # 正文/工具内容已到即停止留存诊断样本（样本只服务
        # 「无正文」日志；留存会钉住含巨型 inlineData 的对象）。
        if any(et in BODY_EVENT_TYPES for et, _ in events):
            st.sampler.stop_retaining()
        del obj
        # 与流式路径同口径：空分片/纯错误分片不计入「收到首个上游分片」。
        if any(et != "error" for et, _ in events):
            upstream_chunk()
        for event_type, data in events:
            _apply_whole_body_event(gen, st, event_type, data)
            if st.api_error:
                break
        events = None
    cursor = None
    # 片段表立即释放：join 后它们只是全量拷贝的额外钉子。
    st.full_text = "".join(st.text_pieces)
    st.reasoning_text = "".join(st.thought_pieces)
    st.text_pieces = None
    st.thought_pieces = None
    return st


def _assemble_whole_response(gen: "CookieGeneration", st: _WholeBodyState) -> "Response":
    """整取结果的最终装配（deliver_collect_response 拆分）。

    成功：require_finish_reason + 预填充拼回 + 完整 message_obj（思考/
    工具/签名拓扑）+ usage；无正文：明确诊断而非静默空回复（收到过思考
    仍清零账号失败计数，与流式路径同口径）。
    """
    got_text = bool(st.full_text.strip())
    got_output = got_text or bool(st.tool_calls)
    elapsed = time.time() - gen.start_time

    if got_output:
        require_finish_reason(st.finish_raw)
        credential_manager.record_cookie_success(gen.account_id, credential=gen.record)
        # 预填充智能兼容：去重后把预填充文本拼回输出开头
        if gen.prefill_text and got_text:
            st.full_text = gen.prefill_text + strip_prefill_overlap(
                gen.prefill_text, st.full_text
            )
        finish_reason = map_finish_reason_to_openai(
            st.finish_raw, has_tool_calls=bool(st.tool_calls)
        )
        stats.add_success()
        print(
            f"✅ [Studio] {gen.base_model_name} | {len(st.full_text)} 字符 | {elapsed:.1f}s"
        )
    else:
        # 无正文（只有思考 / 完全空）：返回明确诊断而不是一个空格
        desc, detail, notice, finish_reason = _no_body_outcome(
            st.finish_raw,
            st.blocked_msg,
            bool(st.reasoning_text),
            gen.strip_thoughts,
        )
        stats.add_error()
        # 与流式路径同口径（2026-11 C-2）：收到过思考即证明 Cookie
        # 有效，清零账号失败计数，避免历史累计触发误冷却。
        if st.reasoning_text:
            credential_manager.record_cookie_success(
                gen.account_id, credential=gen.record
            )
        print(
            f"❌ [Studio] {gen.base_model_name} | 上游{desc}（{detail}）| {elapsed:.1f}s"
        )
        print(f"🔎 [Studio 诊断] 原始响应样本：\n{st.sampler.dump()}")
        st.full_text = notice

    usage = _final_usage(st.usage_meta, gen.request_obj, st.full_text, st.reasoning_text)

    content_value = st.full_text + st.safety_html
    message_obj = {
        "role": "assistant",
        "content": content_value if content_value else None,
    }
    if (
        st.reasoning_text and not gen.strip_thoughts
    ):  # strip：batchGraphql 忽略 includeThoughts，输出侧剥离
        message_obj["reasoning_content"] = st.reasoning_text
    if st.tool_calls:
        message_obj["tool_calls"] = st.tool_calls
    if st.ordinary_metadata:
        if st.message_extra is None:
            st.message_extra = {"google": {}}
        google = st.message_extra.setdefault("google", {})
        google["ordinary_parts"] = st.ordinary_metadata
        if st.tool_calls:
            google["part_order"] = st.part_order
    if st.message_extra:
        message_obj["extra_content"] = st.message_extra

    payload = {
        "id": gen.response_id,
        "object": "chat.completion",
        "created": int(time.time()),
        "model": gen.model_display,
        "choices": [
            {
                "index": 0,
                "message": message_obj,
                "finish_reason": finish_reason,
            }
        ],
        "usage": usage,
    }
    response_class = LargeJSONResponse if gen.is_image else JSONResponse
    return response_class(content=payload)


async def deliver_collect_response(gen: "CookieGeneration") -> "Response":
    """Cookie 非流式整取交付的编排壳（重试循环 + 错误分类）。

    事件收集在 _collect_whole_body（单事件分发 _apply_whole_body_event），
    结果装配在 _assemble_whole_response；请求体双形态缓存与退避经
    _body_cache_for / _attempt_context / _BackoffPolicy（QUA-P1-2）。
    """

    # 请求体两形态各自只构建一次（同流式路径：防重试反复下载图片）
    body_cache = _body_cache_for(gen)
    backoff = _BackoffPolicy.from_gen(gen)

    # 非流式整取：不参与首回复计时（2026-11 语义，见 first_reply.timed_call）。
    for attempt in range(gen.retry_max + 1):
        # 客户端已断开则立即停止，避免无谓的上游调用与重试
        if await gen.fastapi_request.is_disconnected():
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
            body, fallback_body, req_headers = await _attempt_context(gen, body_cache)

            async with open_upstream_client(gen.client_kwargs) as client:
                upstream_attempt()
                response = await timed_call(
                    gen.base_model_name,
                    client.post(BATCH_GRAPHQL_URL, headers=req_headers, json=body),
                    whole_call=True,
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
                        gen.base_model_name,
                        client.post(
                            BATCH_GRAPHQL_URL,
                            headers=req_headers,
                            json=resolved_fallback,
                        ),
                        whole_call=True,
                    )
                    fallback_body = None

            if is_retryable_status(response.status_code):
                if attempt < gen.retry_max:
                    if not await backoff.wait(
                        attempt,
                        retry_after=getattr(response, "headers", {}).get("retry-after"),
                        error=f"HTTP {response.status_code}: {response.text[:200]}",
                        label=f"HTTP {response.status_code}",
                    ):
                        return _client_gone()
                    continue

            if response.status_code != 200:
                elapsed = time.time() - gen.start_time
                print(
                    f"❌ [Studio] {gen.base_model_name} | HTTP {response.status_code} | {elapsed:.1f}s"
                )
                return JSONResponse(
                    status_code=response.status_code,
                    content=create_openai_error_response(
                        response.status_code, response.text[:500], "upstream_error"
                    ),
                )

            # 生图内存（2026-11 第五轮）：httpx 整取同时持有 content bytes
            # 与解码 str；text 取本地引用后置 None，bytes 缓冲在解析前回收。
            # （非流式文本模型响应体积小，收益集中在非流式生图请求。）
            response_text = response.text
            response = None
            text_decoder = (
                gen.text_transport.decoder() if gen.text_transport else None
            )
            st = await _collect_whole_body(gen, response_text, text_decoder)
            response_text = None

            if (
                st.api_error
                and is_retryable_status(st.api_error_status)
                and attempt < gen.retry_max
            ):
                if not await backoff.wait(
                    attempt, error=st.api_error, label="429/限流",
                ):
                    return _client_gone()
                continue
            if st.api_error:
                if st.api_error_status in (401, 403):
                    credential_manager.record_cookie_auth_failure(
                        gen.account_id, credential=gen.record
                    )
                return JSONResponse(
                    status_code=st.api_error_status,
                    content=create_openai_error_response(
                        st.api_error_status, st.api_error, "upstream_error"
                    ),
                )

            if text_decoder:
                text_decoder.validate_end()

            return _assemble_whole_response(gen, st)

        except Exception as e:
            err_msg = str(e)
            is_retryable = is_retryable_exception(e)

            if is_retryable and attempt < gen.retry_max:
                if not await backoff.wait(
                    attempt, error=err_msg, label="异常",
                ):
                    return _client_gone()
                continue

            elapsed = time.time() - gen.start_time
            print(
                f"❌ [Studio] {gen.base_model_name} | 异常 | {elapsed:.1f}s: {err_msg[:300]}"
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

    elapsed = time.time() - gen.start_time
    print(
        f"❌ [Studio] {gen.base_model_name} | 重试 {gen.retry_max} 次后仍失败 | {elapsed:.1f}s"
    )
    return JSONResponse(
        status_code=429,
        content=create_openai_error_response(
            429, "请求被限流，已重试多次仍失败。请稍后再试。", "rate_limit_error"
        ),
    )



async def deliver_buffered_frames(gen: "CookieGeneration") -> AsyncIterator[str]:

    async def result_once():
        yield await deliver_collect_response(gen)

    events = with_heartbeats(result_once(), gen.fastapi_request)
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
                payload, include_usage=gen.want_usage
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



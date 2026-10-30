"""Anthropic Claude Messages API 兼容路由（/v1/messages 双端点）。

为什么需要它：Claude 生态（Claude Code、Anthropic SDK、Claude Code Router、
Cherry Studio 等说 Claude 协议的客户端）说的是另一套协议：

    POST /v1/messages                （主端点，stream:true 走 SSE）
    POST /v1/messages/count_tokens   （上下文 token 估算）
    GET  /api/hello                  （Claude Code 网关启动探测）

本模块把 Anthropic 请求翻译成内部 OpenAIRequest，**完全复用**
/v1/chat/completions 的上游管线（Express Key / Cookie 双通道调度、重试、
抗截断、预填充兼容、思考档位、安全设置等），再把响应翻译回 Anthropic
格式；协议转换细节见 claude_protocol.py。

实现要点：
- 认证兼容 Anthropic 标准写法 x-api-key，同时接受 Authorization: Bearer、
  x-goog-api-key 与 ?key=（值必须等于本服务 API_KEY，常量时间比较）；
  401/400 等错误一律返回 Anthropic 错误形状 {"type":"error","error":{...}}；
- anthropic-version / anthropic-beta 头不校验（放行即可）；
- 方法约束对齐 vertex-master：/v1/messages 与 count_tokens 仅接受 POST、
  /api/hello 仅接受 GET/HEAD，其余方法一律 405 + Anthropic 错误形状
  （FastAPI 路由层的默认 405 是 {"detail":...} 形状，Claude 客户端无法解析）；
- 流式：进入即发 message_start（客户端先拿到头），上游心跳注释帧转换为
  event: ping，上游错误帧转换为 event: error；另有 10s 空闲保活——长思考
  场景 Gemini 可能 10-30s 不发内容，静默超 10s 自动补 event: ping，防止
  Claude Code SDK 误判超时断连（对齐 vertex-master 的 10s ping ticker）；
  中途错误不改变 HTTP 状态码（SSE 头已提交，对齐 Anthropic 官方流式行为）；
- 非流式与流式的快速失败（0.05s 窗口内）：保留真实 HTTP 状态码 + Anthropic
  错误体（invalid_request_error / not_found_error / rate_limit_error 等）；
- 续传缓冲（resumable）为 OpenAI SSE 专属语义，Claude 路径直连上游，
  X-Resumable 头不生效。
"""

import asyncio
import json
import threading
import time
from typing import Optional

from fastapi import APIRouter, Depends, Header, Request
from fastapi.responses import JSONResponse, StreamingResponse
from google.genai import types

from admission import GenerationCapacityError, ManagedStreamingResponse
from auth import constant_time_equal
from claude_protocol import (
    ClaudeRequestError,
    ClaudeStreamState,
    PING_EVENT,
    anthropic_error_type,
    anthropic_models_payload,
    anthropic_omit_thinking,
    anthropic_to_openai_request,
    estimate_request_tokens,
    error_event,
    new_message_id,
    new_request_id,
    openai_completion_to_anthropic,
    resolve_claude_model,
)
from config import API_KEY
from express_endpoint import resolve_express_model_path
from generation_config import _function_tools, _system_instruction
from logger import stats
from media_tasks import run_media_task
from message_processing import create_gemini_prompt
from model_loader import get_express_models
from proxy_failover import FailoverSDKClient, ProxyChain, ProxyResolveError
from proxy_resolver import ProxyUsage, current_proxy_usage
from client_pool import get_pooled_express_client
from request_tracking import configure as track_configure
from response_validation import IncompleteResponseError
from request_connection import BodyTooLargeError
from routes.chat_api import _call_upstream
from runtime_state import app_state
from streaming import sse_frames
from upstream_errors import extract_upstream_error
from upstreams.express_sdk import _normalize_model_name

router = APIRouter()

# 空闲 ping 周期（秒）：模块级常量便于单测注入缩短周期。
# Claude Code SDK 要求流式连接静默不超过 ~15s，否则误判超时断连；
# 长思考场景 Gemini 可能 10-30s 不发任何内容，静默超时即补 event: ping
# （对齐 vertex-master 的 10s ping ticker，取「空闲才补」语义）。
IDLE_PING_SECONDS = 10.0


class ClaudeAPIError(Exception):
    """携带 Anthropic 错误 JSONResponse 的异常，由 main.py 注册的全局
    exception_handler 转成响应（依赖函数里不能直接 return Response）。"""

    def __init__(self, response: JSONResponse):
        self.response = response
        super().__init__("ClaudeAPIError")


def claude_error(status: int, err_type: str, message: str) -> JSONResponse:
    """构造 Anthropic 错误体（官方形状 {"type":"error","error":{...},"request_id":...}）。"""
    return JSONResponse(status_code=status, content={
        "type": "error",
        "error": {"type": err_type, "message": message},
        "request_id": new_request_id(),
    })


# ============================================================
# 认证：x-api-key（Anthropic 标准）> Bearer > x-goog-api-key > ?key=
# ============================================================

async def get_claude_auth(
    request: Request,
    authorization: Optional[str] = Header(None, include_in_schema=False),
    x_api_key: Optional[str] = Header(None, alias="x-api-key", include_in_schema=False),
    x_goog_api_key: Optional[str] = Header(None, alias="x-goog-api-key", include_in_schema=False),
) -> str:
    if not API_KEY:
        raise ClaudeAPIError(claude_error(503, "api_error", "服务端未配置 API_KEY，无法鉴权。"))
    bearer = ""
    if authorization and authorization.startswith("Bearer "):
        bearer = authorization[len("Bearer "):]
    candidate = x_api_key or bearer or x_goog_api_key or request.query_params.get("key", "")
    if not candidate:
        raise ClaudeAPIError(claude_error(
            401, "authentication_error",
            "缺少 API Key：请传入 x-api-key 或 Authorization: Bearer 请求头。"))
    if not constant_time_equal(str(candidate), API_KEY):
        raise ClaudeAPIError(claude_error(401, "authentication_error", "API Key 无效。"))
    return str(candidate)


# ============================================================
# 工具函数
# ============================================================

async def _parse_json_body(fastapi_request: Request) -> dict:
    """手动解析 JSON 体：让 400 错误保持 Anthropic 形状（而非 FastAPI 422）。"""
    try:
        body = await fastapi_request.json()
    except BodyTooLargeError:
        raise
    except Exception:
        raise ClaudeRequestError("invalid JSON request body") from None
    if not isinstance(body, dict):
        raise ClaudeRequestError("request body must be a JSON object")
    return body


def _track_channel() -> str:
    if app_state.get_channel_mode() == "mixed":
        return "mixed"
    return "cookie" if app_state.is_web_proxy_enabled() else "express"


async def _available_models() -> list:
    try:
        return list(await get_express_models() or [])
    except Exception:
        return []


def _openai_error_message(payload: Optional[dict], fallback: str) -> str:
    error = payload.get("error") if isinstance(payload, dict) else None
    if isinstance(error, dict) and isinstance(error.get("message"), str) and error["message"]:
        return error["message"]
    return fallback


def _json_response_payload(response) -> Optional[dict]:
    try:
        return json.loads(response.body)
    except Exception:
        return None


def _claude_stream_response(response: StreamingResponse, display_model: str, *,
                           omit_thinking: bool,
                           started_at: Optional[float] = None) -> ManagedStreamingResponse:
    """把上游 OpenAI SSE 流翻译成 Anthropic 事件流（claude_messages 的流式段）。

    职责与保活策略（详见模块 docstring 与 IDLE_PING_SECONDS 注释）：
    - message_start 先行（不等上游首字节）；
    - 10s 空闲补 ping、上游心跳注释帧转 ping；
    - 有界帧队列背压防慢客户端堆内存；生产者/消费者生命周期与上游
      关闭均在 finally 中收尾，客户端断开不泄漏任务与连接。
    """
    state = ClaudeStreamState(display_model, omit_thinking=omit_thinking)
    upstream_iterator = response.body_iterator

    async def _bytes_source():
        async for chunk in upstream_iterator:
            if isinstance(chunk, str):
                yield chunk.encode("utf-8")
            else:
                yield chunk

    _END = object()  # 生产者结束哨兵
    # 有界队列：慢客户端（网络拥塞）不会让上游读帧无限堆内存——
    # 队满时生产者阻塞在 put 上，形成自然背压；正常生成速率远低于消费
    # 速率，128 帧余量足够不产生延迟。客户端断开/异常时 producer.cancel()
    # 会唤醒阻塞中的 put。
    frame_queue: asyncio.Queue = asyncio.Queue(maxsize=128)

    async def _produce_frames():
        """后台任务：从上游读帧入队；异常/结束也入队，交给消费者统一处理。"""
        try:
            async for frame in sse_frames(_bytes_source()):
                await frame_queue.put(frame)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await frame_queue.put(exc)
        # Cancellation means the consumer has gone away. Never enqueue a
        # sentinel in finally: a full queue would block cancellation forever.
        await frame_queue.put(_END)

    async def event_stream():
        failed = False
        done = False
        delivered = False  # message_stop 已完整发出（客户端断开/异常时不算完成）
        producer = asyncio.create_task(_produce_frames())
        try:
            # message_start 先行：不等上游首字节，客户端连接立刻建立
            yield state.message_start_event()
            while True:
                try:
                    item = await asyncio.wait_for(frame_queue.get(), timeout=IDLE_PING_SECONDS)
                except asyncio.TimeoutError:
                    # 静默超时：补发 ping 保活（不取消上游读取任务）
                    yield PING_EVENT
                    continue
                if item is _END:
                    break
                if isinstance(item, Exception):
                    raise item
                frame = item
                text = frame.decode("utf-8", errors="replace")
                data_events = list(_sse_data_lines(text))
                if not data_events and text.lstrip().startswith(":"):
                    # 上游心跳注释帧 → Anthropic ping 事件（防客户端空闲超时）
                    yield PING_EVENT
                    continue
                for data in data_events:
                    if not data.strip():
                        continue
                    if data.strip() == "[DONE]":
                        done = True
                        break
                    try:
                        payload = json.loads(data)
                    except (ValueError, TypeError):
                        raise IncompleteResponseError("上游 OpenAI 流包含无效 JSON 事件。") from None
                    if isinstance(payload, dict) and payload.get("error"):
                        # 流中错误帧：转成 Anthropic error 事件后终止
                        err_type = anthropic_error_type(502)
                        message = _openai_error_message(payload, "上游流式响应出错。")
                        yield error_event(err_type, message)
                        failed = True
                        break
                    if not isinstance(payload, dict):
                        raise IncompleteResponseError("上游 OpenAI 流事件必须是 JSON 对象。")
                    choices = payload.get("choices")
                    if not choices and isinstance(payload.get("usage"), dict):
                        state.feed_usage(payload["usage"])
                        continue
                    for event in state.feed_openai_chunk(payload):
                        yield event
                if failed or done:
                    break
            if not failed:
                if not state.finish:
                    raise IncompleteResponseError("上游 OpenAI 流缺少结束原因，回复可能不完整。")
                for event in state.finish_events():
                    yield event
                delivered = True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            code, message = extract_upstream_error(exc)
            stats.add_error()
            print(f"❌ [Claude] 模型 {display_model} 流式转发失败 | HTTP {code} | {message[:200]}")
            yield error_event(anthropic_error_type(code), message[:1024])
        finally:
            # 与 OpenAI 通道的完成日志同构：显示模型（claude-* 或原样 gemini-*）
            # | 耗时 | tokens；上游内部管线已按内部模型名打过 📊/✅，这里补齐
            # Claude 协议视角的收尾行。
            if delivered:
                elapsed = time.monotonic() - started_at if started_at is not None else 0.0
                usage = state.usage or {}
                tokens = ""
                if usage.get("input_tokens") or usage.get("output_tokens"):
                    tokens = f" | 输入 {usage.get('input_tokens', 0)} / 输出 {usage.get('output_tokens', 0)} tokens"
                print(f"✅ [Claude] 模型 {display_model} | 流式翻译完成 | 耗时 {elapsed:.1f}s{tokens}")
            # 结束/异常/客户端断开：停掉生产者并回收任务，释放上游连接
            producer.cancel()
            try:
                await producer
            except BaseException:
                pass
            close = getattr(response, "aclose", None) or getattr(upstream_iterator, "aclose", None)
            if close is not None:
                try:
                    await close()
                except Exception:
                    pass

    async def cleanup():
        close = getattr(response, "aclose", None) or getattr(upstream_iterator, "aclose", None)
        if close is not None:
            try:
                await close()
            except Exception:
                pass

    return ManagedStreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-store, no-transform", "X-Accel-Buffering": "no"},
        cleanup=cleanup,
    )


# ============================================================
# 主端点：POST /v1/messages
# ============================================================

@router.post("/v1/messages")
async def claude_messages(fastapi_request: Request, api_key: str = Depends(get_claude_auth)):
    """Anthropic Messages 端点：Anthropic 请求 → 内部管线 → Anthropic 响应。

    并发闸（limit_generation）内置于 _call_upstream，不在此重复装饰，
    避免同一请求占用两份并发配额。
    上游错误（404 模型不可用 / 403 权限 / 429 拥堵等）如实透传状态码，
    仅把错误体改写为 Anthropic 形状；流式已开始后错误以 SSE error 事件表达。
    """
    try:
        body = await _parse_json_body(fastapi_request)
    except ClaudeRequestError as exc:
        return claude_error(400, exc.err_type, str(exc))

    raw_model = body.get("model")
    if not isinstance(raw_model, str) or not raw_model.strip():
        return claude_error(400, "invalid_request_error", "model is required")
    if "max_tokens" not in body:
        return claude_error(400, "invalid_request_error", "max_tokens is required")

    display_model, internal_model = resolve_claude_model(raw_model, await _available_models())
    try:
        request_obj = anthropic_to_openai_request(body, internal_model)
    except ClaudeRequestError as exc:
        return claude_error(exc.status, exc.err_type, str(exc))

    # thinking.display=omitted：响应侧不回传思考块（对齐 vertex-master
    # includeThoughts 口径；模型照常思考，reasoning_tokens 照计 usage）。
    omit_thinking = anthropic_omit_thinking(body)
    request_started = time.monotonic()

    track_configure(model=display_model, protocol="claude",
                    stream=bool(request_obj.stream), channel=_track_channel())
    print(f"🔵 [Claude] 模型 {display_model} → {internal_model} | "
          f"{'流式' if request_obj.stream else '非流式'} | max_tokens={request_obj.max_tokens}")

    try:
        response = await _call_upstream(request_obj, fastapi_request)
    except GenerationCapacityError:
        return claude_error(529, "overloaded_error", "服务并发已达上限，请稍后重试。")
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        code, message = extract_upstream_error(exc)
        print(f"❌ [Claude] 模型 {display_model} 调用失败 | HTTP {code} | {message[:200]}")
        return claude_error(code, anthropic_error_type(code), message[:1024])

    if not isinstance(response, StreamingResponse):
        # 非流式 / 流式快速失败：真实 HTTP 状态码 + Anthropic 错误体
        status = getattr(response, "status_code", 200) or 200
        payload = _json_response_payload(response)
        if status >= 400 or (isinstance(payload, dict) and payload.get("error")):
            message = _openai_error_message(payload, f"上游请求失败（HTTP {status}）。")
            err_status = status if status >= 400 else 502
            return claude_error(err_status, anthropic_error_type(err_status), message)
        try:
            anthropic_payload = openai_completion_to_anthropic(
                payload or {}, display_model, new_message_id(), omit_thinking=omit_thinking)
        except IncompleteResponseError as exc:
            return claude_error(502, "api_error", str(exc))
        # 与 OpenAI 通道的完成日志同构：显示模型 | 耗时 | tokens
        usage = (anthropic_payload.get("usage") or {}) if isinstance(anthropic_payload, dict) else {}
        tokens = ""
        if usage.get("input_tokens") or usage.get("output_tokens"):
            tokens = f" | 输入 {usage.get('input_tokens', 0)} / 输出 {usage.get('output_tokens', 0)} tokens"
        print(f"✅ [Claude] 模型 {display_model} | 非流式响应完成 | "
              f"耗时 {time.monotonic() - request_started:.1f}s{tokens}")
        return JSONResponse(content=anthropic_payload)

    # ---- 流式：OpenAI SSE 帧 → Anthropic SSE 事件 ----
    return _claude_stream_response(response, display_model, omit_thinking=omit_thinking,
                                   started_at=request_started)


def _sse_data_lines(frame_text: str):
    """从单条 SSE 帧中提取全部 data: 行负载（多行 data 按 SSE 规范拼接）。"""
    data_parts: list[str] = []
    for line in frame_text.splitlines():
        if line.startswith("data:"):
            data_parts.append(line[len("data:"):].lstrip(" "))
        elif line.startswith(":"):
            continue  # SSE comments do not dispatch or split the current event.
        elif not line.strip():
            if data_parts:
                yield "\n".join(data_parts)
                data_parts = []
    if data_parts:
        yield "\n".join(data_parts)


# ============================================================
# 方法约束：非 POST 访问 /v1/messages 系列端点 → 405 Anthropic 形状
# ============================================================

@router.api_route("/v1/messages", methods=["GET", "PUT", "DELETE", "PATCH", "OPTIONS"])
async def claude_messages_method_guard(fastapi_request: Request):
    """对齐 vertex-master 的方法检查：非 POST 一律 405 invalid_request_error。

    不挂鉴权依赖：405 语义与凭据无关（官方 API 同样未授权也回 405），
    且避免在方法错误的请求上多吐一条鉴权日志。
    """
    return claude_error(405, "invalid_request_error", "method not allowed: use POST")


@router.api_route("/v1/messages/count_tokens", methods=["GET", "PUT", "DELETE", "PATCH", "OPTIONS"])
async def claude_count_tokens_method_guard(fastapi_request: Request):
    return claude_error(405, "invalid_request_error", "method not allowed: use POST")


# ============================================================
# count_tokens：POST /v1/messages/count_tokens
# ============================================================

COUNT_TOKENS_TIMEOUT_SECONDS = 15.0
_count_tokens_slots = threading.BoundedSemaphore(4)


async def _express_exact_input_tokens(request_obj, fastapi_request: Request,
                                      raw_internal_model: str) -> Optional[int]:
    # Auxiliary requests have their own small, fail-fast budget. Saturation and
    # timeouts fall back to estimation; cancellation always propagates.
    if not _count_tokens_slots.acquire(blocking=False):
        return None
    usage = ProxyUsage()
    token = current_proxy_usage.set(usage)
    try:
        async with asyncio.timeout(COUNT_TOKENS_TIMEOUT_SECONDS):
            return await _count_exact_input_tokens(request_obj, fastapi_request, raw_internal_model)
    except TimeoutError:
        return None
    finally:
        current_proxy_usage.reset(token)
        usage.close()
        _count_tokens_slots.release()


async def _count_exact_input_tokens(request_obj, fastapi_request: Request,
                                    raw_internal_model: str) -> Optional[int]:
    """Express 通道模型侧精确计数（vertex-master CountTokensExact 的对应物）。

    计数口径：协议转换后的客户端请求本身（消息 contents + system_instruction
    + tools），不含控制台注入的 system / 预填充——count_tokens 语义是
    “客户端请求占多少 token”，注入部分是代理自身的常量开销，不由客户端
    上下文管理买单（与 vertex-master 把 prompt policy 注入计入计数的口径
    略有差异，但与 Anthropic 官方 count_tokens = 请求原文口径一致）。
    contents 由 create_gemini_prompt 生成：与真实生成请求同一转换器，
    Gemini 3.x 的思考签名哨兵也会被计入（生成时确实存在）。

    任一环节失败（无 Key / 代理解析失败 / 上游错误 / contents 为空）返回
    None，由调用方回退启发式估算：与 vertex-master「错误透传」不同，
    本服务双通道架构下计数通道≠生成通道，计数失败不代表请求无效，
    count_tokens 是辅助端点，可用性优先（Claude Code 上下文管理依赖它）。
    """
    app_state_obj = getattr(fastapi_request, "app", None)
    manager = getattr(getattr(app_state_obj, "state", None), "express_key_manager", None)
    get_record = getattr(manager, "get_express_record", None)
    if not callable(get_record):
        return None
    try:
        record = get_record()
    except Exception:
        return None
    if not isinstance(record, dict) or not record.get("key"):
        return None

    base_model, _, model_error = _normalize_model_name(raw_internal_model)
    if model_error or not base_model:
        return None
    try:
        # 放线程池：create_gemini_prompt 内含远程图片下载（同步 httpx +
        # DNS + 最多 3 跳重定向 + 代理链重试，单次超时 10s）与 PIL 全图
        # 压缩——Express 主路径（api_helpers）与 Cookie 路径（cookie_request）
        # 都已线程化，唯独此处曾在事件循环上直跑：多模态 count_tokens 可把
        # 整个服务冻结 10~40s，所有并发流的心跳集体停摆（2026-10 修复）。
        contents = await run_media_task(create_gemini_prompt, request_obj.messages, base_model)
    except Exception:
        return None
    if not contents:
        return None

    try:
        settings = app_state.get_effective_settings(base_model)
        model_path = resolve_express_model_path(
            base_model, settings, key_project_id=str(record.get("project_id") or ""))
        client_wrapper = await FailoverSDKClient(
            ProxyChain(record).bind_to_generation(),
            lambda raw, resolved: get_pooled_express_client(record["key"], raw),
        ).prepare()
        sdk_client = getattr(client_wrapper, "client", None)
        count_tokens_call = getattr(getattr(sdk_client, "aio", None), "models", None)
        count_tokens_call = getattr(count_tokens_call, "count_tokens", None)
        if not callable(count_tokens_call):
            return None
        config = types.CountTokensConfig(
            system_instruction=_system_instruction(request_obj) or None,
            tools=_function_tools(request_obj) or None,
        )
        response = await count_tokens_call(model=model_path, contents=contents, config=config)
        total = getattr(response, "total_tokens", None)
        if isinstance(total, bool) or not isinstance(total, int) or total < 0:
            return None
        return total
    except ProxyResolveError:
        return None
    except Exception as exc:
        print(f"⚠️ [Claude] count_tokens 精确计数失败，回退启发式估算：{type(exc).__name__}: {exc}")
        return None


@router.post("/v1/messages/count_tokens")
async def claude_count_tokens(fastapi_request: Request, api_key: str = Depends(get_claude_auth)):
    """统计请求输入 token 数（Claude Code 上下文管理依赖此端点）。

    两级口径：
    1. Express 通道有可用 Key → 调 Vertex countTokens 拿模型侧精确计数
       （消息 contents + system_instruction + tools 全部计入，同真实生成
       请求的分词器；比 vertex-master 只计 contents 多算了 tools，对齐
       Anthropic 官方 count_tokens 含工具声明的口径）；
    2. 纯 Cookie 通道 / 精确计数任一环节失败 → 启发式估算：
       system / 消息文本 / tool_use 入参 / tool_result / 工具声明全部计入，
       CJK ≈ 1 token/字、其余 ≈ 4 字符/token；量级正确，非逐 token 精确。
    """
    try:
        body = await _parse_json_body(fastapi_request)
    except ClaudeRequestError as exc:
        return claude_error(400, exc.err_type, str(exc))

    raw_model = body.get("model")
    if not isinstance(raw_model, str) or not raw_model.strip():
        return claude_error(400, "invalid_request_error", "model is required")

    display_model, internal_model = resolve_claude_model(raw_model, await _available_models())
    try:
        # 复用同一套消息校验（但不要求 max_tokens）
        request_obj = anthropic_to_openai_request(body, internal_model, require_max_tokens=False)
    except ClaudeRequestError as exc:
        return claude_error(exc.status, exc.err_type, str(exc))

    # Express 通道有 Key 时优先模型侧精确计数（同 Vertex countTokens 分词器）；
    # 无 Key（纯 Cookie 通道）/ 任一失败 → 启发式估算（量级正确，非逐 token 精确）。
    exact = await _express_exact_input_tokens(request_obj, fastapi_request, internal_model)
    if exact is not None:
        print(f"🔢 [Claude] count_tokens 模型 {display_model} → {internal_model}："
              f"精确 {exact} tokens（Express 分词器）")
        return JSONResponse(content={"input_tokens": exact})
    estimated = estimate_request_tokens(body)
    print(f"🔢 [Claude] count_tokens 模型 {display_model} → {internal_model}：估算 ~{estimated} tokens")
    return JSONResponse(content={"input_tokens": estimated})


# ============================================================
# Claude Code 网关探测：GET /api/hello
# ============================================================

@router.api_route("/api/hello", methods=["GET", "HEAD"])
async def claude_hello(fastapi_request: Request):
    """Claude Code 启动时对自定义网关的可达性探测（不鉴权、不泄露信息）。"""
    response = JSONResponse(content={"status": "ok"})
    if fastapi_request.method == "HEAD":
        response.body = b""
    return response


@router.api_route("/api/hello", methods=["POST", "PUT", "DELETE", "PATCH", "OPTIONS"])
async def claude_hello_method_guard(fastapi_request: Request):
    """Claude Code 网关探测仅接受 GET/HEAD。"""
    return claude_error(405, "invalid_request_error", "method not allowed: use GET")


# ============================================================
# Anthropic 格式模型目录（供 models_api 的 /v1/models 复用）
# ============================================================

def is_anthropic_client(fastapi_request: Request) -> bool:
    """/v1/models 的响应格式判定：anthropic-version 头或 x-api-key 头存在
    即视为 Anthropic 客户端（OpenAI 客户端发 Authorization: Bearer）。
    getattr 兜底：单测里常以 SimpleNamespace 直调路由，请求对象可能没有 headers。"""
    headers = getattr(fastapi_request, "headers", None) or {}
    get_header = getattr(headers, "get", None)
    if callable(get_header):
        if get_header("anthropic-version"):
            return True
        return bool(get_header("x-api-key"))
    return False


async def anthropic_models_data() -> dict:
    """Anthropic 模型目录数据体（列表 /v1/models 与单模型检索共用）。

    变体目录与 OpenAI /v1/models 同源展开（anti_truncation.catalog_variant_ids
    单一事实源）：-search 变体与按模型设置开启的抗截断后缀一并列出——
    请求侧 resolve_claude_model 对非 claude-* 名字原样透传，Claude 客户端
    可直接使用这些变体，目录应与实际能力一致。
    """
    from anti_truncation import catalog_variant_ids, split_model_alias

    available = await _available_models()
    settings_base = app_state.get_settings()
    variants: list[str] = []
    for model_id in available:
        plain_id, _ = split_model_alias(model_id)
        effective = app_state.get_effective_settings(plain_id, base=settings_base)
        variants.extend(catalog_variant_ids(model_id, effective))
    return anthropic_models_payload(available, variants)


async def anthropic_model_list() -> JSONResponse:
    return JSONResponse(content=await anthropic_models_data())

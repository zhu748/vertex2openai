"""Cookie 通道调用编排：凭证装配、请求校验与交付路径分发。

请求构造由 cookie_request 负责，响应解析由 cookie_response 负责，
四条交付路径（真流式 / 非流式整取 / 生图假流式 / 缓冲假流式）的实现
自 2026-11 P2-2 起在 cookie_delivery（本模块保留每请求上下文类
CookieGeneration、装配函数与路由适配器）。
"""

import time
import uuid
from dataclasses import dataclass
import config as app_config
import httpx
import model_capabilities as mc
from anti_truncation import TextTransport, prepare_transport, split_model_alias
from fastapi import Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from models import OpenAIRequest
from request_tracking import configure as track_configure
from runtime_state import app_state
from schema_validation import SchemaValidationError
from upstream_errors import create_openai_error_response
from upstreams.base import BaseUpstream
from channel_capabilities import cookie_feature_error
from credential_manager import credential_manager
from proxy_failover import ProxyChain
from request_validation import validate_openai_request
from impersonate import open_upstream_client  # noqa: F401  # 测试 seam：与 cookie_delivery/cookie_native 同名 patch 面
from upstreams.cookie_request import (
    classify_tool_traffic,
    is_forced_builtin_search,
    search_suffix_has_priority,
)
from cookie_auth import build_headers
from upstreams.request_preparation import prepare_messages
from retry_policy import get_retry_settings  # noqa: F401  # 交付路径 seam
from upstreams.cookie_delivery import (  # noqa: F401
    _RequestBodyCache,
    _collect_full_response,
    deliver_buffered_frames,
    deliver_collect_response,
    deliver_image_frames,
    deliver_stream_events,
    deliver_stream_frames,
)
from upstreams.cookie_transport import stream_events  # noqa: F401  # 测试 seam
from usage import estimate_usage, map_usage, messages_prompt_text, wants_usage  # noqa: F401

_THINKING_RUNAWAY_HINT = (
    "这通常是原生思考在高强度下跑飞或被截断（前端如 SillyTavern 常发 reasoning_effort=xhigh，"
    "会覆盖控制台档位）。解决：控制台“模型参数→思考强度→原生思考控制”选“关闭原生思考”"
    "（可用“保存为该模型专属”只对本模型生效），即忽略前端强度、压到 minimal 并剥离原生思考。"
)


_NO_BODY_HINT = "可能被安全策略拦截或接口行为变化；原始响应样本已写入运行日志，可到大盘“运行日志”页查看。"



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

    async def response(self) -> Response:
        """按请求形态分发到 cookie_delivery 的四条交付路径。"""
        if self.is_stream and self.is_image:
            return StreamingResponse(
                deliver_image_frames(self), media_type="text/event-stream"
            )
        if self.is_stream:
            return StreamingResponse(
                deliver_stream_frames(self), media_type="text/event-stream"
            )
        if self.request_obj.stream:
            return StreamingResponse(
                deliver_buffered_frames(self), media_type="text/event-stream"
            )
        return await deliver_collect_response(self)


def _get_cookie_string() -> str:
    return app_state.get_google_cookie() or ""


def _get_project_id() -> str:
    return app_state.get_project_id() or ""


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
    # 生图分辨率别名（-1k/-2k/-4k/-512）与 -search 按任意顺序循环剥离
    #（2026-11 第五轮）：目录从不叠加两种变体，但手打名两种次序都应全生效
    #——旧实现固定「先 -search 后档位」："model-search-4k" 的 -search 对尾部
    # 检查不可见，档位剥离后残留 "-search" 发往上游（404）；"model-4k-search"
    # 的档位提示则对 resolve_image_size 不可见（静默回落 1K）。档位由
    # resolve_image_size 从请求原始名解析（仅生图档案生效）。
    base_model_name, _search_seen = mc.strip_image_and_search_suffixes(
        base_model_name
    )
    is_grounded_search = is_grounded_search or _search_seen
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
        proxy_chain.log_current()
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
        credential_record: dict=None,
    ) -> Response:
        prepared = _prepare_cookie_generation(
            request_obj, fastapi_request, credential_record
        )
        if isinstance(prepared, JSONResponse):
            return prepared
        return await prepared.response()

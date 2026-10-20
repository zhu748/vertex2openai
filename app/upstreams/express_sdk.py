from functools import partial

from typing import Any

from fastapi import Request
from fastapi.responses import JSONResponse

from models import OpenAIRequest
from generation_config import create_generation_config
from express_endpoint import resolve_express_model_path
from upstream_errors import create_openai_error_response
from anti_truncation import prepare_transport, split_model_alias
from upstreams.base import BaseUpstream
from upstreams.request_preparation import prepare_messages
from api_helpers import execute_gemini_call
from message_processing import create_gemini_prompt
from client_pool import get_pooled_express_client
import model_capabilities as mc
from runtime_state import app_state
from proxy_resolver import ProxyResolveError, mask_proxy_uri
from proxy_failover import FailoverSDKClient, ProxyChain
from schema_validation import SchemaValidationError
from request_validation import validate_openai_request
from request_tracking import configure as track_configure

LEGACY_EXPRESS_PREFIX = "[EXPRESS] "
LEGACY_PAY_PREFIX = "[PAY]"
OPENAI_DIRECT_SUFFIX = "-openai"
OPENAI_SEARCH_SUFFIX = "-openaisearch"


def _normalize_model_name(model_name: str) -> tuple[str, bool, str | None]:
    base_model_name, _ = split_model_alias(model_name)
    if base_model_name == LEGACY_EXPRESS_PREFIX.strip():
        return "", False, "模型名称不能为空。"

    if base_model_name.startswith(LEGACY_EXPRESS_PREFIX):
        base_model_name = base_model_name[len(LEGACY_EXPRESS_PREFIX) :].strip()

    if base_model_name.startswith(LEGACY_PAY_PREFIX):
        return (
            base_model_name,
            False,
            "当前版本已经移除 Pay/Service Account 模式，请改用 Express Mode 模型名称。",
        )

    if base_model_name.endswith(OPENAI_SEARCH_SUFFIX) or base_model_name.endswith(OPENAI_DIRECT_SUFFIX):
        return (
            base_model_name,
            False,
            "当前版本已经移除 -openai/-openaisearch 直连上游路径，请直接使用普通模型名或 -search 模型名。",
        )

    is_grounded_search = base_model_name.endswith("-search")
    if is_grounded_search:
        base_model_name = base_model_name[: -len("-search")]

    if not base_model_name.strip():
        return base_model_name, False, "模型名称不能为空。"

    return base_model_name, is_grounded_search, None


def _build_thinking_config(
    base_model_name: str,
    request: OpenAIRequest,
    prefill_active: bool = False,
    *,
    settings: dict | None = None,
) -> dict | None:
    """按模型能力档案 + 控制台设置 + 单次请求构建思考配置（SDK 线格式）。
    生图模型同样下发 thinking_level（三模型默认 HIGH，用户决策 2026-10-13）。"""
    if settings is None:
        settings = app_state.get_effective_settings(base_model_name)
    t = mc.resolve_thinking(base_model_name, request, settings, prefill_active=prefill_active)
    if t.get("mode") is None:
        return None

    thinking_config = {"include_thoughts": t.get("include_thoughts", True)}

    if t["mode"] == "level":
        thinking_config["thinking_level"] = t["level"]

    if app_state.get_setting("debug_outbound", False):
        print(f"🔎 [出站调试] Express 通道 模型={base_model_name} thinkingConfig={thinking_config}")

    return thinking_config


_ENDPOINT_LOGGED = False


def _log_resolved_endpoint(client: Any) -> None:
    """把 SDK 实际解析出的上游端点打进日志（每进程一次）。

    为什么需要它：Express（api_key）模式下**无法**指定 location——SDK 里
    project/location 与 api_key 互斥，硬传会 `ValueError: Project/location and
    API key are mutually exclusive`。SDK 解析出的 base_url 就是**全局端点**
    https://aiplatform.googleapis.com/ ，project/location 均为 None，请求 URL 里
    根本没有 location 段。因此报错信息里出现的 `locations/xxx` 区域是 **Google 后端
    为该 Key 自行路由**的结果，不是本代理选的，客户端侧无从指定。
    打印出来便于随时核实（尤其怀疑"被路由到冷门区域"时）。
    """
    global _ENDPOINT_LOGGED
    if _ENDPOINT_LOGGED:
        return
    _ENDPOINT_LOGGED = True
    try:
        api = client._api_client
        base_url = getattr(api._http_options, "base_url", None)
        api_version = getattr(api._http_options, "api_version", None)
        scope = (
            "全局端点（URL 无 location 段）"
            if base_url == "https://aiplatform.googleapis.com/"
            else "自定义/区域端点"
        )
        print(
            f"🌐 [上游端点] 标准模式解析结果：base_url={base_url} api_version={api_version} "
            f"project={api.project} location={api.location} → {scope}。"
            "Express Key 模式下具体区域由 Google 后端路由，无法在客户端指定"
            "（如需强制指向某端点，可设环境变量 VERTEX_BASE_URL）。"
        )
    except Exception as e:
        print(f"⚠️ [上游端点] 读取端点解析结果失败（不影响调用）：{e}")


class ExpressSDKUpstream(BaseUpstream):
    """
    官方 API Key Express Mode 渠道处理器
    封装了原有的多密钥切匙、代理挂载以及 SDK 运行时调用
    """

    async def chat_completions(self, request_obj: OpenAIRequest, fastapi_request: Request, *, credential_record=None):
        track_configure(channel="express")
        try:
            validate_openai_request(request_obj)
        except SchemaValidationError as exc:
            return JSONResponse(
                status_code=400,
                content=create_openai_error_response(400, str(exc), "invalid_request_error"),
            )

        express_key_manager_instance = fastapi_request.app.state.express_key_manager

        base_model_name, is_grounded_search, model_error = _normalize_model_name(request_obj.model)
        if model_error:
            print(f"❌ [模型名称] {model_error} 收到的模型名：{request_obj.model}")
            return JSONResponse(
                status_code=400,
                content=create_openai_error_response(400, model_error, "invalid_request_error"),
            )

        if express_key_manager_instance.get_total_keys() == 0:
            error_msg = (
                "未配置任何 Express API Key，无法调用 Gemini Express Mode"
                "（请在控制台「通道与凭证」页添加，或设环境变量 VERTEX_EXPRESS_API_KEY）。"
            )
            print(f"❌ [密钥配置] {error_msg}")
            return JSONResponse(
                status_code=401,
                content=create_openai_error_response(401, error_msg, "authentication_error"),
            )

        key_record = credential_record or express_key_manager_instance.get_express_record()
        if not key_record:
            error_msg = "没有可用的 Express API Key。"
            print(f"❌ [密钥配置] {error_msg}")
            return JSONResponse(
                status_code=401,
                content=create_openai_error_response(401, error_msg, "authentication_error"),
            )

        express_api_key = key_record["key"]
        track_configure(project_id=key_record.get("project_id", ""))
        _inj_settings = app_state.get_effective_settings(base_model_name)
        # 多账号：location 钉定用**这个 Key 自己的**项目，而不是 Cookie 通道的全局项目
        model_to_call = resolve_express_model_path(
            base_model_name, _inj_settings, key_project_id=key_record.get("project_id", "")
        )
        # The request owns the ordered route, while the pool owns reusable
        # clients for each selected outlet. No per-call client is closed here.
        async def client_for(record):
            return await FailoverSDKClient(
                ProxyChain(record).bind_to_generation(),
                lambda raw, resolved: get_pooled_express_client(record["key"], raw),
            ).prepare()

        try:
            # Resolve protocol bridges asynchronously and skip failed candidates.
            client_to_use = await client_for(key_record)
        except ProxyResolveError as e:
            print(f"❌ [出站代理] {e}")
            return JSONResponse(
                status_code=502,
                content=create_openai_error_response(502, f"出站代理不可用：{e}", "proxy_error"),
            )
        if client_to_use.chain.current:
            print(f"🌐 [出站代理] 当前出口：{mask_proxy_uri(client_to_use.chain.current)}")
        _log_resolved_endpoint(client_to_use)
        print(f"🌐 [上游端点] 使用官方 Gemini Express Mode SDK 调用模型 {base_model_name}。")
        if model_to_call != base_model_name:
            print(f"🌐 [上游端点] 已钉定 location：{model_to_call}")

        profile = mc.get_profile(base_model_name)

        request_obj, prefill_text, prefill_active = prepare_messages(
            request_obj, profile, _inj_settings, has_tools=bool(request_obj.tools)
        )

        request_obj, text_transport = prepare_transport(
            request_obj,
            base_model_name,
            _inj_settings,
            search=is_grounded_search,
            native_stream=not app_state.get_setting("fake_streaming", False),
            prefill=prefill_active or bool(prefill_text),
        )
        gen_config_dict = create_generation_config(request_obj)
        if text_transport and text_transport.native_stream:
            gen_config_dict["tool_config"]["function_calling_config"]["stream_function_call_arguments"] = True
        thinking_config = _build_thinking_config(
            base_model_name, request_obj, prefill_active=prefill_active, settings=_inj_settings
        )
        if thinking_config:
            gen_config_dict["thinking_config"] = thinking_config

        _tools_disabled = (
            isinstance(request_obj.tool_choice, str) and request_obj.tool_choice.lower() == "none"
        )
        if is_grounded_search and not _tools_disabled:
            search_tool = {"google_search": {}}
            if "tools" in gen_config_dict and isinstance(gen_config_dict["tools"], list):
                if search_tool not in gen_config_dict["tools"]:
                    gen_config_dict["tools"].append(search_tool)
            else:
                gen_config_dict["tools"] = [search_tool]
            print(f"🔎 [搜索增强] 已为模型 {base_model_name} 启用 Google Search 工具。")

        # 传入真实模型名：create_gemini_prompt 需要它来判断是否对缺失的思考签名
        # 启用官方哨兵（仅 Gemini 3.x 强校验，见 thought_signatures.resolve_tool_call_signature）
        prompt_func = partial(create_gemini_prompt, model_name=base_model_name)

        if app_state.get_setting("debug_outbound", False):
            _dbg = {
                k: v
                for k, v in gen_config_dict.items()
                if k
                in (
                    "temperature",
                    "top_p",
                    "top_k",
                    "candidate_count",
                    "max_output_tokens",
                    "stop_sequences",
                    "thinking_config",
                    "response_modalities",
                    "image_config",
                )
            }
            print(f"🔎 [出站调试] Express 通道 生成参数={_dbg}")

        async def retry_target_factory():
            # A newly selected proxy must be warmed asynchronously as well.
            nonlocal express_api_key, key_record, client_to_use
            record = express_key_manager_instance.get_express_record(exclude_key=express_api_key)
            if record is None:
                return None
            if any(record.get(k) != key_record.get(k) for k in
                   ("key", "project_id", "proxies", "proxy", "proxy_fallback")):
                client_to_use = await client_for(record)
            key_record = record
            express_api_key = record["key"]
            model_path = resolve_express_model_path(
                base_model_name, _inj_settings, key_project_id=record.get("project_id", "")
            )
            track_configure(project_id=record.get("project_id", ""), upstream_model=model_path)
            return (
                client_to_use,
                model_path,
                base_model_name if model_path != base_model_name else None,
            )

        return await execute_gemini_call(
            client_to_use,
            model_to_call,
            prompt_func,
            gen_config_dict,
            request_obj,
            fastapi_request=fastapi_request,
            prefill_text=prefill_text,
            # 钉定路径万一不对（Project ID 与 Key 不同项目、该区域没有此模型），
            # 自动退回裸模型名重试一次。退回重试复用同一个池化客户端
            # （同 Key 同签名 → 同实例，零额外开销）。
            fallback_model=(base_model_name if model_to_call != base_model_name else None),
            fallback_client_factory=(lambda: client_to_use),
            retry_target_factory=retry_target_factory,
            text_transport=text_transport,
        )

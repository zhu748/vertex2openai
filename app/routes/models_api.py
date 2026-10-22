import time
from typing import List, Dict, Any, Set, Optional
from fastapi import APIRouter, Depends, Header, HTTPException, Request

from auth import validate_api_key
from model_loader import get_express_models
from runtime_state import app_state
from anti_truncation import split_model_alias, listed_suffixes
from routes import claude_api

router = APIRouter()


async def get_models_api_key(
    fastapi_request: Request,
    authorization: Optional[str] = Header(None),
    x_api_key: Optional[str] = Header(None, alias="x-api-key"),
    x_goog_api_key: Optional[str] = Header(None, alias="x-goog-api-key"),
) -> str:
    """模型目录鉴权依赖：提取候选 Key（Bearer / x-api-key / x-goog-api-key / ?key=）
    并就地完成有效性校验——只有这里能同时看到请求头，从而按客户端协议选择错误
    形状：Anthropic 客户端要 {"type":"error",...}（经 ClaudeAPIError 全局处理器），
    OpenAI 客户端保持原有 detail 形状。

    单测直调路由时依赖被整体绕过，与旧版 get_api_key 的行为契约一致。"""
    if authorization and authorization.startswith("Bearer "):
        candidate = authorization[len("Bearer "):]
    else:
        candidate = (x_api_key or x_goog_api_key
                     or fastapi_request.query_params.get("key", ""))
    anthropic = claude_api.is_anthropic_client(fastapi_request)
    if not candidate:
        if anthropic:
            raise claude_api.ClaudeAPIError(claude_api.claude_error(
                401, "authentication_error",
                "缺少 API Key：请传入 x-api-key 或 Authorization: Bearer 请求头。"))
        raise HTTPException(
            status_code=401,
            detail="缺少 API Key，请在请求头中传入 Authorization: Bearer YOUR_API_KEY。",
        )
    if not validate_api_key(candidate):
        if anthropic:
            raise claude_api.ClaudeAPIError(
                claude_api.claude_error(401, "authentication_error", "API Key 无效。"))
        raise HTTPException(status_code=401, detail="API Key 无效。")
    return candidate


@router.get("/v1/models")
async def list_models(fastapi_request: Request, api_key: str = Depends(get_models_api_key)):
    express_key_manager_instance = fastapi_request.app.state.express_key_manager

    # Anthropic 客户端：返回 Anthropic List Models 格式（claude 别名 + Gemini 模型）
    if claude_api.is_anthropic_client(fastapi_request):
        return await claude_api.anthropic_model_list()

    return {"object": "list", "data": await openai_model_entries(express_key_manager_instance)}


@router.get("/v1/models/{model_id}")
async def retrieve_model(model_id: str, fastapi_request: Request,
                         api_key: str = Depends(get_models_api_key)):
    """单模型检索（Anthropic models.retrieve / OpenAI models.retrieve 双格式）。

    Anthropic SDK 与 claude-code-router 等客户端在启动时会拉取并逐个检索
    模型元数据；找不到时 Anthropic 客户端要 not_found_error 形状，
    OpenAI 客户端保持 HTTPException detail 形状（与列表端点同口径）。
    检索范围与各自列表口径完全一致：Anthropic 侧 claude 别名 + Gemini 模型，
    OpenAI 侧含 -search / 抗截断后缀的展开目录。
    """
    express_key_manager_instance = fastapi_request.app.state.express_key_manager

    if claude_api.is_anthropic_client(fastapi_request):
        data = await claude_api.anthropic_models_data()
        for entry in data.get("data") or []:
            if isinstance(entry, dict) and entry.get("id") == model_id:
                return entry
        raise claude_api.ClaudeAPIError(claude_api.claude_error(
            404, "not_found_error", f"model: {model_id} not found"))

    for entry in await openai_model_entries(express_key_manager_instance):
        if entry.get("id") == model_id:
            return entry
    raise HTTPException(
        status_code=404,
        detail=f"The model '{model_id}' does not exist or you do not have access to it.")


async def openai_model_entries(express_key_manager_instance) -> List[Dict[str, Any]]:
    """OpenAI 格式模型目录（列表 /v1/models 与检索 /v1/models/{id} 共用）。"""
    # 混合模式也可以仅启用 Cookie；模型目录与独立 Cookie 模式保持一致。
    has_web_proxy = app_state.get_channel_mode() in ("web_proxy", "mixed")
    has_express_key = express_key_manager_instance.get_total_keys() > 0

    raw_models = await get_express_models() if (has_express_key or has_web_proxy) else []

    # 设置基底只取一次（旧实现每个模型都 get_effective_settings → 全量
    # deepcopy 整个设置树，N 个模型 = N 次深拷贝；现在每模型只剩浅拷贝）。
    settings_base = app_state.get_settings()

    final_model_list: List[Dict[str, Any]] = []
    processed_ids: Set[str] = set()
    current_time = int(time.time())

    def add_model(base_id: str):
        suffixes = [""]
        plain_id, mode = split_model_alias(base_id)
        available = listed_suffixes(plain_id, app_state.get_effective_settings(plain_id, base=settings_base))
        if mode is not None and f"-antitruncation-{mode}" not in available:
            return
        if mode is None and not base_id.endswith("-search") and "gemini" in base_id.lower() and "image" not in base_id.lower():
            suffixes.append("-search")
        # Native Gemini listings remain a transparent upstream catalog. These
        # transport variants belong only to the OpenAI-compatible endpoint.
        if mode is None:
            suffixes.extend(available)

        for suffix in suffixes:
            final_id = f"{base_id}{suffix}"
            if final_id in processed_ids:
                continue
            final_model_list.append({
                "id": final_id,
                "object": "model",
                "created": int(current_time),
                "owned_by": "google",
                "permission": [],
                "root": base_id,
                "parent": None,
            })
            processed_ids.add(final_id)

    for model_id in raw_models:
        add_model(model_id)

    return sorted(final_model_list, key=lambda item: item["id"])

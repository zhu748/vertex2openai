import time
from fastapi import APIRouter, Depends, Request
from typing import List, Dict, Any, Set
from auth import get_api_key
from model_loader import get_express_models
from runtime_state import app_state
from anti_truncation import split_model_alias, listed_suffixes

router = APIRouter()

@router.get("/v1/models")
async def list_models(fastapi_request: Request, api_key: str = Depends(get_api_key)):
    express_key_manager_instance = fastapi_request.app.state.express_key_manager
    
    # 动态放行：开启网页反代或者配置有 Express API Key 均可安全获取模型列表
    has_web_proxy = app_state.is_web_proxy_enabled()
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

    return {"object": "list", "data": sorted(final_model_list, key=lambda item: item["id"])}

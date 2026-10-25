import httpx
import asyncio
import json
import time
from pathlib import Path
from typing import List, Dict, Optional

import config as app_config
from proxy_failover import ProxyChain
from express_fingerprint import build_direct_headers
from http_io import read_limited_bytes
from proxy_tls import build_httpx_proxy

_model_cache: Optional[Dict[str, List[str]]] = None
_cache_lock = asyncio.Lock()
_next_refresh_allowed = 0.0
_refresh_task: Optional[asyncio.Task] = None
MAX_MODELS_CONFIG_BYTES = 1024 * 1024
MODELS_FETCH_TIMEOUT_SECONDS = 20.0
DEFAULT_MODELS_CONFIG_URL = "https://raw.githubusercontent.com/bad-woman/vertex2openai/main/vertexModels.json"
FALLBACK_MODELS_CONFIG_URLS = [
    "https://cdn.jsdelivr.net/gh/bad-woman/vertex2openai@main/vertexModels.json",
]

_LOCAL_MODEL_FILE_CANDIDATES = [
    Path.cwd() / "vertexModels.json",
    Path(__file__).resolve().parent / "vertexModels.json",
    Path(__file__).resolve().parent.parent / "vertexModels.json",
]


def _normalize_models_config(data: object) -> Optional[Dict[str, List[str]]]:
    if not isinstance(data, dict):
        return None

    models = data.get("models")
    if isinstance(models, list):
        return {"models": list(dict.fromkeys(model.strip() for model in models
                                             if isinstance(model, str) and model.strip()))}

    legacy_express_models = data.get("vertex_express_models")
    if isinstance(legacy_express_models, list):
        return _normalize_models_config({"models": legacy_express_models})

    return None


def _load_local_models_config() -> Dict[str, List[str]]:
    for local_file in dict.fromkeys(_LOCAL_MODEL_FILE_CANDIDATES):
        try:
            if not local_file.exists():
                continue
            data = json.loads(local_file.read_text(encoding="utf-8"))
            normalized = _normalize_models_config(data)
            if normalized and normalized["models"]:
                print(f"📦 [模型配置] 已使用本地模型配置文件：{local_file}。")
                return normalized
            print(f"⚠️ [模型配置] 本地模型配置为空或结构无效，尝试下一份：{local_file}。")
        except Exception as exc:
            print(f"⚠️ [模型配置] 读取本地模型配置失败，尝试下一份 {local_file}：{exc}")
    searched = ", ".join(str(path) for path in _LOCAL_MODEL_FILE_CANDIDATES)
    print(f"❌ [模型配置] 没有可用的本地 vertexModels.json，已检查：{searched}")
    return {"models": []}


async def fetch_and_parse_models_config() -> Optional[Dict[str, List[str]]]:
    primary_url = app_config.MODELS_CONFIG_URL or DEFAULT_MODELS_CONFIG_URL
    urls_to_try = [primary_url]
    for fallback_url in (FALLBACK_MODELS_CONFIG_URLS if primary_url == DEFAULT_MODELS_CONFIG_URL else []):
        if fallback_url not in urls_to_try:
            urls_to_try.append(fallback_url)

    client_args = {"timeout": 20.0, "trust_env": False}
    if app_config.SSL_CERT_FILE:
        client_args["verify"] = app_config.SSL_CERT_FILE
    chain = ProxyChain()
    fp_headers = build_direct_headers()
    for index, url in enumerate(urls_to_try, start=1):
        print(f"🌐 [模型配置] 正在获取远程模型配置（第 {index} 个地址）：{url}")
        try:
            async def fetch(raw, resolved):
                options = dict(client_args)
                if resolved:
                    options["proxy"] = build_httpx_proxy(resolved, verify=app_config.SSL_CERT_FILE or True)
                async with httpx.AsyncClient(**options) as client:
                    async with client.stream("GET", url, headers=fp_headers) as response:
                        response.raise_for_status()
                        return json.loads(await read_limited_bytes(response, MAX_MODELS_CONFIG_BYTES))
            async with asyncio.timeout(MODELS_FETCH_TIMEOUT_SECONDS):
                normalized = _normalize_models_config(await chain.run(fetch))
            if normalized is not None:
                print(f"✅ [模型配置] 远程模型配置加载成功，共 {len(normalized['models'])} 个模型。")
                return normalized
            print(f"⚠️ [模型配置] 远程模型配置结构无效，准备尝试下一个地址：{url}")
        except httpx.HTTPStatusError as exc:
            print(f"⚠️ [模型配置] 远程模型配置地址返回 HTTP {exc.response.status_code}，准备尝试下一个地址：{url}")
        except httpx.RequestError as exc:
            print(f"⚠️ [模型配置] 获取远程模型配置失败，准备尝试下一个地址。网络错误：{exc}")
        except json.JSONDecodeError as exc:
            print(f"⚠️ [模型配置] 远程模型配置不是有效 JSON，准备尝试下一个地址。解析错误：{exc}")
        except Exception as exc:
            print(f"⚠️ [模型配置] 加载远程模型配置时出现异常，准备尝试下一个地址：{exc}")
    print("⚠️ [模型配置] 所有远程地址都不可用，保留当前模型缓存。")
    return None


async def get_models_config() -> Dict[str, List[str]]:
    global _model_cache
    if _model_cache is not None:
        return _model_cache
    async with _cache_lock:
        if _model_cache is None:
            print("📦 [模型配置] 缓存为空，正在初始化模型列表。")
            _model_cache = _load_local_models_config()
    return _model_cache


async def get_express_models() -> List[str]:
    config = await get_models_config()
    schedule_models_refresh()
    return list(config.get("models", []))


async def refresh_models_config_cache() -> bool:
    global _model_cache, _next_refresh_allowed
    async with _cache_lock:
        if time.monotonic() < _next_refresh_allowed:
            return bool(_model_cache)
        print("🔄 [模型配置] 正在刷新模型配置缓存。")
        try:
            new_config = await fetch_and_parse_models_config()
        except Exception as exc:
            print(f"⚠️ [模型配置] 刷新失败，保留当前缓存：{type(exc).__name__}: {exc}")
            new_config = None
        ok = bool(new_config and new_config.get("models"))
        _next_refresh_allowed = time.monotonic() + (3600 if ok else 300)
        if ok:
            _model_cache = new_config
            print("✅ [模型配置] 模型配置缓存刷新成功。")
            return True
        print("❌ [模型配置] 模型配置缓存刷新失败。")
        return False


def schedule_models_refresh() -> None:
    """统一所有模型列表入口的后台刷新；请求直接使用当前缓存。"""
    global _refresh_task
    if time.monotonic() < _next_refresh_allowed:
        return
    if _refresh_task is None or _refresh_task.done():
        _refresh_task = asyncio.create_task(refresh_models_config_cache())


async def close_models_refresh() -> None:
    global _refresh_task, _next_refresh_allowed
    if _refresh_task is not None:
        if not _refresh_task.done():
            _refresh_task.cancel()
        await asyncio.gather(_refresh_task, return_exceptions=True)
        _refresh_task = None
    _next_refresh_allowed = 0.0

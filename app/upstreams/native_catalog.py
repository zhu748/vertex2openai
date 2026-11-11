"""Gemini 原生透传的目录 / 变体 / URL 层（QUA-P2-9 自 routes/gemini_native.py 拆出）。

模型目录（models.list / models.get 条目与 ID 展开）、-search 与生图分辨率
后缀的最小改写、location 钉定模型路径与上游 URL 构造。纯函数层，无
FastAPI 依赖；透传编排在 upstreams/native_passthrough.py，路由装配在
routes/gemini_native.py（兼容 re-export 层）。
"""

import json
from typing import Optional

import config as app_config
from express_endpoint import resolve_express_model_path
from runtime_state import app_state

EXPRESS_BASE = app_config.VERTEX_BASE_URL or "https://aiplatform.googleapis.com"


EXPRESS_API_VERSION = "v1beta1"


def _native_model_entry(model_id: str) -> dict:
    """Gemini 原生 models.list / models.get 共用的目录条目。"""
    lowered = model_id.lower()
    if lowered.endswith("-search"):
        variant_note = "（Google 搜索增强变体：请求时注入 google_search 工具）"
    else:
        # 生图分辨率后缀变体（-1k/-2k/-4k/-512）：与 -search 同为目录列出的
        # 本服务扩展名，透传侧剥后缀并注入 imageConfig.imageSize。
        from model_capabilities import image_resolution_suffix
        tier = image_resolution_suffix(model_id)[1]
        variant_note = f"（生图分辨率变体：等价 imageConfig.imageSize={tier}）" if tier else ""
    return {
        "name": f"models/{model_id}",
        "displayName": model_id,
        "description": f"Vertex Express 模型（经 vertex2openai 透传）：{model_id}{variant_note}",
        "inputTokenLimit": 1048576,
        "outputTokenLimit": 65536,
        "supportedGenerationMethods": ["generateContent", "streamGenerateContent", "countTokens"],
    }


def _native_catalog_ids(raw_models: list) -> list[str]:
    """原生目录 ID 列表：裸模型 + -search 变体 + 生图分辨率变体
    （与 OpenAI / Anthropic 目录同源，规则来自 anti_truncation.
    search_variant_available 与 model_capabilities.image_size_variant_ids）。

    抗截断后缀不列入：其解码/重建语义只在 OpenAI 兼容端点的统一管线里
    实现，原生透传无法承载（保持「透明目录」的老约束，测试也断言这一点）。
    分辨率变体可列：透传侧 _strip_image_size_variant 做剥后缀 + 注入
    imageConfig.imageSize 的最小改写（与 -search 同模式）。
    """
    from anti_truncation import search_variant_available
    from model_capabilities import image_size_variant_ids

    ids: list[str] = []
    seen: set[str] = set()
    for model_id in sorted(raw_models):
        extra: list[str] = []
        if search_variant_available(model_id):
            extra.append(f"{model_id}-search")
        # 生图模型无 -search 变体，两类后缀互斥，不会叠出 -4k-search。
        extra.extend(f"{model_id}{suffix}" for suffix in image_size_variant_ids(model_id))
        for candidate in (model_id, *extra):
            if candidate not in seen:
                seen.add(candidate)
                ids.append(candidate)
    return ids


def _split_model_action(model_action: str) -> tuple[str, str]:
    """把 `gemini-3.6-pro:generateContent` 拆成 (model, action)。

    模型名本身不含冒号（Gemini 命名规范），第一个冒号即为分隔符。
    客户端若传了 `models/gemini-xxx:action` 形式，也一并剥掉前缀。
    """
    name = model_action
    if name.startswith("models/"):
        name = name[len("models/"):]
    if ":" in name:
        model, _, action = name.partition(":")
        return model.strip(), action.strip()
    return name.strip(), ""


def _strip_search_variant(model: str, action: str, parsed: dict) -> tuple[str, Optional[bytes]]:
    """-search 变体的最小改写：返回 (基础模型名, 重组后的请求体或 None)。

    与 OpenAI / Claude 端点对齐：三套协议的模型目录都列出 -search 变体，
    请求侧统一「剥后缀 + 强制 Google 搜索」。原生透传的差异在于普通模型
    保持字节级透传不变；仅当模型名显式带 -search 后缀（本服务的扩展名，
    不存在任何「客户端 SDK 原生序列化形态」，Google 也无此模型名）时才
    重组 body——否则该请求会在上游 404（Publisher model not found）。

    - 生成动作（generateContent / streamGenerateContent）：注入
      {"google_search": {}} 工具（客户端已自带搜索工具 / tools 非法时不注入，
      交给上游按原样处理）；
    - countTokens / embedContent：只剥后缀（body 与模型无关，无需重组）。
    body 为 None 表示未做任何改写，调用方继续用原始字节。
    """
    from anti_truncation import search_variant_available

    lowered = (model or "").lower()
    if not lowered.endswith("-search") or len(lowered) <= len("-search"):
        return model, None
    base = model[: -len("-search")]
    if not search_variant_available(base):
        return model, None
    if action not in ("generateContent", "streamGenerateContent"):
        return base, None
    tools = parsed.get("tools")
    if tools is None:
        parsed["tools"] = [{"google_search": {}}]
    elif isinstance(tools, list) and all(isinstance(t, dict) for t in tools):
        if any("google_search" in t or "googleSearch" in t for t in tools):
            return base, None  # 客户端已自带搜索工具
        parsed["tools"] = [*tools, {"google_search": {}}]
    else:
        return base, None  # tools 非法：原样透传，让上游报真实错误
    body = json.dumps(parsed, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return base, body


def _strip_image_size_variant(model: str, action: str, parsed: dict) -> tuple[str, Optional[bytes]]:
    """生图分辨率后缀变体的最小改写：返回 (基础模型名, 重组后的请求体或 None)。

    与 -search 变体同一模式（目录列出 -1k/-2k/-4k/-512 别名，本服务扩展名、
    Google 无此模型 ID，原样透传会 404）：剥后缀并把档位写进
    generationConfig.imageConfig.imageSize——客户端已显式设置 imageSize 时
    不覆盖（与 resolve_image_size 的优先级链一致：请求级 > 模型名后缀）。

    - 生成动作（generateContent / streamGenerateContent）：注入 imageSize
      （generationConfig / imageConfig 非法时只剥后缀，交给上游报真实错误）；
    - countTokens / embedContent：只剥后缀（body 与模型无关，无需重组）。
    body 为 None 表示未做任何改写，调用方继续用原始字节。
    """
    from model_capabilities import image_resolution_suffix

    base, tier = image_resolution_suffix(model)
    if tier is None:
        return model, None
    if action not in ("generateContent", "streamGenerateContent"):
        return base, None
    gen = parsed.get("generationConfig")
    if gen is None:
        # 极简调用可以不带 generationConfig：创建后再注入，否则档位意图
        # 会随剥后缀一起丢失（-search 对 tools 缺失同样创建）。
        gen = parsed["generationConfig"] = {}
    if not isinstance(gen, dict):
        return base, None
    img_cfg = gen.get("imageConfig")
    if img_cfg is None:
        img_cfg = gen["imageConfig"] = {}
    if not isinstance(img_cfg, dict) or img_cfg.get("imageSize"):
        # 非法结构 → 原样透传报真实错误；已有档位 → 尊重请求级显式设置。
        return base, None
    img_cfg["imageSize"] = tier
    body = json.dumps(parsed, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return base, body


def _resolve_upstream_model_path(model: str, key_project_id: str,
                                 settings: dict | None = None) -> str:
    """复用 Express 通道的 location 钉定规则。

    settings 可由调用方传入请求级快照：重试循环里逐次调用时免掉
    get_settings() 的全量深拷贝（retry_max 上限 50，最坏一次请求 51 次）。
    """
    if settings is None:
        settings = app_state.get_settings()
    return resolve_express_model_path(model, settings, key_project_id=key_project_id)


def _build_upstream_url(model_path: str, action: str) -> str:
    """把模型路径拼成上游 URL，规则对齐 google-genai SDK 的 t_model()：

    - 裸模型名 → publishers/google/models/{model}（Express 端点格式）；
    - 完整资源路径（projects/、publishers/、models/、tunedModels/、
      endpoints/ 开头——包括 location 钉定生成的
      projects/{p}/locations/{l}/publishers/google/models/{m} 与客户端
      自带的完整路径）→ 原样拼接，**不能再叠 publishers 前缀**。

    修复前的 bug：这里无条件拼 publishers/google/models/，location 钉定
    生效时（express_location 默认 global）会请求
    …/v1/publishers/google/models/projects/… 双重前缀的畸形 URL，
    Google 前端直接返回 404 空 body（客户端只能显示 Unknown error: 404）
    ——这正是「OpenAI 兼容接口正常、Gemini 原生透传一律 404」的根因
    （OpenAI 路径走 SDK，t_model() 对 projects/ 开头原样透传，URL 正确）。
    """
    if model_path.startswith(("projects/", "publishers/", "models/",
                              "tunedModels/", "endpoints/")):
        model_segment = model_path
    else:
        model_segment = f"publishers/google/models/{model_path}"
    return f"{EXPRESS_BASE.rstrip('/')}/{EXPRESS_API_VERSION}/{model_segment}:{action}"



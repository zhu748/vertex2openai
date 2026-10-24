"""控制台设置的类型、枚举、数值范围与代理格式校验。"""

import math

import config
from proxy_resolver import validate_proxy_uri
from proxy_config import ProxyFormatError, parse_proxy_list
from keepalive import validate_keepalive_url


_SETTING_ENUMS = {
    "anti_truncation_mode": ("off", "nonstream", "stream", "both"),
    "prefill_mode": ("smart", "keep_turn", "minimal", "off"),
    "native_thinking_mode": ("request", "off", "console"),
    "sampling_policy": ("auto", "deprecated", "allowed"),
    "thinking_g3_level": ("", "minimal", "low", "medium", "high"),
    "image_size": ("", "512", "1K", "2K", "4K"),
    "cookie_impersonate": ("off", "chrome", "edge", "safari", "firefox", "safari_ios"),
    "retry_backoff_mode": ("fixed", "exponential"),
}
# 数值键的允许范围（与读取侧的钳位逻辑一致，如 get_retry_settings 0-50/0-120）
_SETTING_NUMERIC_RANGE = {
    "retry_max": (0, 50),
    "first_reply_timeout_seconds": (0.0, 7200.0),
    "resumable_disconnect_grace_seconds": (0.0, 7200.0),
    "retry_backoff_seconds": (0.0, 120.0),
    "retry_jitter_seconds": (0.0, 60.0),
    "fake_streaming_interval": (0.0, 30.0),
    "img_compress_max_dim": (64, 8192),
    "img_compress_max_mb": (0.05, 32.0),
    "img_compress_quality": (10, 100),
    "resumable_ttl_seconds": (60, 604800),
    "resumable_max_streams": (1, 10000),
    "resumable_max_buffer_mb": (1, 1024),
    "resumable_max_total_mb": (1, 8192),
    "request_timeout_seconds": (30, 7200),
    "upstream_max_concurrency": (1, 1000),
    "keepalive_interval": (5, 86400),
}
# 已有专项校验的键（代理格式 / 指纹档案白名单），通用层跳过避免重复
_SETTING_SPECIAL_KEYS = {"proxy_url", "proxy_urls", "express_ua_profile", "express_ua_custom", "model_overrides"}


def _check_setting_value(key: str, v) -> str | None:
    """校验单个设置值；返回错误消息或 None（合法）。"""
    if isinstance(v, float) and not math.isfinite(v):
        return f"{key} 必须是有限数字。"
    enum = _SETTING_ENUMS.get(key)
    if enum is not None:
        if not (isinstance(v, str) and v in enum):
            return f"{key} 必须是 {'/'.join(enum) or '空串'} 之一，收到的是「{v!r}」"
        return None
    default = config.DEFAULT_SETTINGS.get(key)
    if isinstance(default, bool):
        if not isinstance(v, bool):
            return f"{key} 必须是布尔值（true/false），收到的是「{v!r}」"
        return None
    if isinstance(default, (int, float)):
        # bool 是 int 的子类，先排除（true 会被当成 1 混过数字检查）
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            return f"{key} 必须是数字（内置默认 {default}），收到的是「{v!r}」"
        if isinstance(default, int) and (not isinstance(v, int) and not v.is_integer()):
            return f"{key} 必须是整数。"
        lo, hi = _SETTING_NUMERIC_RANGE.get(key, (None, None))
        if lo is not None and not (lo <= v <= hi):
            return f"{key} 超出允许范围 [{lo}, {hi}]，收到的是 {v!r}"
        return None
    if isinstance(default, str):
        if not isinstance(v, str):
            return f"{key} 必须是字符串，收到的是「{v!r}」"
        return None
    if default is None:
        # default_temperature / default_top_p / default_max_tokens：None=不注入，或数字
        if v is None:
            return None
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            return f"{key} 必须是数字或 null（null=不注入默认值），收到的是「{v!r}」"
        if key == "default_max_tokens" and (not isinstance(v, int) or v <= 0):
            return f"{key} 必须是正整数或 null。"
        if key == "default_temperature" and not 0 <= v <= 2.0:
            # Gemini 上限 2.0：超界值保存时静默通过，之后每个请求注入默认值
            # 都会 400（不可重试），报错点离保存点十万八千里（2026-10 修复）。
            return f"{key} 必须在 [0, 2] 范围内，或为 null。"
        if key == "default_top_p" and not 0 <= v <= 1:
            return f"{key} 必须在 [0, 1] 范围内，或为 null。"
        return None
    return None


def validate_settings_patch(patch: dict, keys_scope=None) -> dict | None:
    """批量校验设置键值形态。返回 {field, error} 或 None（全部合法）。

    keys_scope 限定允许校验的键集合（如 set(PER_MODEL_KEYS)），
    None = 全部 DEFAULT_SETTINGS 键。未知键由 update_settings /
    set_model_override 静默忽略（保持旧行为），这里不报错。
    """
    for k, v in patch.items():
        if keys_scope is not None and k not in keys_scope:
            continue
        if k not in config.DEFAULT_SETTINGS or k in _SETTING_SPECIAL_KEYS:
            continue
        err = _check_setting_value(k, v)
        if err:
            return {"field": k, "error": err}
    return None


def validate_global_settings_patch(patch: dict) -> dict | None:
    """全局设置额外校验通道参数；抗截断只能在模型专属设置启用。"""
    if "anti_truncation_mode" in patch and patch["anti_truncation_mode"] != "off":
        return {"error": "抗截断后缀只能按模型开启，请使用模型专属设置。", "field": "anti_truncation_mode"}
    for field in ("proxy_url", "proxy_urls"):
        if field not in patch:
            continue
        if field == "proxy_url" and not isinstance(patch[field], str):
            return {"error": "proxy_url 必须是字符串。", "field": field}
        if field == "proxy_urls" and not isinstance(patch[field], list):
            return {"error": "proxy_urls 必须是数组。", "field": field}
        try:
            proxies = parse_proxy_list(patch[field])
        except ProxyFormatError as exc:
            return {"error": str(exc), "field": field}
        if len(proxies) > 32:
            return {"error": "每个代理列表最多 32 条。", "field": field}
        for index, proxy in enumerate(proxies):
            err = validate_proxy_uri(proxy)
            if err:
                return {"error": f"第 {index + 1} 条代理地址格式不合法：{err}", "field": field}
    if "express_ua_profile" in patch:
        from express_fingerprint import EXPRESS_UA_PROFILES

        v = patch["express_ua_profile"]
        if not isinstance(v, str) or v not in EXPRESS_UA_PROFILES:
            return {
                "error": f"express_ua_profile 必须是 {'/'.join(EXPRESS_UA_PROFILES)} 之一，收到的是「{v}」",
                "field": "express_ua_profile",
            }
    if "express_ua_custom" in patch and (not isinstance(patch["express_ua_custom"], str)):
        return {"error": "express_ua_custom 必须是字符串。", "field": "express_ua_custom"}
    if "express_ua_custom" in patch:
        custom_ua = patch["express_ua_custom"]
        if not custom_ua.isascii() or any((ord(c) < 32 or ord(c) == 127 for c in custom_ua)):
            return {
                "error": "express_ua_custom 必须为不含控制字符的 ASCII 请求头。",
                "field": "express_ua_custom",
            }
    if "keepalive_url" in patch:
        v = patch["keepalive_url"]
        if not isinstance(v, str):
            return {"error": "keepalive_url 必须是字符串。", "field": "keepalive_url"}
        v = v.strip()
        if not validate_keepalive_url(v):
            return {"error": "keepalive_url 必须是包含主机名的合法 HTTP(S) 地址（例如 https://你的域名/healthz），或留空关闭保活。",
                    "field": "keepalive_url"}
    return validate_settings_patch(patch)

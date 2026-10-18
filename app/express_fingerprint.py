"""Express HTTP client labels and compatibility overrides.

The default leaves the official SDK in charge. Optional node/gcloud/custom
labels are retained for integration compatibility, not verified emulation of
another client's TLS/HTTP2 stack or a way to avoid upstream restrictions.
MockTransport comparisons cover HTTP objects only.
"""

import sys
from typing import Any, Dict

import google.genai as _genai_pkg

from runtime_state import app_state

# 档案白名单（/api/settings 与本模块共用）
EXPRESS_UA_PROFILES = ("sdk", "node", "gcloud", "custom")

# Legacy optional compatibility label (does not change the actual runtime).
_NODE_LABEL = "gl-node/22.14.0"

# Legacy optional CLI labels. The default SDK profile does not use them.
_GCCL_LABEL = "gccl/2.24.0"
_GCLOUD_LABEL = "gcloud/520.0.0"
_GCLOUD_COMMAND = "command/gcloud.ai-platform"


def sdk_label() -> str:
    """当前运行时 google-genai 的精确标签：google-genai-sdk/<ver>。"""
    return f"google-genai-sdk/{getattr(_genai_pkg, '__version__', '') or '2.31.0'}"


def python_label() -> str:
    """当前解释器的精确标签：gl-python/<ver>（与 SDK 内部构造逻辑一字不差）。"""
    return "gl-python/" + sys.version.split()[0]


def node_label() -> str:
    return _NODE_LABEL


def get_express_ua_profile() -> str:
    """读取当前档案（热生效）。非法值回退到 SDK 默认行为。"""
    try:
        value = str(app_state.get_setting("express_ua_profile", "sdk") or "sdk").strip().lower()
    except Exception:
        value = "sdk"
    return value if value in EXPRESS_UA_PROFILES else "sdk"


def get_express_ua_custom() -> str:
    try:
        return str(app_state.get_setting("express_ua_custom", "") or "").strip()
    except Exception:
        return ""


def _ensure_sdk_labels(ua: str) -> str:
    """把精确标签补齐（已存在则原样返回）——变换与真实 SDK 逐字符一致。

    对齐 google-genai 2.31.0 append_library_version_headers 的真实逻辑：
      library 标签（google-genai-sdk/<ver>）缺失 → 把 sdk+python 两个标签
        **整体前置**（SDK 的 version_header_value 就是这个二元组），这是
        真实用户经 HttpOptions.headers 传自定义 UA 时 SDK 产出的原样形态；
      library 在场而 gl-python 缺失 → 尾部追加 gl-python。
    这样无论用户怎么填，最终串都必然是「真实 SDK 对同样输入的输出」，
    而不会出现 SDK 自己造不出来的标签排布。标签在场后 SDK 原样放行，
    也就不会被再追加一份。
    """
    if sdk_label() not in ua:
        return f"{sdk_label()} {python_label()} {ua}".strip()
    if python_label() not in ua:
        return f"{ua} {python_label()}".strip()
    return ua


def _build_ua(profile: str) -> str:
    """按档案构造 UA 串（x-goog-api-client 与它同值，官方 SDK 本来就同值）。"""
    if profile == "node":
        # 仅供裸 REST 出站点的兼容标签；SDK 主通道不采用此覆盖。
        return f"{sdk_label()} {node_label()}"
    if profile == "gcloud":
        # gcloud 也是 Python 程序，gl-python 标签真实存在；gcloud/<ver> +
        # command/<子命令> + gccl/<ver> 是 GCP 官方 CLI 的合法标签组合。
        return f"{sdk_label()} {python_label()} {_GCLOUD_LABEL} {_GCLOUD_COMMAND} {_GCCL_LABEL}"
    if profile == "custom":
        raw = get_express_ua_custom()
        if not raw:
            return f"{sdk_label()} {python_label()}"   # 空自定义 = 退回官方形态
        ua = raw
        for token, real in (("{sdk}", sdk_label()), ("{python}", python_label()),
                            ("{node}", node_label())):
            ua = ua.replace(token, real)
        return _ensure_sdk_labels(ua)
    # sdk 档案：官方 Python genai SDK 的原生形态
    return f"{sdk_label()} {python_label()}"


def build_sdk_channel_overrides() -> Dict[str, str]:
    """Leave SDK identity alone by default; custom labels preserve its version tags."""
    profile = get_express_ua_profile()
    if profile in ("sdk", "node"):
        return {}
    ua = _build_ua(profile)
    return {"user-agent": ua, "x-goog-api-client": ua}


def build_direct_headers(api_key: str = "") -> Dict[str, str]:
    """Build explicit HTTP labels for direct REST requests (not a TLS fingerprint)."""
    ua = _build_ua(get_express_ua_profile())
    headers: Dict[str, str] = {"Content-Type": "application/json"}
    if api_key:
        headers["x-goog-api-key"] = api_key
    headers["user-agent"] = ua
    headers["x-goog-api-client"] = ua
    return headers


def describe_current_profile() -> str:
    """启动横幅 / 排障日志用的一句话描述。"""
    profile = get_express_ua_profile()
    if profile == "sdk":
        return "官方 Python SDK 原生形态（SDK 自动管理）"
    if profile == "node":
        return ("Node 兼容标签（仅裸出站点：透传/探测/模型配置）；"
                "SDK 主通道保持纯 python 形态（避免 gl-node+gl-python 混合标签）")
    if profile == "custom" and not get_express_ua_custom():
        return "自定义（当前为空 → 官方形态）"
    return f"{profile} 兼容标签：{_build_ua(profile)}"

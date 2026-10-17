"""Express API Key（官方 SDK）通道的请求头指纹模块。

与 impersonate.py（Cookie 通道浏览器 TLS 伪装）互补，分工：
- impersonate.py      → Cookie 直连通道的 **TLS 指纹 + 浏览器请求头**（curl_cffi）；
- 本模块             → Express API Key 通道的 **请求头指纹**（User-Agent /
                        x-goog-api-client 等）。
  SDK 通道的 TLS 栈无法更换（google-genai 强依赖 httpx，且 Client 不接受
  自定义 transport），但请求头可以完整接管——这已覆盖风控看的大头：
  Google 后端日志里每条请求的 client 标签就是这两个头。

SDK 强制标签机制（google-genai 2.31.0 _api_client.append_library_version_headers，
本地 mock 上游实测）：注入的 user-agent / x-goog-api-client 必须包含**当前
精确的** `google-genai-sdk/<ver>` 与 `gl-python/<pyver>` 两个标签，SDK 才原样
放行；缺任何一个都会被 SDK 前置/追加（实测自定义 UA 会出现「双重标签」，
比不改还扎眼）。因此本模块运行时读取 SDK 与 Python 的真实版本号来构造
精确标签，让 SDK 的三个分支全部落空。

档案（express_ua_profile，控制台「通道与凭证」页热改）：
  sdk    = 不注入，SDK 自动管理 = 官方 Python genai SDK 形态（默认，零改动）
  node   = 官方 Node.js SDK 形态（gl-node/22 LTS）。JS 生态调用量最大、
           风控最难一刀切。注意：SDK 主通道会被强制追加 gl-python 标签
           （混合形态）；Gemini 原生透传等裸 httpx 出站点则是纯 node 形态
  gcloud = gcloud CLI 形态（追加 gcloud/command/gccl 标签 = GCP 官方运维
           流量的合法组合）
  custom = 自定义串（express_ua_custom，支持 {sdk} {python} {node} 占位符）。
           占位符或串内必须让两个精确标签齐活，缺了会自动补——宁可补
           也不要让 SDK 追加出双重标签

裸 httpx 出站点（Gemini 原生透传 / Key 探测 / 模型配置拉取 / 代理连通测试）
没有 SDK 自动注入，一律用 build_direct_headers()：sdk 档案时也显式带上
官方 SDK 同款头（这些点缺 x-goog-api-client 反而是异常形态）。
"""

import sys
from typing import Any, Dict

import google.genai as _genai_pkg

from runtime_state import app_state

# 档案白名单（/api/settings 与本模块共用）
EXPRESS_UA_PROFILES = ("sdk", "node", "gcloud", "custom")

# Node 22 LTS（Jodie）—— gl-node 生态当前最常见的运行时版本
_NODE_LABEL = "gl-node/22.14.0"

# gcloud CLI 近期版本（gccl = google-cloud-core；command 标签是 gcloud 子命令）
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
    """读取当前档案（热生效）。非法值一律回退 sdk（= 官方原始行为，最安全）。"""
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
    """把两个精确标签补齐（已存在则原样返回）。

    这是骗过 SDK append_library_version_headers 的关键：标签只要「在场」
    就放行，因此宁可自己补，也不让 SDK 再追加一份。
    """
    if sdk_label() not in ua:
        ua = f"{sdk_label()} {ua}".strip()
    if python_label() not in ua:
        ua = f"{ua} {python_label()}".strip()
    return ua


def _build_ua(profile: str) -> str:
    """按档案构造 UA 串（x-goog-api-client 与它同值，官方 SDK 本来就同值）。"""
    if profile == "node":
        # 裸 httpx 出站点发出去的是纯 Node SDK 形态；SDK 主通道会被 SDK
        # 自动追加 gl-python（我们拦不住），但标签齐全总比缺标签好。
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
    """给 http_options.get_http_options() 的 HttpOptions.headers 用（SDK 主通道）。

    sdk 档案返回 {}：不注入任何头，SDK 自动管理，行为与历史版本逐字节一致。
    其它档案返回 user-agent / x-goog-api-client（两标签齐全，SDK 原样放行）。
    """
    profile = get_express_ua_profile()
    if profile == "sdk":
        return {}
    ua = _build_ua(profile)
    return {"user-agent": ua, "x-goog-api-client": ua}


def build_direct_headers(api_key: str = "") -> Dict[str, str]:
    """给所有裸 httpx 出站点用（Gemini 透传 / Key 探测 / 模型配置 / 代理测试）。

    返回与官方 SDK **同款、同线序**的头集合（MockTransport 抓包对照验证，
    与真实 google-genai 请求逐字节一致）：
      Content-Type: application/json   —— SDK 对所有请求（含 GET models.list）
                                          无条件下发，缺了反而是异常形态；
      x-goog-api-key                   —— 仅在传入 key 时携带（api_key 模式）；
      user-agent / x-goog-api-client   —— 跟随控制台 express_ua_profile 档案。

    线序原理：httpx 的 _merge_headers 先铺默认头 [accept, accept-encoding,
    connection, user-agent]，再按本 dict 的顺序 in-place 覆盖 / 追加——
    本函数的构造顺序 (Content-Type → x-goog-api-key → user-agent →
    x-goog-api-client) 恰好落成 SDK 的最终线序：
      host, accept, accept-encoding(gzip, deflate), connection,
      Content-Type, x-goog-api-key, user-agent, x-goog-api-client, content-length
    绝不能出现 python-httpx 或任何含 "vertex2openai" 的 UA——直接管自己
    叫代理等于在 Google 风控日志里自首。

    注：调用方发 JSON 时应传 content=json.dumps(body)（默认分隔符带空格，
    与 SDK 的序列化一致），而不要用 httpx 的 json= 参数——后者用的是紧凑
    分隔符（无空格），真实 Python SDK 从不这么发。
    """
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
    if profile == "custom" and not get_express_ua_custom():
        return "自定义（当前为空 → 官方形态）"
    return f"{profile} 档案：{_build_ua(profile)}"

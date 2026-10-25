"""Pure proxy URI classification, validation, redaction and sing-box translation.

This module performs no network IO and never starts a proxy process. Runtime
resolution and process ownership belong to proxy_resolver.
"""

import base64
import hashlib
import json
from typing import Any, Optional
from urllib.parse import parse_qs, unquote, urlsplit, urlunsplit

from proxy_config import (
    BRIDGE_PROXY_SCHEMES,
    DIRECT_TOKENS,
    ProxyFormatError,
    normalize_proxy_uri,
    parse_bridge_proxy_uri,
)


class ProxyResolveError(Exception):
    """代理解析/转换失败。消息面向最终用户，须给出可操作的指引。"""

    code = 502

    def __init__(self, message, *, retry_new_port=True):
        super().__init__(message)
        self.retry_new_port = retry_new_port


# ============================================================
# 协议分类
# ============================================================

# httpx[socks] / curl_cffi 原生支持：原样透传，不经过 sing-box
NATIVE_PROXY_SCHEMES = {"http", "https", "socks5", "socks5h"}

# 需要 sing-box 转换的协议（scheme 小写）
SINGBOX_PROXY_SCHEMES = {
    "socks4",
    "socks4a",
    "vless",
    "vmess",
    "trojan",
    "ss",
    "hysteria2",
    "hy2",
    "tuic",
}

# 「强制直连」哨兵值（Key 级代理里最常用，全局代理里等价于留空）
ALL_PROXY_SCHEMES = NATIVE_PROXY_SCHEMES | SINGBOX_PROXY_SCHEMES | BRIDGE_PROXY_SCHEMES


def _uri_scheme(uri: str) -> str:
    try:
        return (urlsplit(uri).scheme or "").strip().lower()
    except Exception:
        return ""


def classify_proxy_uri(uri: str) -> str:
    """分类代理 URI：native / singbox / bridge / direct / invalid。"""
    try:
        value = normalize_proxy_uri(str(uri or ""))
    except ProxyFormatError:
        return "invalid"
    if not value:
        return "direct"
    if value.lower() in DIRECT_TOKENS:
        return "direct"
    scheme = _uri_scheme(value)
    if scheme in NATIVE_PROXY_SCHEMES:
        return "native"
    if scheme in SINGBOX_PROXY_SCHEMES:
        return "singbox"
    if scheme in BRIDGE_PROXY_SCHEMES:
        return "bridge"
    return "invalid"


# ============================================================
# 基础工具
# ============================================================


def _b64decode_text(s: str) -> str:
    """宽容的 base64 解码：去空白（链接常被折行复制）+ urlsafe 归一 + 补 padding + utf-8。"""
    s = "".join(str(s or "").split()).replace("-", "+").replace("_", "/")
    s += "=" * (-len(s) % 4)
    return base64.b64decode(s).decode("utf-8", errors="strict")


def _to_port(text: Any, uri: str) -> int:
    """端口文本 → int；非数字/越界统一报 ProxyResolveError（否则会以裸
    ValueError 冒出去，错误信息晦涩且消费方难归类）。"""
    try:
        port = int(str(text).strip())
    except (TypeError, ValueError):
        raise ProxyResolveError("代理节点端口不是数字。") from None
    if not (1 <= port <= 65535):
        raise ProxyResolveError("代理节点端口超出 1–65535。")
    return port


def _parse_host_port(hostport: str, uri: str) -> tuple[str, int]:
    """'host:port'（兼容 IPv6 [::1]:443 与 SIP002 尾斜杠）→ (host, port)。"""
    hostport = (hostport or "").strip().rstrip("/")
    if not hostport:
        raise ProxyResolveError("代理节点缺少主机与端口。")
    if hostport.startswith("["):  # IPv6 字面量
        end = hostport.find("]")
        if end < 0:
            raise ProxyResolveError("代理节点 IPv6 地址格式不完整。")
        host = hostport[1:end]
        rest = hostport[end + 1 :]
        if not rest.startswith(":"):
            raise ProxyResolveError("代理节点缺少端口。")
        port = _to_port(rest[1:], uri)
    else:
        if ":" not in hostport:
            raise ProxyResolveError("代理节点缺少端口（正确形态 host:port）。")
        host, _, port_s = hostport.rpartition(":")
        port = _to_port(port_s, uri)
    if not host:
        raise ProxyResolveError("代理节点缺少服务器地址。")
    return host, port


def _qs(query: str) -> dict[str, str]:
    """查询串 → 单值 dict（重复键取首个；同名不同值时告警）。"""
    try:
        parsed = parse_qs(query or "", keep_blank_values=True)
    except Exception:
        return {}
    for key, values in parsed.items():
        # 同名不同值的重复参数（如 path=/a&path=/b）静默取首个会掩盖节点真实
        # 配置（与 bridge URI 的严格重复检测不对齐）；值不打到日志防泄露。
        if len(values) > 1 and len(set(values)) > 1:
            print(f"⚠️ [代理] 链接参数 {key!r} 重复出现且取值不同，已采用首个值。")
    return {k: v[0] for k, v in parsed.items()}


def _truthy(v: str) -> bool:
    return str(v or "").strip().lower() in ("1", "true", "yes")


# ============================================================
# TLS / transport 公共构造（vless / vmess / trojan 共用）
# ============================================================


def _build_tls(
    q: dict,
    *,
    default_sni: str = "",
    force: bool = False,
    allow_insecure_param: bool = True,
) -> Optional[dict]:
    """从查询参数构造 sing-box tls 对象。

    security: tls / reality（vless 用）；trojan/vmess 由调用方决定 force。
    utls 指纹（fp=chrome 等）一并挂上——分享链接几乎都带，sing-box 全支持。
    """
    security = str(q.get("security") or "").strip().lower()
    if security not in ("", "none", "tls", "reality"):
        raise ProxyResolveError("不支持该 TLS security 类型，不能自动改为未加密连接。")
    enabled = force or security in ("tls", "reality")
    if not enabled:
        return None
    tls: dict[str, Any] = {"enabled": True}
    sni = str(q.get("sni") or q.get("peer") or "").strip() or default_sni
    if sni:
        tls["server_name"] = sni
    alpn = str(q.get("alpn") or "").strip()
    if alpn:
        tls["alpn"] = [a for a in alpn.split(",") if a.strip()]
    fp = str(q.get("fp") or "").strip()
    if fp:
        tls["utls"] = {"enabled": True, "fingerprint": fp}
    if allow_insecure_param and (
        _truthy(q.get("allowInsecure")) or _truthy(q.get("insecure"))
    ):
        tls["insecure"] = True
    if security == "reality":
        # sing-box 要求 REALITY 必须带 uTLS 指纹；分享链接偶有漏带 fp 的，
        # 缺省会直接拒启（"missing uTLS fingerprint"），这里补最常见的 chrome。
        if not fp:
            tls["utls"] = {"enabled": True, "fingerprint": "chrome"}
        reality: dict[str, Any] = {
            "enabled": True,
            "public_key": str(q.get("pbk") or ""),
        }
        sid = str(q.get("sid") or "").strip()
        if sid:
            reality["short_id"] = sid
        tls["reality"] = reality
    return tls


def _build_transport(net: str, q: dict) -> Optional[dict]:
    """从查询参数构造 sing-box transport 对象，未知类型明确拒绝。

    vless/trojan 用 type= 参数；vmess 用 net= 字段（值域相同，调用方归一后传入）。
    tcp / 无值 → 返回 None（不设 transport）。
    """
    net = (net or "tcp").strip().lower()
    if net in ("", "tcp", "raw", "none"):
        if str(q.get("headerType") or "none").strip().lower() != "none":
            raise ProxyResolveError(
                "当前转换不支持 TCP 伪装头，不能忽略伪装要求改用普通 TCP。"
            )
        return None
    if net == "ws":
        tr: dict[str, Any] = {"type": "ws"}
        # 注意：_qs/parse_qs 已做过一次百分号解码，这里**不再** unquote——
        # 否则 path=%252F 这类字面量 "(%2F)" 会被二次解码成 "/"，改掉真实路径。
        path = str(q.get("path") or "").strip()
        if path:
            tr["path"] = path
        host = str(q.get("host") or "").strip()
        if host:
            tr["headers"] = {"Host": host}
        ed = str(q.get("ed") or "").strip()  # WebSocket 0-RTT（SFM/V2Ray 分支）
        if ed.isdigit():
            tr["max_early_data"] = int(ed)
            tr["early_data_header_name"] = "Sec-WebSocket-Protocol"
        return tr
    if net == "grpc":
        tr = {"type": "grpc"}
        service = str(q.get("serviceName") or q.get("servicename") or "").strip()
        if service:
            tr["service_name"] = service
        return tr
    if net in ("http", "h2"):
        tr = {"type": "http"}
        host = str(q.get("host") or "").strip()
        if host:
            tr["host"] = [h for h in host.split(",") if h.strip()]
        path = str(q.get("path") or "").strip()
        if path:
            tr["path"] = path  # 同上：parse_qs 已解码，不二次 unquote
        return tr
    if net == "httpupgrade":
        tr = {"type": "httpupgrade"}
        for field in ("host", "path"):
            if q.get(field):
                tr[field] = str(q[field])
        return tr
    if net == "quic":
        if (
            str(q.get("quicSecurity") or "none").lower() != "none"
            or q.get("key")
            or str(q.get("headerType") or "none").lower() != "none"
        ):
            raise ProxyResolveError(
                "QUIC 传输不支持额外加密、密钥或伪装头，不能转换为普通 QUIC。"
            )
        return {"type": "quic"}
    raise ProxyResolveError(
        "不支持该传输类型；可用 tcp/ws/grpc/http/h2/httpupgrade/quic，不能自动改为 TCP。"
    )


# ============================================================
# 各协议 URI → sing-box outbound
# ============================================================


def _require_port(parts, scheme: str, *, default: int | None = None) -> int:
    """从 urlsplit 结果取端口；缺失/非法/越界直接报错（sing-box 配置不允许）。"""
    try:
        port = parts.port
    except ValueError as e:
        raise ProxyResolveError(f"{scheme} 链接端口不合法：{e}")
    if port is None:
        if default is not None:
            return default
        raise ProxyResolveError(
            f"{scheme} 链接缺少端口（正确形态 {scheme}://…@host:port）。"
        )
    if port < 1:
        raise ProxyResolveError(
            f"{scheme} 链接端口必须在 1–65535 范围内（收到 {port}）。"
        )
    return port


def _parse_vless(uri: str) -> dict:
    """vless://uuid@host:port?encryption=none&security=tls&sni=..&type=ws&path=..&fp=..#name"""
    parts = urlsplit(uri)
    uuid = unquote(parts.username or "")
    if not uuid:
        raise ProxyResolveError(
            "vless 链接缺少 UUID（正确形态 vless://uuid@host:port?…）。"
        )
    server = parts.hostname or ""
    if not server:
        raise ProxyResolveError("vless 链接缺少服务器地址。")
    port = _require_port(parts, "vless")
    ob: dict[str, Any] = {
        "type": "vless",
        "server": server,
        "server_port": port,
        "uuid": uuid,
    }
    q = _qs(parts.query)
    if str(q.get("encryption") or "none").strip().lower() != "none":
        raise ProxyResolveError(
            "当前 VLESS 转换仅支持 encryption=none，不能忽略额外加密要求。"
        )
    flow = str(q.get("flow") or "").strip()
    tls = _build_tls(q)
    if flow:
        if tls is None:
            raise ProxyResolveError(
                "VLESS flow 需要 TLS 或 REALITY，不能忽略 flow 改用普通 VLESS。"
            )
        ob["flow"] = flow
    if tls:
        ob["tls"] = tls
    tr = _build_transport(str(q.get("type") or ""), q)
    if tr:
        ob["transport"] = tr
    return ob


def _parse_vmess(uri: str) -> dict:
    """vmess://base64(json)。json 字段：add/port/id/aid/scy/net/tls/sni/host/path/type。"""
    payload = uri[len("vmess://") :]
    try:
        data = json.loads(_b64decode_text(payload))
    except Exception as e:
        raise ProxyResolveError(f"vmess 链接的 base64/JSON 无法解码：{e}")
    if not isinstance(data, dict):
        raise ProxyResolveError("vmess 链接解码后不是 JSON 对象。")
    server = str(data.get("add") or "").strip()
    if not server:
        raise ProxyResolveError("vmess 链接缺少服务器地址（add 字段）。")
    try:
        port = int(data.get("port"))
    except (TypeError, ValueError):
        raise ProxyResolveError("vmess 链接缺少有效端口（port 字段）。")
    if not (1 <= port <= 65535):
        raise ProxyResolveError(f"vmess 链接端口必须在 1–65535 范围内（收到 {port}）。")
    uuid = str(data.get("id") or "").strip()
    if not uuid:
        raise ProxyResolveError("vmess 链接缺少用户 ID（id 字段）。")
    ob: dict[str, Any] = {
        "type": "vmess",
        "server": server,
        "server_port": port,
        "uuid": uuid,
        "security": str(data.get("scy") or "auto"),
    }
    try:
        ob["alter_id"] = int(data.get("aid") or 0)
    except (TypeError, ValueError):
        pass
    tls_mode = str(data.get("tls", "")).strip().lower()
    if tls_mode not in ("", "none", "0", "false", "tls", "1", "true"):
        raise ProxyResolveError(
            "vmess 链接使用了不支持的 TLS 类型，不能自动改为未加密连接。"
        )
    if tls_mode in ("tls", "1", "true"):
        q = {
            "sni": data.get("sni"),
            "alpn": data.get("alpn"),
            "fp": data.get("fp"),
            "allowInsecure": data.get("allowInsecure"),
        }
        tls = _build_tls(
            {k: v for k, v in q.items() if v}, force=True, default_sni=server
        )
        if tls:
            ob["tls"] = tls
    tr = _build_transport(
        str(data.get("net") or ""),
        {
            "path": data.get("path"),
            "host": data.get("host"),
            "headerType": data.get("type"),
            "serviceName": data.get("path")
            if str(data.get("net") or "").lower() == "grpc"
            else "",
            **(
                {
                    "quicSecurity": data.get("host"),
                    "key": data.get("path"),
                    "headerType": data.get("type"),
                }
                if str(data.get("net") or "").lower() == "quic"
                else {}
            ),
        },
    )
    if tr:
        ob["transport"] = tr
    return ob


def _parse_trojan(uri: str) -> dict:
    """trojan://password@host:port?sni=..&type=ws&path=..&fp=..#name（TLS 强制开启）"""
    parts = urlsplit(uri)
    password = unquote(parts.username or "")
    if parts.password is not None:
        # 密码含未编码冒号：urlsplit 把 userinfo 切成 username:password 两段，
        # 旧实现静默丢弃后者导致连错密码（握手失败无提示指向截断）。Trojan 的
        # userinfo 整体即密码，拼回完整值；规范写法应把冒号编码为 %3A。
        password = unquote(parts.username or "") + ":" + unquote(parts.password)
        print(
            "⚠️ [代理] trojan 密码含未编码的冒号，已按原样拼接；建议改为 %3A 编码以避免歧义。"
        )
    if not password:
        raise ProxyResolveError("trojan 链接缺少密码（userinfo 部分）。")
    server = parts.hostname or ""
    if not server:
        raise ProxyResolveError("trojan 链接缺少服务器地址。")
    port = _require_port(parts, "trojan")
    ob: dict[str, Any] = {
        "type": "trojan",
        "server": server,
        "server_port": port,
        "password": password,
    }
    q = _qs(parts.query)
    tls = _build_tls(q, force=True, default_sni=server)
    if tls:
        ob["tls"] = tls
    tr = _build_transport(str(q.get("type") or ""), q)
    if tr:
        ob["transport"] = tr
    return ob


def _parse_ss(uri: str) -> dict:
    """ss://（三种形态：SIP002 base64 userinfo / SIP002 明文 / 旧式整体 base64）。"""
    body = uri[len("ss://") :]
    if "#" in body:
        body = body.split("#", 1)[0]
    if "?" in body:
        body, _, query = body.partition("?")
        if _qs(query).get("plugin"):
            # SIP003 plugin（v2ray-plugin/obfs 等）需要额外外部程序配合，
            # 静默忽略会产出一个永远连不上的代理、错误还难排查——明确拒绝。
            raise ProxyResolveError(
                "ss 链接启用了 SIP003 plugin（如 v2ray-plugin/obfs），本项目暂不支持："
                "请换不带 plugin 的节点，或改用 vless/trojan/vmess 等协议的分享链接。"
            )
    userinfo: str
    percent_encoded = False
    if "@" in body:
        userinfo_raw, hostport = body.rsplit("@", 1)
        userinfo = userinfo_raw
        if ":" not in userinfo:  # base64(method:password)
            try:
                userinfo = _b64decode_text(userinfo_raw)
            except Exception:
                raise ProxyResolveError(
                    "ss 链接的 userinfo 无法解码（应为 base64(method:password)）。"
                )
        else:
            percent_encoded = True
    else:  # 旧式：整体 base64(method:pass@host:port)
        try:
            decoded = _b64decode_text(body)
        except Exception as e:
            raise ProxyResolveError(f"ss 链接无法解码：{e}")
        if "@" not in decoded:
            raise ProxyResolveError(
                "ss 链接格式不认识（既非 SIP002 也非旧式 base64）。"
            )
        userinfo, _, hostport = decoded.rpartition("@")
    method, _, password = userinfo.partition(":")
    if percent_encoded:
        method, password = unquote(method), unquote(password)
    if not method or not password:
        raise ProxyResolveError("ss 链接缺少 method:password（userinfo 解析失败）。")
    host, port = _parse_host_port(hostport, uri)
    return {
        "type": "shadowsocks",
        "server": host,
        "server_port": port,
        "method": method,
        "password": password,
    }


def _parse_socks4(uri: str, version: str = "4") -> dict:
    """socks4://[user@]host:port → sing-box socks outbound（version 4 / 4a）。"""
    parts = urlsplit(uri)
    server = parts.hostname or ""
    if not server:
        raise ProxyResolveError("socks4 链接缺少服务器地址。")
    port = _require_port(parts, "socks4")
    ob: dict[str, Any] = {
        "type": "socks",
        "server": server,
        "server_port": port,
        "version": version,
    }
    user = unquote(parts.username or "")
    if user:
        ob["username"] = user
    return ob


def _parse_hysteria2(uri: str) -> dict:
    """hysteria2://auth@host:port?sni=..&insecure=1&obfs=salamander&obfs-password=..#name"""
    parts = urlsplit(uri)
    auth = unquote(parts.username or "")
    if parts.password is not None:
        auth += ":" + unquote(parts.password)
    if not auth:
        raise ProxyResolveError("hysteria2 链接缺少认证密码（userinfo 部分）。")
    server = parts.hostname or ""
    if not server:
        raise ProxyResolveError("hysteria2 链接缺少服务器地址。")
    port = _require_port(parts, "hysteria2", default=443)
    ob: dict[str, Any] = {
        "type": "hysteria2",
        "server": server,
        "server_port": port,
        "password": auth,
    }
    q = _qs(parts.query)
    if q.get("mport"):
        # 端口跳跃（mport=443,8443 或 443-8443）无法等价转换为单端口出站；
        # 旧实现静默忽略会只连基础端口、高峰期实际不可用，与 docs 声明的
        # 「多端口列表会被拒绝」直接矛盾——按 fail-closed 原则明确拒绝。
        raise ProxyResolveError(
            "当前 Hysteria2 转换不支持端口跳跃参数 mport（多端口/端口范围无法"
            "等价转换为单端口出站）：请使用不带 mport 的节点，或改用原生客户端。"
        )
    if q.get("pinSHA256") or q.get("ech"):
        raise ProxyResolveError(
            "当前 Hysteria2 转换不支持 pinSHA256 或 ECH，不能忽略这些 TLS 校验参数。"
        )
    if q.get("up") or q.get("down"):
        # 带宽参数只影响速度控制不影响连通性，丢弃可接受，但不再静默。
        print(
            "ℹ️ [代理] hysteria2 的 up/down 带宽参数已忽略（不影响连通性，仅放弃限速）。"
        )
    tls: dict[str, Any] = {"enabled": True, "alpn": ["h3"]}
    sni = str(q.get("sni") or "").strip()
    if sni:
        tls["server_name"] = sni
    if _truthy(q.get("insecure")):
        tls["insecure"] = True
    ob["tls"] = tls
    obfs = str(q.get("obfs") or "").strip().lower()
    if obfs == "salamander":
        ob["obfs"] = {
            "type": "salamander",
            "password": str(q.get("obfs-password") or ""),
        }
    elif obfs:
        raise ProxyResolveError(
            f"hysteria2 不支持的混淆类型：{obfs}（仅 salamander）。"
        )
    return ob


def _parse_tuic(uri: str) -> dict:
    """tuic://uuid:password@host:port?congestion_control=bbr&alpn=h3&sni=..#name"""
    parts = urlsplit(uri)
    user = unquote(parts.username or "")
    password = unquote(parts.password or "")
    if not user:
        raise ProxyResolveError("tuic 链接缺少 UUID（userinfo 部分）。")
    server = parts.hostname or ""
    if not server:
        raise ProxyResolveError("tuic 链接缺少服务器地址。")
    port = _require_port(parts, "tuic")
    q = _qs(parts.query)
    ob: dict[str, Any] = {
        "type": "tuic",
        "server": server,
        "server_port": port,
        "uuid": user,
        "password": password,
        "congestion_control": str(q.get("congestion_control") or "bbr"),
    }
    tls: dict[str, Any] = {"enabled": True}
    sni = str(q.get("sni") or "").strip()
    if sni:
        tls["server_name"] = sni
    alpn = str(q.get("alpn") or "h3").strip()
    if alpn:
        tls["alpn"] = [a for a in alpn.split(",") if a.strip()]
    if _truthy(q.get("allow_insecure")) or _truthy(q.get("insecure")):
        tls["insecure"] = True
    ob["tls"] = tls
    return ob


def parse_share_uri_to_outbound(uri: str) -> dict:
    """代理分享 URI → sing-box outbound JSON（dict）。只接受 sing-box 系协议。"""
    scheme = _uri_scheme(uri)
    if scheme == "vless":
        return _parse_vless(uri)
    if scheme == "vmess":
        return _parse_vmess(uri)
    if scheme == "trojan":
        return _parse_trojan(uri)
    if scheme == "ss":
        return _parse_ss(uri)
    if scheme == "socks4":
        return _parse_socks4(uri, "4")
    if scheme == "socks4a":
        return _parse_socks4(uri, "4a")
    if scheme in ("hysteria2", "hy2"):
        return _parse_hysteria2(uri)
    if scheme == "tuic":
        return _parse_tuic(uri)
    raise ProxyResolveError(
        f"协议「{scheme or '（空）'}」不是 sing-box 转换协议"
        f"（支持：{'/'.join(sorted(SINGBOX_PROXY_SCHEMES))}）。"
    )


def validate_proxy_uri(uri: str) -> str:
    """静态校验代理 URI，返回错误消息（空串 = 合法）。不启动转换进程、不发包。

    合法性 = 能解析出该协议的必要字段；连通性请走 /api/proxy/test。
    """
    try:
        value = normalize_proxy_uri(uri)
    except ProxyFormatError as exc:
        return str(exc)
    if not value or value.lower() in DIRECT_TOKENS:
        return ""
    kind = classify_proxy_uri(value)
    if kind == "native":
        try:
            parts = urlsplit(value)
            if not parts.hostname:
                return "缺少主机地址，正确格式：协议://用户名:密码@主机:端口"
            if parts.port is not None and parts.port < 1:
                return "代理端口必须在 1–65535 范围内。"
            return ""
        except Exception:
            return "无法解析的代理地址，正确格式：协议://用户名:密码@主机:端口"
    if kind == "singbox":
        try:
            parse_share_uri_to_outbound(value)
            return ""
        except ProxyResolveError as e:
            return str(e)
        except Exception as e:
            return f"解析失败：{type(e).__name__}: {e}"
    if kind == "bridge":
        try:
            parse_bridge_proxy_uri(value)
            return ""
        except ProxyFormatError as exc:
            return str(exc)
    scheme = _uri_scheme(value)
    return (
        f"协议必须是 {'/'.join(sorted(ALL_PROXY_SCHEMES))} 之一"
        f"（或 direct = 强制直连），收到的是「{scheme or '（无协议）'}」"
    )


def mask_proxy_uri(uri: str) -> str:
    """代理 URI 脱敏：保留协议与 host:port，凭据（密码/UUID/base64）整体打码。

    同时覆盖原生 URL 与 sing-box 分享链接两种形态；解析失败时退回哈希指纹，
    保证日志/healthz 里绝不泄露凭据。
    """
    value = str(uri or "").strip()
    try:
        value = normalize_proxy_uri(value)
    except ProxyFormatError:
        digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:8]
        return f"proxy:{digest}(格式无法解析)"
    if not value:
        return ""
    if value.lower() in DIRECT_TOKENS:
        return "direct（强制直连）"
    scheme = _uri_scheme(value)
    try:
        if scheme in BRIDGE_PROXY_SCHEMES:
            config = parse_bridge_proxy_uri(value)
            host = config["server"]
            if ":" in host:
                host = f"[{host}]"
            # 匿名分配 TURN 节点可以没有凭据，此时不显示 ***@ 占位
            if config["username"] or config["password"]:
                return f"{scheme}://***@{host}:{config['server_port']}"
            return f"{scheme}://{host}:{config['server_port']}"
        if scheme == "vmess":
            # vmess 整体是 base64，没有可提取的 host:port，只报长度
            return f"vmess://***({len(value)} 字符)"
        parts = urlsplit(value)
        if scheme == "ss" and "@" not in parts.netloc:
            # Legacy SS embeds credentials and the endpoint in one base64
            # authority. urlsplit can mistake that payload for a hostname.
            return f"ss://***({len(value)} 字符)"
        host = parts.hostname or ""
        if not host:
            raise ValueError
        port = f":{parts.port}" if parts.port else ""
        if ":" in host:
            host = f"[{host}]"
        cred = parts.username or parts.password
        masked_netloc = f"***@{host}{port}" if cred else f"{host}{port}"
        # ss 旧式整体 base64 无 @：host 解析不出来，落到 except
        return urlunsplit((scheme or "proxy", masked_netloc, "", "", ""))
    except Exception:
        digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:8]
        return f"proxy:{digest}(已配置，格式无法解析)"

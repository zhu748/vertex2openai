"""出站代理解析与协议转换（多协议代理支持层）。

背景：项目所有上游请求都走 httpx / curl_cffi，它们原生只认
  http:// https:// socks5:// socks5h://
四种代理 URL。而现实中大量用户手里是 vless:// vmess:// trojan:// ss://
socks4:// hysteria2:// tuic:// 等分享链接——这些协议 httpx 无法直接消费。

本模块的职责（三层）：
  1. 解析层：把各种代理分享 URI 解析成 sing-box 的 outbound JSON 片段；
  2. 运行层：SingboxManager 为每个「sing-box 系协议」的 URI 惰性启动一个
     sing-box 子进程（本地 127.0.0.1:{动态端口} 的 socks5 入站 → 远端 outbound），
     Python 侧统一连本地 socks5，对 httpx/curl_cffi 完全透明；
  3. 统一入口 resolve_proxy_uri()：
       http/https/socks5/socks5h → 原样返回（零开销，不经 sing-box）；
       socks4/socks4a/vless/vmess/trojan/ss/hysteria2/tuic → 本地 socks5 URL；
       direct/none/直连 → 空串（强制直连）；空串 → 空串。

设计决策（与用户确认过）：
  - sing-box 二进制：Docker 镜像内置；非 Docker 运行时自动探测
    （环境变量 SINGBOX_BIN → 常见路径 → PATH）；
  - 环境缺失 sing-box 时**明确报错**（ProxyResolveError），绝不静默直连，
    防止真实服务器 IP 意外泄露；
  - 同一 URI 全进程只起一个 sing-box 实例（端口稳定复用，池签名不抖动）；
    空闲超时自动回收（SINGBOX_IDLE_MINUTES，默认 30，0 = 不回收）。

sing-box 配置模板（每个实例一份）：
  { "log": {"level": "warn"},
    "inbounds": [{"type": "socks", "tag": "in", "listen": "127.0.0.1",
                  "listen_port": PORT}],
    "outbounds": [OB({"tag": "proxy"}), {"type": "direct", "tag": "direct"}],
    "route": {"final": "proxy"} }

socks5 入站不开启 sniff：上游域名原样转给远端解析（等价 socks5h 语义，
本地不出 DNS 查询，也不受容器 DNS 限制）。
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
import socket
import subprocess
import threading
import time
import atexit
from typing import Any, Optional
from urllib.parse import unquote, parse_qs, urlsplit


class ProxyResolveError(Exception):
    """代理解析/转换失败。消息面向最终用户，须给出可操作的指引。"""


# ============================================================
# 协议分类
# ============================================================

# httpx[socks] / curl_cffi 原生支持：原样透传，不经过 sing-box
NATIVE_PROXY_SCHEMES = {"http", "https", "socks5", "socks5h"}

# 需要 sing-box 转换的协议（scheme 小写）
SINGBOX_PROXY_SCHEMES = {
    "socks4", "socks4a",
    "vless", "vmess", "trojan",
    "ss", "hysteria2", "hy2", "tuic",
}

# 「强制直连」哨兵值（Key 级代理里最常用，全局代理里等价于留空）
DIRECT_TOKENS = {"direct", "none", "off", "直连", "无代理"}

ALL_PROXY_SCHEMES = NATIVE_PROXY_SCHEMES | SINGBOX_PROXY_SCHEMES


def _uri_scheme(uri: str) -> str:
    try:
        return (urlsplit(uri).scheme or "").strip().lower()
    except Exception:
        return ""


def classify_proxy_uri(uri: str) -> str:
    """分类代理 URI：native / singbox / direct / invalid。"""
    value = str(uri or "").strip()
    if not value:
        return "direct"
    if value.lower() in DIRECT_TOKENS:
        return "direct"
    scheme = _uri_scheme(value)
    if scheme in NATIVE_PROXY_SCHEMES:
        return "native"
    if scheme in SINGBOX_PROXY_SCHEMES:
        return "singbox"
    return "invalid"


# ============================================================
# 基础工具
# ============================================================

def _b64decode_text(s: str) -> str:
    """宽容的 base64 解码：urlsafe 字母表归一 + 自动补 padding + utf-8。"""
    s = s.strip().replace("-", "+").replace("_", "/")
    s += "=" * (-len(s) % 4)
    return base64.b64decode(s).decode("utf-8", errors="strict")


def _parse_host_port(hostport: str, uri: str) -> tuple[str, int]:
    """'host:port'（兼容 IPv6 [::1]:443 与 SIP002 尾斜杠）→ (host, port)。"""
    hostport = (hostport or "").strip().rstrip("/")
    if not hostport:
        raise ProxyResolveError(f"[{uri[:40]}…] 缺少主机与端口。")
    if hostport.startswith("["):                      # IPv6 字面量
        end = hostport.find("]")
        if end < 0:
            raise ProxyResolveError(f"[{uri[:40]}…] IPv6 地址格式不完整。")
        host = hostport[1:end]
        rest = hostport[end + 1:]
        if not rest.startswith(":"):
            raise ProxyResolveError(f"[{uri[:40]}…] 缺少端口。")
        port = int(rest[1:])
    else:
        if ":" not in hostport:
            raise ProxyResolveError(f"[{uri[:40]}…] 缺少端口（正确形态 host:port）。")
        host, _, port_s = hostport.rpartition(":")
        port = int(port_s)
    if not (1 <= port <= 65535):
        raise ProxyResolveError(f"[{uri[:40]}…] 端口超出 1–65535：{port}。")
    return host, port


def _qs(query: str) -> dict[str, str]:
    """查询串 → 单值 dict（重复键取首个）。"""
    try:
        return {k: v[0] for k, v in parse_qs(query or "", keep_blank_values=True).items()}
    except Exception:
        return {}


def _truthy(v: str) -> bool:
    return str(v or "").strip().lower() in ("1", "true", "yes")


# ============================================================
# TLS / transport 公共构造（vless / vmess / trojan 共用）
# ============================================================

def _build_tls(q: dict, *, default_sni: str = "", force: bool = False,
               allow_insecure_param: bool = True) -> Optional[dict]:
    """从查询参数构造 sing-box tls 对象。

    security: tls / reality（vless 用）；trojan/vmess 由调用方决定 force。
    utls 指纹（fp=chrome 等）一并挂上——分享链接几乎都带，sing-box 全支持。
    """
    security = str(q.get("security") or "").strip().lower()
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
    if allow_insecure_param and (_truthy(q.get("allowInsecure")) or _truthy(q.get("insecure"))):
        tls["insecure"] = True
    if security == "reality":
        reality: dict[str, Any] = {"enabled": True, "public_key": str(q.get("pbk") or "")}
        sid = str(q.get("sid") or "").strip()
        if sid:
            reality["short_id"] = sid
        tls["reality"] = reality
    return tls


def _build_transport(net: str, q: dict) -> Optional[dict]:
    """从查询参数构造 sing-box transport 对象（ws/grpc/http）。

    vless/trojan 用 type= 参数；vmess 用 net= 字段（值域相同，调用方归一后传入）。
    tcp / 无值 → 返回 None（不设 transport）。
    """
    net = (net or "tcp").strip().lower()
    if net in ("", "tcp", "raw", "none"):
        return None
    if net == "ws":
        tr: dict[str, Any] = {"type": "ws"}
        path = str(q.get("path") or "").strip()
        if path:
            tr["path"] = unquote(path)
        host = str(q.get("host") or "").strip()
        if host:
            tr["headers"] = {"Host": host}
        ed = str(q.get("ed") or "").strip()          # WebSocket 0-RTT（SFM/V2Ray 分支）
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
            tr["path"] = unquote(path)
        return tr
    # quic / kcp 等罕见值：sing-box 无对应 transport，忽略（回落 TCP）
    return None


# ============================================================
# 各协议 URI → sing-box outbound
# ============================================================

def _require_port(parts, scheme: str) -> int:
    """从 urlsplit 结果取端口；缺失/非法/越界直接报错（sing-box 配置不允许）。"""
    try:
        port = parts.port
    except ValueError as e:
        raise ProxyResolveError(f"{scheme} 链接端口不合法：{e}")
    if port is None:
        raise ProxyResolveError(f"{scheme} 链接缺少端口（正确形态 {scheme}://…@host:port）。")
    if port < 1:
        raise ProxyResolveError(f"{scheme} 链接端口必须在 1–65535 范围内（收到 {port}）。")
    return port


def _parse_vless(uri: str) -> dict:
    """vless://uuid@host:port?encryption=none&security=tls&sni=..&type=ws&path=..&fp=..#name"""
    parts = urlsplit(uri)
    uuid = unquote(parts.username or "")
    if not uuid:
        raise ProxyResolveError("vless 链接缺少 UUID（正确形态 vless://uuid@host:port?…）。")
    server = parts.hostname or ""
    if not server:
        raise ProxyResolveError("vless 链接缺少服务器地址。")
    port = _require_port(parts, "vless")
    ob: dict[str, Any] = {"type": "vless", "server": server, "server_port": port, "uuid": uuid}
    q = _qs(parts.query)
    flow = str(q.get("flow") or "").strip()
    if flow:
        ob["flow"] = flow
    tls = _build_tls(q)
    if tls:
        ob["tls"] = tls
    tr = _build_transport(str(q.get("type") or ""), q)
    if tr:
        ob["transport"] = tr
    return ob


def _parse_vmess(uri: str) -> dict:
    """vmess://base64(json)。json 字段：add/port/id/aid/scy/net/tls/sni/host/path/type。"""
    payload = uri[len("vmess://"):]
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
    uuid = str(data.get("id") or "").strip()
    if not uuid:
        raise ProxyResolveError("vmess 链接缺少用户 ID（id 字段）。")
    ob: dict[str, Any] = {
        "type": "vmess", "server": server, "server_port": port, "uuid": uuid,
        "security": str(data.get("scy") or "auto"),
    }
    try:
        ob["alter_id"] = int(data.get("aid") or 0)
    except (TypeError, ValueError):
        pass
    if str(data.get("tls", "")).strip().lower() in ("tls", "1", "true"):
        q = {
            "sni": data.get("sni"), "alpn": data.get("alpn"),
            "fp": data.get("fp"),
            "allowInsecure": data.get("allowInsecure"),
        }
        tls = _build_tls({k: v for k, v in q.items() if v}, force=True, default_sni=server)
        if tls:
            ob["tls"] = tls
    tr = _build_transport(str(data.get("net") or ""), {
        "path": data.get("path"), "host": data.get("host"),
        "serviceName": data.get("path") if str(data.get("net") or "").lower() == "grpc" else "",
    })
    if tr:
        ob["transport"] = tr
    return ob


def _parse_trojan(uri: str) -> dict:
    """trojan://password@host:port?sni=..&type=ws&path=..&fp=..#name（TLS 强制开启）"""
    parts = urlsplit(uri)
    password = unquote(parts.username or "")
    if not password:
        raise ProxyResolveError("trojan 链接缺少密码（userinfo 部分）。")
    server = parts.hostname or ""
    if not server:
        raise ProxyResolveError("trojan 链接缺少服务器地址。")
    port = _require_port(parts, "trojan")
    ob: dict[str, Any] = {"type": "trojan", "server": server, "server_port": port,
                          "password": password}
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
    body = uri[len("ss://"):]
    if "#" in body:
        body = body.split("#", 1)[0]
    if "?" in body:                                   # SIP003 plugin 参数暂不支持，剥掉
        body = body.split("?", 1)[0]
    userinfo: str
    if "@" in body:
        userinfo_raw, hostport = body.rsplit("@", 1)
        userinfo = userinfo_raw
        if ":" not in userinfo:                       # base64(method:password)
            try:
                userinfo = _b64decode_text(userinfo_raw)
            except Exception:
                raise ProxyResolveError("ss 链接的 userinfo 无法解码（应为 base64(method:password)）。")
    else:                                             # 旧式：整体 base64(method:pass@host:port)
        try:
            decoded = _b64decode_text(body)
        except Exception as e:
            raise ProxyResolveError(f"ss 链接无法解码：{e}")
        if "@" not in decoded:
            raise ProxyResolveError("ss 链接格式不认识（既非 SIP002 也非旧式 base64）。")
        userinfo, _, hostport = decoded.rpartition("@")
    method, _, password = unquote(userinfo).partition(":")
    if not method or not password:
        raise ProxyResolveError("ss 链接缺少 method:password（userinfo 解析失败）。")
    host, port = _parse_host_port(hostport, uri)
    return {"type": "shadowsocks", "server": host, "server_port": port,
            "method": method, "password": password}


def _parse_socks4(uri: str, version: str = "4") -> dict:
    """socks4://[user@]host:port → sing-box socks outbound（version 4 / 4a）。"""
    parts = urlsplit(uri)
    server = parts.hostname or ""
    if not server:
        raise ProxyResolveError("socks4 链接缺少服务器地址。")
    port = _require_port(parts, "socks4")
    ob: dict[str, Any] = {"type": "socks", "server": server, "server_port": port,
                          "version": version}
    user = unquote(parts.username or "")
    if user:
        ob["username"] = user
    return ob


def _parse_hysteria2(uri: str) -> dict:
    """hysteria2://auth@host:port?sni=..&insecure=1&obfs=salamander&obfs-password=..#name"""
    parts = urlsplit(uri)
    auth = unquote(parts.username or "")
    if not auth:
        raise ProxyResolveError("hysteria2 链接缺少认证密码（userinfo 部分）。")
    server = parts.hostname or ""
    if not server:
        raise ProxyResolveError("hysteria2 链接缺少服务器地址。")
    port = _require_port(parts, "hysteria2")
    ob: dict[str, Any] = {"type": "hysteria2", "server": server, "server_port": port,
                          "password": auth}
    q = _qs(parts.query)
    tls: dict[str, Any] = {"enabled": True, "alpn": ["h3"]}
    sni = str(q.get("sni") or "").strip()
    if sni:
        tls["server_name"] = sni
    if _truthy(q.get("insecure")):
        tls["insecure"] = True
    ob["tls"] = tls
    obfs = str(q.get("obfs") or "").strip().lower()
    if obfs == "salamander":
        ob["obfs"] = {"type": "salamander", "password": str(q.get("obfs-password") or "")}
    elif obfs:
        raise ProxyResolveError(f"hysteria2 不支持的混淆类型：{obfs}（仅 salamander）。")
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
    ob: dict[str, Any] = {"type": "tuic", "server": server, "server_port": port,
                          "uuid": user, "password": password,
                          "congestion_control": str(_qs(parts.query).get("congestion_control")
                                                    or "bbr")}
    q = _qs(parts.query)
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
        f"（支持：{'/'.join(sorted(SINGBOX_PROXY_SCHEMES))}）。")


# ============================================================
# sing-box 二进制探测
# ============================================================

_SINGBOX_CANDIDATE_PATHS = (
    "/usr/local/bin/sing-box",
    "/usr/bin/sing-box",
    "/opt/sing-box/sing-box",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "bin", "sing-box"),
)


def find_singbox_binary() -> Optional[str]:
    """按优先级探测 sing-box：环境变量 SINGBOX_BIN → 常见路径 → PATH。

    返回可执行文件绝对路径，找不到返回 None。结果做进程内缓存
    （None=未探测，""=探测过但缺失；部署环境里装上 sing-box 后需重启才会被发现）。
    """
    global _singbox_bin_cache
    if _singbox_bin_cache is not None:
        return _singbox_bin_cache or None
    env_path = str(os.environ.get("SINGBOX_BIN") or "").strip()
    candidates: list[str] = []
    if env_path:
        candidates.append(env_path)
    candidates.extend(_SINGBOX_CANDIDATE_PATHS)
    found = shutil.which("sing-box") if hasattr(shutil, "which") else None
    if found:
        candidates.append(found)
    for path in candidates:
        if path and os.path.isfile(path) and os.access(path, os.X_OK):
            _singbox_bin_cache = path
            return path
    _singbox_bin_cache = ""
    return None


# None=尚未探测；""=探测过但不存在；其他=可执行文件路径
_singbox_bin_cache: Optional[str] = None


def singbox_available() -> bool:
    return find_singbox_binary() is not None


_SINGBOX_VERSION_CACHE: Optional[str] = None


def singbox_version() -> str:
    """sing-box 版本串（探测失败返回空串）。供启动日志与 /healthz 展示。"""
    global _SINGBOX_VERSION_CACHE
    if _SINGBOX_VERSION_CACHE is not None:
        return _SINGBOX_VERSION_CACHE
    binary = find_singbox_binary()
    if not binary:
        _SINGBOX_VERSION_CACHE = ""
        return ""
    try:
        out = subprocess.run([binary, "version"], capture_output=True, text=True,
                             timeout=10)
        first = (out.stdout or out.stderr or "").strip().splitlines()
        _SINGBOX_VERSION_CACHE = first[0].strip() if first else ""
    except Exception:
        _SINGBOX_VERSION_CACHE = ""
    return _SINGBOX_VERSION_CACHE


# ============================================================
# sing-box 实例管理器
# ============================================================

def _idle_minutes() -> float:
    try:
        return max(0.0, float(os.environ.get("SINGBOX_IDLE_MINUTES", "30")))
    except ValueError:
        return 30.0


def _state_dir() -> str:
    base = os.environ.get("STATE_DIR", ".")
    return os.path.join(base, "singbox")


class _SingboxInstance:
    __slots__ = ("uri", "port", "process", "config_path", "log_path", "started_at", "last_used")

    def __init__(self, uri: str, port: int, process, config_path: str, log_path: str):
        self.uri = uri
        self.port = port
        self.process = process
        self.config_path = config_path
        self.log_path = log_path
        self.started_at = time.time()
        self.last_used = time.time()

    def alive(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def log_tail(self, limit: int = 400) -> str:
        try:
            with open(self.log_path, "r", encoding="utf-8", errors="replace") as f:
                return f.read()[-limit:]
        except Exception:
            return ""


def _find_free_port() -> int:
    """让 OS 分配一个空闲端口（bind(0) 后立即关掉，存在微小竞态，可接受）。"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_port_ready(port: int, timeout: float = 8.0) -> bool:
    """轮询本地端口直到可连通（sing-box 起监听即认为就绪）。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.4):
                return True
        except OSError:
            time.sleep(0.05)
    return False


_MISSING_HINT = (
    "当前环境未找到 sing-box 可执行文件。解决方法任选其一："
    "① 使用本项目的 Docker 镜像（已内置 sing-box）；"
    "② 手动安装 sing-box（https://github.com/SagerNet/sing-box/releases）；"
    "③ 已安装但不在默认路径时，设环境变量 SINGBOX_BIN 指向可执行文件。"
    "在装好之前，vless/vmess/trojan/ss/socks4/hysteria2/tuic 协议的代理无法使用"
    "（为防真实 IP 泄露，不会自动降级直连）。"
)


class SingboxManager:
    """sing-box 子进程池：代理 URI → 本地 socks5 入站，惰性启动、稳定复用。

    线程安全；同 URI 只起一个进程。崩溃的实例在下次取用时自动重启
    （复用原端口，客户端池签名不变，httpx 连接池无需重建）。
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._instances: dict[str, _SingboxInstance] = {}
        self._closed = False

    # ---------- 对外 ----------

    def get_local_socks5(self, uri: str) -> str:
        """URI → socks5://127.0.0.1:{port}。首次调用启动 sing-box（同步，约百毫秒级）。"""
        uri = str(uri or "").strip()
        if not uri:
            raise ProxyResolveError("get_local_socks5 收到空 URI。")
        inst = self._instances.get(uri)
        if inst is not None and inst.alive():
            inst.last_used = time.time()
            return f"socks5://127.0.0.1:{inst.port}"

        with self._lock:
            if self._closed:
                raise ProxyResolveError("进程正在退出，sing-box 已停。")
            inst = self._instances.get(uri)
            if inst is not None and inst.alive():
                inst.last_used = time.time()
                return f"socks5://127.0.0.1:{inst.port}"
            port = inst.port if inst is not None else _find_free_port()
            inst = self._launch(uri, port)
            self._instances[uri] = inst
            return f"socks5://127.0.0.1:{inst.port}"

    def shutdown_all(self) -> None:
        """停机：终止全部 sing-box 子进程（FastAPI shutdown / atexit 双保险）。"""
        with self._lock:
            self._closed = True
            instances = list(self._instances.values())
            self._instances.clear()
        for inst in instances:
            self._terminate(inst)

    def reap_idle(self) -> int:
        """回收空闲实例（超过 SINGBOX_IDLE_MINUTES 未使用）。返回回收数量。"""
        idle_seconds = _idle_minutes() * 60.0
        if idle_seconds <= 0:
            return 0
        now = time.time()
        with self._lock:
            stale = [k for k, v in self._instances.items()
                     if now - v.last_used > idle_seconds]
            for k in stale:
                inst = self._instances.pop(k)
                self._terminate(inst)
        return len(stale)

    def stats(self) -> dict:
        """运行概览（供日志 / 控制台展示）。"""
        with self._lock:
            items = list(self._instances.values())
        return {
            "available": singbox_available(),
            "binary": find_singbox_binary() or "",
            "version": singbox_version(),
            "instances": [
                {"port": i.port, "uri": mask_proxy_uri(i.uri),
                 "alive": i.alive(), "started_at": int(i.started_at)}
                for i in items
            ],
        }

    # ---------- 内部 ----------

    def _launch(self, uri: str, port: int) -> _SingboxInstance:
        binary = find_singbox_binary()
        if not binary:
            raise ProxyResolveError(_MISSING_HINT)

        try:
            outbound = parse_share_uri_to_outbound(uri)
        except ProxyResolveError:
            raise
        except Exception as e:
            raise ProxyResolveError(f"解析代理链接失败：{type(e).__name__}: {e}")

        outbound = dict(outbound)
        outbound["tag"] = "proxy"

        directory = _state_dir()
        os.makedirs(directory, exist_ok=True)
        digest = hashlib.sha1(uri.encode("utf-8")).hexdigest()[:16]
        # 清掉同 URI 的旧实例残留（崩溃重启/端口变化后避免配置与日志文件无限积累）
        try:
            import glob as _glob
            for stale in _glob.glob(os.path.join(directory, f"sb-{digest}-*")):
                os.unlink(stale)
        except OSError:
            pass
        config_path = os.path.join(directory, f"sb-{digest}-{port}.json")
        log_path = os.path.join(directory, f"sb-{digest}-{port}.log")

        config = {
            "log": {"level": "warn", "timestamp": True},
            "inbounds": [{
                "type": "socks", "tag": "in",
                "listen": "127.0.0.1", "listen_port": port,
            }],
            "outbounds": [outbound, {"type": "direct", "tag": "direct"}],
            "route": {"final": "proxy"},
        }
        try:
            with open(config_path, "w", encoding="utf-8") as f:
                json.dump(config, f, ensure_ascii=False, indent=2)
        except OSError as e:
            raise ProxyResolveError(f"无法写入 sing-box 配置文件（{config_path}）：{e}")

        log_file = open(log_path, "ab", buffering=0)
        try:
            process = subprocess.Popen(
                [binary, "run", "-c", config_path, "-D", directory],
                stdout=log_file, stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                start_new_session=True,          # 进程组独立，Ctrl+C 不误杀
            )
        except OSError as e:
            log_file.close()
            raise ProxyResolveError(f"启动 sing-box 失败（{binary}）：{e}")

        inst = _SingboxInstance(uri, port, process, config_path, log_path)
        if not _wait_port_ready(port, timeout=10.0):
            tail = ""
            if process.poll() is not None:       # 进程已退出：配置大概率有错
                inst.process = process
                tail = inst.log_tail()
            self._terminate(inst)
            detail = f"sing-box 启动后端口 {port} 未就绪。日志尾部：{tail or '（无输出）'}"
            raise ProxyResolveError(f"代理「{mask_proxy_uri(uri)}」转换失败：{detail}")
        log_file.close()
        print(f"🛰️ [代理转换] sing-box 已启动：{mask_proxy_uri(uri)} → 127.0.0.1:{port}")
        return inst

    @staticmethod
    def _terminate(inst: _SingboxInstance) -> None:
        process = inst.process
        if process is None or process.poll() is not None:
            return
        try:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
        except Exception:
            pass


singbox_manager = SingboxManager()


def _reaper_loop() -> None:
    """后台空闲回收线程（每 5 分钟扫一次，daemon）。"""
    while True:
        time.sleep(300)
        try:
            n = singbox_manager.reap_idle()
            if n:
                print(f"♻️ [代理转换] 已回收 {n} 个空闲 sing-box 实例。")
        except Exception:
            pass


_reaper_started = False
_reaper_lock = threading.Lock()


def ensure_reaper() -> None:
    """惰性启动空闲回收线程（首次成功创建 sing-box 实例时调用）。"""
    global _reaper_started
    with _reaper_lock:
        if _reaper_started:
            return
        _reaper_started = True
        threading.Thread(target=_reaper_loop, daemon=True,
                         name="singbox-reaper").start()


atexit.register(singbox_manager.shutdown_all)


# ============================================================
# 统一入口：resolve / validate / mask
# ============================================================

def resolve_proxy_uri(uri: Optional[str]) -> str:
    """任意代理 URI → httpx / curl_cffi 可直接使用的代理 URL。

    - 空 / direct 哨兵 → ""（直连）；
    - http/https/socks5/socks5h → 原样返回；
    - socks4/socks4a/vless/vmess/trojan/ss/hysteria2/tuic → socks5://127.0.0.1:{port}
      （首次使用时惰性启动 sing-box；之后纯缓存查找，开销≈0）；
    - 其他 → 抛 ProxyResolveError（含可操作指引，绝不静默直连）。

    结果稳定：同一 URI 在进程生命周期内永远映射到同一端口，
    客户端池签名不会因重复 resolve 而抖动。
    """
    value = str(uri or "").strip()
    if not value or value.lower() in DIRECT_TOKENS:
        return ""
    kind = classify_proxy_uri(value)
    if kind == "native":
        return value
    if kind == "singbox":
        url = singbox_manager.get_local_socks5(value)
        ensure_reaper()
        return url
    scheme = _uri_scheme(value)
    raise ProxyResolveError(
        f"不支持的代理协议「{scheme or '（无协议）'}」。"
        f"支持：{'/'.join(sorted(ALL_PROXY_SCHEMES))} 或 direct（强制直连）。"
        f"收到的是：{mask_proxy_uri(value)}")


def validate_proxy_uri(uri: str) -> str:
    """静态校验代理 URI，返回错误消息（空串 = 合法）。不启动 sing-box、不发包。

    合法性 = 能解析出该协议的必要字段；连通性请走 /api/proxy/test。
    """
    value = str(uri or "").strip()
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
    scheme = _uri_scheme(value)
    return (f"协议必须是 {'/'.join(sorted(ALL_PROXY_SCHEMES))} 之一"
            f"（或 direct = 强制直连），收到的是「{scheme or '（无协议）'}」")


def mask_proxy_uri(uri: str) -> str:
    """代理 URI 脱敏：保留协议与 host:port，凭据（密码/UUID/base64）整体打码。

    同时覆盖原生 URL 与 sing-box 分享链接两种形态；解析失败时退回哈希指纹，
    保证日志/healthz 里绝不泄露凭据。
    """
    value = str(uri or "").strip()
    if not value:
        return ""
    if value.lower() in DIRECT_TOKENS:
        return "direct（强制直连）"
    scheme = _uri_scheme(value)
    try:
        if scheme == "vmess":
            # vmess 整体是 base64，没有可提取的 host:port，只报长度
            return f"vmess://***({len(value)} 字符)"
        parts = urlsplit(value)
        host = parts.hostname or ""
        if not host:
            raise ValueError
        port = f":{parts.port}" if parts.port else ""
        cred = parts.username or parts.password
        masked_netloc = f"***@{host}{port}" if cred else f"{host}{port}"
        # ss 旧式整体 base64 无 @：host 解析不出来，落到 except
        from urllib.parse import urlunsplit
        return urlunsplit((scheme or "proxy", masked_netloc, "", "", ""))
    except Exception:
        digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:8]
        return f"proxy:{digest}(已配置，格式无法解析)"

"""Proxy input normalization shared by environment settings, APIs and transports."""

import base64
import ipaddress
import json
import re
from urllib.parse import parse_qs, quote, unquote, urlencode, urlsplit

DIRECT_TOKENS = {"direct", "none", "off", "直连", "无代理"}
FORM_PROXY_SCHEMES = {"http", "https", "socks5", "socks5h", "socks4", "socks4a"}
BRIDGE_PROXY_SCHEMES = {"turn", "turns", "sstp"}
BRIDGE_DEFAULT_PORTS = {"turn": 3478, "turns": 5349, "sstp": 443}
_ALIASES = {"socks": "socks5", "socks5-hostname": "socks5h", "socks4-hostname": "socks4a"}

# sing-box 转换的分享链接协议（见 proxy_resolver.SINGBOX_PROXY_SCHEMES）。
# proxy_fields 对这些协议回传实际 scheme：前端代理编辑器据此在识别提示里
# 显示协议名，并把它们统一归入「sing-box 节点」下拉选项（整条链接编辑）。
SHARE_PROXY_SCHEMES = {"vless", "vmess", "trojan", "ss", "hysteria2", "hy2", "tuic"}


class ProxyFormatError(ValueError):
    pass


def _host_port(value):
    try:
        parts = urlsplit("//" + value)
        if parts.hostname and parts.port and not parts.username and not parts.path:
            return parts.hostname, parts.port
    except ValueError:
        pass
    return None


def _looks_like_host(value):
    host = value.strip("[]")
    try:
        ipaddress.ip_address(host)
        return True
    except ValueError:
        return "." in host or host.lower() == "localhost"


def build_proxy_uri(scheme, host, port, username="", password="", *, options=None):
    """Build an unambiguous URL; reserved characters in credentials are encoded."""
    scheme = _ALIASES.get(str(scheme).lower(), str(scheme).lower())
    if scheme not in FORM_PROXY_SCHEMES | BRIDGE_PROXY_SCHEMES:
        raise ProxyFormatError("请选择 HTTP、HTTPS、SOCKS、TURN、TURNS 或 SSTP 代理类型。")
    if scheme in BRIDGE_PROXY_SCHEMES and (port is None or str(port).strip() == ""):
        port = BRIDGE_DEFAULT_PORTS[scheme]
    host = str(host or "").strip().strip("[]")
    if not host or any(c.isspace() or c in "/@?#\\" for c in host):
        raise ProxyFormatError("请填写有效的代理 IP 或主机名。")
    if ":" in host:
        try:
            ipaddress.IPv6Address(host)
        except ValueError:
            raise ProxyFormatError("IPv6 地址无效。") from None
        host = f"[{host}]"
    try:
        port_text = str(port).strip()
        # 严格十进制：拒绝全角数字、"+80"、空串等 int() 能吞的形态
        # （这是表单校验入口，宽松会放过手误）。
        if not re.fullmatch(r"[0-9]{1,5}", port_text):
            raise ValueError
        port = int(port_text)
        if not 1 <= port <= 65535:
            raise ValueError
    except (TypeError, ValueError):
        raise ProxyFormatError("代理端口必须是 1–65535 的整数。") from None
    username, password = str(username or ""), str(password or "")
    if any(ord(c) < 32 or ord(c) == 127 for c in username + password):
        raise ProxyFormatError("代理账号和密码不能包含换行或控制字符。")
    if password and not username:
        raise ProxyFormatError("填写密码时也需要填写用户名。")
    auth = (quote(username, safe="") + ":" + quote(password, safe="") + "@") if username else ""
    uri = f"{scheme}://{auth}{host}:{port}"
    if options:
        if scheme not in BRIDGE_PROXY_SCHEMES or not isinstance(options, dict):
            raise ProxyFormatError("高级代理参数仅适用于 TURN / TURNS / SSTP。")
        query = {str(k): str(v).lower() if isinstance(v, bool) else str(v)
                 for k, v in options.items() if v is not None and str(v) != ""}
        uri += "?" + urlencode(query)
    return uri


def normalize_proxy_uri(value, default_scheme="socks5"):
    """Accept URL, host:port:user:pass, user:pass:host:port and reversed @ forms.

    Inputs without a protocol default to SOCKS5. Share links for other protocols
    are preserved for proxy_resolver's protocol-specific validation.
    """
    if not isinstance(value, str):
        raise ProxyFormatError("代理地址必须是字符串。")
    value = value.strip()
    if not value:
        return ""
    if value.lower() in DIRECT_TOKENS:
        return "direct"
    if any(ord(c) < 32 or ord(c) == 127 for c in value):
        raise ProxyFormatError("单条代理地址不能包含换行或控制字符。")
    # RFC 7065 TURN URI 使用 turn:host:port，而不是必须带 //。
    # 保留给协议解析层，避免被误当成省略协议的 SOCKS5 地址。
    if re.match(r"^turns?:", value, re.IGNORECASE):
        return value
    match = re.match(r"^([a-zA-Z][a-zA-Z0-9+.-]*)://(.*)$", value)
    scheme, authority = (match.group(1).lower(), match.group(2)) if match else (default_scheme, value)
    scheme = _ALIASES.get(scheme, scheme)
    if scheme not in FORM_PROXY_SCHEMES:
        return value

    username = password = ""
    supplier = re.match(r"^(\[[^\]]+\]|[^:@/?#\s]+):(\d+):([^:]+):(.+)$", authority)
    # A supplier password may contain @, # or :. Prefer this shape only when
    # the suffix is not itself a valid standard URL endpoint.
    right_endpoint = authority.rsplit("@", 1)[-1].split("?", 1)[0].split("#", 1)[0].rstrip("/")
    if supplier and (("@" in authority and not _host_port(right_endpoint)) or
                     ("@" not in authority and (_looks_like_host(supplier[0]) or not supplier[4].isdecimal()))):
        host, port, username, password = supplier.groups()
        return build_proxy_uri(scheme, host, port, unquote(username), unquote(password))
    # netloc 先剥掉 query/fragment 再做 @ 分析：否则 "?peer=a@b" 这类查询参数
    # 里的 @ 会把链接错切成凭据/端点两半，静默生成错误凭据。
    netloc = authority.split("?", 1)[0].split("#", 1)[0]
    if "@" in netloc:
        left, right = netloc.rsplit("@", 1)
        endpoint = right.rstrip("/")
        left_hp, right_hp = _host_port(left), _host_port(endpoint)
        reversed_auth = left_hp and (not right_hp or (
            _looks_like_host(left_hp[0]) and not _looks_like_host(right_hp[0])))
        if reversed_auth:
            endpoint, credentials = left, right
        else:
            credentials = left
        if ":" not in credentials:
            try:
                decoded = base64.b64decode(credentials + "=" * (-len(credentials) % 4), validate=True).decode()
                if ":" in decoded:
                    credentials = decoded
            except (ValueError, UnicodeError):
                pass
        username, _, password = credentials.partition(":")
        username, password = unquote(username), unquote(password)
    else:
        endpoint = netloc.rstrip("/")
        # Colon and whitespace supplier formats. IPv6 endpoints must use brackets.
        tokens = re.split(r"\s+|\|", endpoint) if re.search(r"\s|\|", endpoint) else None
        four = re.match(r"^(\[[^\]]+\]|[^:]+):([^:]+):([^:]+):(.+)$", endpoint)
        ipv6_last = re.match(r"^([^:]+):([^:]+):(\[[^\]]+\]):(\d+)$", endpoint)
        if ipv6_last:
            username, password, host, port = ipv6_last.groups()
            return build_proxy_uri(scheme, host, port, unquote(username), unquote(password))
        if tokens and len(tokens) == 4:
            host, port, username, password = tokens
            return build_proxy_uri(scheme, host, port, username, password)
        if four:
            a, b, c, d = four.groups()
            b_is_port = b.isdecimal() and len(b) <= 5 and 1 <= int(b) <= 65535
            d_is_port = d.isdecimal() and len(d) <= 5 and 1 <= int(d) <= 65535
            if _looks_like_host(a) and b.isdecimal():
                # 主机:端口:用户:密码：host 带点/IP/localhost 时无歧义；端口段
                # 越界也先进此分支，由 build_proxy_uri 给出精确报错。
                endpoint, username, password = f"{a}:{b}", unquote(c), unquote(d)
            elif _looks_like_host(c) and d_is_port:
                # 用户:密码:主机:端口：第三段是主机且第四段为合法端口。
                username, password, endpoint = unquote(a), unquote(b), f"{c}:{d}"
            elif b_is_port:
                # 两端都不含点（LAN 裸主机名）且密码恰为纯数字时真歧义：按供应
                # 商列表惯例（docstring 第一种格式）取 主机:端口:用户:密码，
                # 不再静默错位成 user:pass:host:port。
                endpoint, username, password = f"{a}:{b}", unquote(c), unquote(d)
            elif d_is_port:
                username, password, endpoint = unquote(a), unquote(b), f"{c}:{d}"
        query = parse_qs(authority.split("?", 1)[1].split("#", 1)[0]) if "?" in authority else {}
        username = (query.get("username") or query.get("user") or [username])[0]
        password = (query.get("password") or query.get("pass") or [password])[0]
    try:
        parts = urlsplit("//" + endpoint)
        if parts.path or not parts.hostname or parts.username:
            raise ValueError
    except (ValueError, TypeError):
        raise ProxyFormatError("无法识别代理地址；支持 协议://用户:密码@主机:端口、主机:端口:用户:密码、用户:密码:主机:端口。") from None
    try:
        # urlsplit 的 .port 访问时才做范围校验，单独包一层给出精确报错；
        # build_proxy_uri 自身的 ProxyFormatError 则原样向上抛。
        port = parts.port
    except ValueError:
        raise ProxyFormatError("代理端口必须是 1–65535 的整数。") from None
    if port is None:
        port = {"http": 80, "https": 443}.get(scheme)
    return build_proxy_uri(scheme, parts.hostname, port, username, password)


def parse_proxy_list(value, *, default_scheme="socks5"):
    """Read ordered proxy lists (JSON array, one per line, or comma-separated URLs)."""
    if value is None or value == "":
        return []
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("[") and not re.match(r"^\[[0-9a-fA-F:]+\]:", stripped):
            try:
                value = json.loads(stripped)
            except ValueError:
                raise ProxyFormatError("代理列表 JSON 无效，请使用字符串数组。") from None
        else:
            value = re.split(r"[\r\n]+|,\s*(?=[a-zA-Z][a-zA-Z0-9+.-]*://|turns?:)", stripped,
                             flags=re.IGNORECASE)
    if not isinstance(value, list):
        raise ProxyFormatError("代理列表必须是数组或每行一条的地址。")
    out = []
    for item in value:
        uri = normalize_proxy_uri(item, default_scheme=default_scheme)
        if uri and uri not in out:
            out.append(uri)
    return out


def proxy_fields(uri):
    """Return normalized form fields for the proxy editor."""
    value = normalize_proxy_uri(uri)
    if not value or value.lower() in DIRECT_TOKENS:
        # direct 哨兵与空串不走 urlsplit：urlsplit("direct") 的 scheme 为空，
        # 会被误报成 share（前端应选中"强制直连"）。
        return {"url": value, "type": "direct"}
    parts = urlsplit(value)
    if parts.scheme in BRIDGE_PROXY_SCHEMES:
        config = parse_bridge_proxy_uri(value)
        return {"url": value, "type": config["scheme"], "host": config["server"],
                "port": config["server_port"], "username": config["username"],
                "password": config["password"], "options": config["options"]}
    if parts.scheme not in FORM_PROXY_SCHEMES:
        # 分享链接回传实际协议（vless/vmess/...），前端选中对应类型；
        # 未知协议保持通用 share（仅整条 URI 编辑）。
        scheme = parts.scheme if parts.scheme in SHARE_PROXY_SCHEMES else "share"
        return {"url": value, "type": scheme}
    return {"url": value, "type": parts.scheme, "host": parts.hostname,
            "port": parts.port, "username": unquote(parts.username or ""),
            "password": unquote(parts.password or "")}


def parse_bridge_proxy_uri(uri):
    """Parse TURN/TURNS/SSTP without network access or credential-bearing errors."""
    value = str(uri).strip()
    value = re.sub(r"^(turns?):(?!//)", r"\1://", value, flags=re.IGNORECASE)
    try:
        parts = urlsplit(value)
        scheme = parts.scheme.lower()
        if scheme not in BRIDGE_PROXY_SCHEMES:
            raise ValueError
        host = parts.hostname
        port = parts.port if parts.port is not None else BRIDGE_DEFAULT_PORTS[scheme]
        if not host or not 1 <= port <= 65535 or parts.path not in {"", "/"}:
            raise ValueError
        if any(c.isspace() or c in "%/@?#\\" for c in host):
            raise ValueError
        if ":" in host:
            ipaddress.IPv6Address(host)
        else:
            host = host.encode("idna").decode("ascii")
        query = parse_qs(parts.query, keep_blank_values=True, strict_parsing=True,
                         encoding="utf-8", errors="strict")
        if any(len(values) != 1 for values in query.values()):
            raise ValueError
        query = {key: values[0] for key, values in query.items()}
        username = unquote(parts.username or "", errors="strict")
        password = unquote(parts.password or "", errors="strict")
        if (username or password) and ({"username", "password"} & query.keys()):
            raise ValueError
        username = username or query.pop("username", "")
        password = password or query.pop("password", "")
        if any(ord(c) < 32 or ord(c) == 127 for text in [host, username, password, *query.values()]
               for c in text):
            raise ValueError
    except (ValueError, UnicodeError):
        raise ProxyFormatError("TURN / SSTP 地址无效，请检查主机、端口、凭据编码和重复参数。") from None
    if not username or not password:
        raise ProxyFormatError("TURN / SSTP 需要同时填写用户名和密码；链接内的特殊字符须进行 URL 编码。")
    allowed = {"sni", "ca", "insecure"}
    allowed |= {"transport", "realm"} if scheme in {"turn", "turns"} else {"mtu"}
    if query.keys() - allowed:
        raise ProxyFormatError("TURN / SSTP 包含不支持的高级参数，请查看接入文档。")
    if scheme in {"turn", "turns"} and query.get("transport", "tcp").lower() != "tcp":
        raise ProxyFormatError("TURN 代理仅支持 RFC 6062 TCP 中继，transport 必须为 tcp。")
    if scheme == "turn" and {"sni", "ca", "insecure"} & query.keys():
        raise ProxyFormatError("TLS 参数仅适用于 turns:// 或 sstp://。")
    if "insecure" in query and query["insecure"].lower() not in {"0", "1", "false", "true"}:
        raise ProxyFormatError("insecure 必须是 true / false 或 1 / 0。")
    try:
        mtu = int(query.get("mtu", "1400"))
        if not 576 <= mtu <= 9000:
            raise ValueError
    except ValueError:
        raise ProxyFormatError("SSTP MTU 必须是 576–9000 的整数。") from None
    return {"scheme": scheme, "server": host, "server_port": port,
            "username": username, "password": password,
            "tls_server_name": query.get("sni") or host, "ca_file": query.get("ca", ""),
            "insecure": query.get("insecure", "false").lower() in {"true", "1"},
            "realm": query.get("realm", ""), "mtu": mtu, "options": query}

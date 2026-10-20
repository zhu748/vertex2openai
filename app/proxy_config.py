"""Proxy input normalization shared by environment settings, APIs and transports."""

import base64
import ipaddress
import json
import re
from urllib.parse import parse_qs, quote, unquote, urlsplit

DIRECT_TOKENS = {"direct", "none", "off", "直连", "无代理"}
FORM_PROXY_SCHEMES = {"http", "https", "socks5", "socks5h", "socks4", "socks4a"}
_ALIASES = {"socks": "socks5", "socks5-hostname": "socks5h", "socks4-hostname": "socks4a"}


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


def build_proxy_uri(scheme, host, port, username="", password=""):
    """Build an unambiguous URL; reserved characters in credentials are encoded."""
    scheme = _ALIASES.get(str(scheme).lower(), str(scheme).lower())
    if scheme not in FORM_PROXY_SCHEMES:
        raise ProxyFormatError("请选择 HTTP、HTTPS 或 SOCKS 代理类型。")
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
        port = int(str(port))
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
    return f"{scheme}://{auth}{host}:{port}"


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
                     ("@" not in authority and (_looks_like_host(supplier[1]) or not supplier[4].isdecimal()))):
        host, port, username, password = supplier.groups()
        return build_proxy_uri(scheme, host, port, unquote(username), unquote(password))
    if "@" in authority:
        left, right = authority.rsplit("@", 1)
        endpoint = right.split("?", 1)[0].split("#", 1)[0].rstrip("/")
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
        endpoint = authority.split("?", 1)[0].split("#", 1)[0].rstrip("/")
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
            if b.isdecimal() and (_looks_like_host(a) or not d.isdecimal()):
                endpoint, username, password = f"{a}:{b}", unquote(c), unquote(d)
            elif d.isdecimal():
                username, password, endpoint = unquote(a), unquote(b), f"{c}:{d}"
            elif b.isdecimal():
                endpoint, username, password = f"{a}:{b}", unquote(c), unquote(d)
        query = parse_qs(authority.split("?", 1)[1].split("#", 1)[0]) if "?" in authority else {}
        username = (query.get("username") or query.get("user") or [username])[0]
        password = (query.get("password") or query.get("pass") or [password])[0]
    try:
        parts = urlsplit("//" + endpoint)
        if parts.path or not parts.hostname or parts.username:
            raise ValueError
        port = parts.port if parts.port is not None else {"http": 80, "https": 443}.get(scheme)
        return build_proxy_uri(scheme, parts.hostname, port, username, password)
    except (ValueError, TypeError):
        raise ProxyFormatError("无法识别代理地址；支持 协议://用户:密码@主机:端口、主机:端口:用户:密码、用户:密码:主机:端口。") from None


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
            value = re.split(r"[\r\n]+|,\s*(?=[a-zA-Z][a-zA-Z0-9+.-]*://)", stripped)
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
    parts = urlsplit(value)
    if parts.scheme not in FORM_PROXY_SCHEMES:
        return {"url": value, "type": "share"}
    return {"url": value, "type": parts.scheme, "host": parts.hostname,
            "port": parts.port, "username": unquote(parts.username or ""),
            "password": unquote(parts.password or "")}

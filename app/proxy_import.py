"""Bounded, partial-success import previews. Saving still uses the strict parser."""

import hashlib
import json
import re
from urllib.parse import urlsplit, urlunsplit

from proxy_config import ProxyFormatError, _unique_json_object, parse_proxy_list, proxy_fields
from proxy_resolver import classify_proxy_uri, mask_proxy_uri, parse_share_uri_to_outbound, validate_proxy_uri

MAX_IMPORT_ITEMS = 200
MAX_IMPORT_BYTES = 256 * 1024


def proxy_identity(uri):
    """Connection identity ignores display names, but retains credentials/options."""
    if classify_proxy_uri(uri) == "singbox":
        value = json.dumps(parse_share_uri_to_outbound(uri), sort_keys=True, ensure_ascii=True)
    else:
        parts = urlsplit(uri)
        value = urlunsplit(parts._replace(fragment=""))
    return hashlib.sha256(value.encode()).hexdigest()


def preview_proxy_import(text, *, default_scheme="socks5", existing=()):
    if len(text.encode("utf-8")) > MAX_IMPORT_BYTES:
        raise ProxyFormatError("导入内容不能超过 256 KiB。")
    if not text.strip():
        raise ProxyFormatError("请先粘贴代理地址。")

    def load_json(value):
        try:
            return json.loads(value, object_pairs_hook=_unique_json_object)
        except (ValueError, RecursionError):
            raise ProxyFormatError("JSON 无效，请检查括号、引号与重复字段。") from None

    # Expand containers before validation, so one bad ICE URL/node does not
    # discard its siblings. Every returned row keeps a source position.
    def expand(value, source):
        if isinstance(value, dict) and set(value) == {"iceServers"} and isinstance(value["iceServers"], list):
            for index, server in enumerate(value["iceServers"]):
                if isinstance(server, dict) and "iceServers" in server:
                    yield source + f".iceServers[{index + 1}]", server
                else:
                    yield from expand(server, source + f".iceServers[{index + 1}]")
        elif (isinstance(value, dict) and isinstance(value.get("urls"), list)
              and not set(value) - {"urls", "username", "credential", "credentialType"}):
            if not value["urls"]:
                yield source, value
            for index, url in enumerate(value["urls"]):
                yield source + f".urls[{index + 1}]", {**value, "urls": url}
        else:
            yield source, value

    stripped = text.strip()
    entries = []
    if stripped.startswith("[") and not re.match(r"^\[[0-9a-fA-F:]+\]:", stripped):
        values = load_json(stripped)
        entries = ((f"JSON 第 {index + 1} 项", value) for index, value in enumerate(values))
    elif stripped.startswith("{"):
        # Multiple one-line node objects are accepted alongside URI lines.
        try:
            value = load_json(stripped)
        except ProxyFormatError:
            if len(stripped.splitlines()) == 1 or not stripped.splitlines()[0].rstrip().endswith("}"):
                raise
        else:
            entries = [("JSON", value)]
    if not entries:
        entries = ((f"第 {line_number} 行" + (f" · 第 {i + 1} 项" if len(parts) > 1 else ""), item)
                   for line_number, line in enumerate(text.splitlines(), 1) if line.strip()
                   for parts in [re.split(r",\s*(?=[a-zA-Z][a-zA-Z0-9+.-]*://|turns?:)", line.strip(), flags=re.I)]
                   for i, item in enumerate(parts))

    saved = set()
    for value in existing:
        try:
            saved.update(proxy_identity(uri) for uri in parse_proxy_list(value) if not validate_proxy_uri(uri))
        except (ValueError, TypeError):
            pass  # Existing editable drafts can themselves contain bad entries.
    seen, items = set(), []
    for source, value in entries:
        if isinstance(value, str) and value.lstrip().startswith("{"):
            try:
                value = load_json(value)
            except ProxyFormatError:
                pass  # The strict parser below reports this row's error.
        for location, candidate in expand(value, source):
            if len(items) >= MAX_IMPORT_ITEMS:
                raise ProxyFormatError("每次最多预览 200 条候选代理，请分批导入。")
            row = {"id": str(len(items)), "source": location, "status": "invalid", "label": "无法识别的代理"}
            ice_url = candidate.get("urls", candidate.get("url", "")) if isinstance(candidate, dict) else ""
            if isinstance(ice_url, str) and re.match(r"^stuns?:", ice_url.strip(), re.I):
                row.update(status="skipped", label="STUN 节点", message="STUN 不能转发请求，已跳过。")
                items.append(row)
                continue
            try:
                uris = parse_proxy_list(candidate, default_scheme=default_scheme)
                if len(uris) != 1:
                    raise ProxyFormatError("请填写一条完整代理地址。")
                uri = uris[0]
                error = validate_proxy_uri(uri)
                if error:
                    raise ProxyFormatError(error)
                if classify_proxy_uri(uri) == "direct":
                    row.update(status="skipped", label="强制直连", message="直连不是代理节点，请在单条添加中单独设置。")
                else:
                    identity = proxy_identity(uri)
                    fields = proxy_fields(uri)
                    row.update(url=uri, identity=identity, label=mask_proxy_uri(uri), type=fields["type"], name=fields.get("name", ""))
                    if identity in saved or identity in seen:
                        row.update(status="duplicate", message="当前列表已存在" if identity in saved else "本次输入重复")
                    else:
                        seen.add(identity)
                        row.update(status="ready", message="格式正确，尚未测试")
            except (ValueError, TypeError) as exc:
                # Validation messages are authored by the shared parser; no raw
                # input is included in the row label (it may contain passwords).
                row["message"] = str(exc) if isinstance(exc, ProxyFormatError) else "代理格式无效，请检查地址、端口和协议参数。"
            items.append(row)
    if not items:
        raise ProxyFormatError("没有识别到代理节点。")
    return {"items": items, "limit": MAX_IMPORT_ITEMS, "save_limit": 32}

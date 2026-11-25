"""Parse initial environment credentials without splitting Cookie header fields."""

import json
import re

from proxy_config import (BRIDGE_PROXY_SCHEMES, DIRECT_TOKENS, FORM_PROXY_SCHEMES,
                          SHARE_PROXY_SCHEMES)

# 内联代理识别（key,代理 / cookie,代理 写法）：scheme:// 前缀必须命中项目支持的
# 代理协议（http/socks/vless/vmess/trojan/ss/hysteria2/tuic/turn/sstp，见
# proxy_config），direct 等直连哨兵也算。API Key 与 Google Cookie 值不含这些
# 形态，凭此即可无歧义地把代理从凭证串里剥出来；无 scheme 的
# host:port:user:pass 供货商格式不参与识别（与 Key / Cookie 本身无法区分），
# 该格式请走 JSON 记录的 proxies 字段。
_PROXY_SCHEMES = FORM_PROXY_SCHEMES | BRIDGE_PROXY_SCHEMES | SHARE_PROXY_SCHEMES
_PROXY_PREFIX = re.compile(r"^([a-zA-Z][a-zA-Z0-9+.-]*)://")


def looks_like_proxy_uri(token):
    """判断一个片段是否是内联代理项（带受支持 scheme 的 URI 或 direct 哨兵）。"""
    token = str(token or "").strip()
    if not token:
        return False
    if token.lower() in DIRECT_TOKENS:
        return True
    match = _PROXY_PREFIX.match(token)
    return bool(match and match.group(1).lower() in _PROXY_SCHEMES)


def split_credential_proxy(value):
    """剥掉凭证串尾部的 ",代理" 内联代理，返回 (凭证, [代理, ...])。

    支持环境变量 key,代理 / cookie,代理 的简写：从右往左把逗号之后形如代理
    地址的片段逐个摘出（cookie,http://a:80,socks5://b:1080 摘出两段，且顺序
    与书写顺序一致）；碰到不像代理的片段立即停手，因此 Cookie 值里的普通
    逗号不受影响。Cookie-Editor 导出的 JSON 串（以 [ 开头）内部可能出现
    任意 URL 值，整体交给 parse_cookie_header，不做内联切分。
    """
    text = str(value or "").strip()
    if text.startswith("["):
        return text, []
    proxies = []
    while True:
        index = text.rfind(",")
        if index < 0:
            break
        candidate = text[index + 1:].strip()
        if not looks_like_proxy_uri(candidate):
            break
        proxies.insert(0, candidate)
        text = text[:index]
    return text.strip(), proxies


def _attach_inline_proxy(record, proxy):
    """把一个内联代理挂到前一条 Key 记录上，保留它已有的 proxies 字段。"""
    existing = record.get("proxies")
    if isinstance(existing, list):
        record["proxies"] = [*existing, proxy]
    elif existing:
        record["proxies"] = [existing, proxy]
    else:
        record["proxies"] = [proxy]


def parse_key_records(value):
    if not value:
        return []
    if isinstance(value, str):
        value = value.strip()
        value = json.loads(value) if value.startswith("[") else re.split(r"[,\r\n]+", value)
    if not isinstance(value, list):
        raise ValueError("VERTEX_EXPRESS_API_KEYS 必须是 JSON 数组或逗号 / 换行分隔的 Key。")
    records = []
    for item in value:
        if isinstance(item, dict):
            records.append(dict(item))
            continue
        text = str(item).strip()
        if not text:
            continue
        key_text, inline = split_credential_proxy(text)
        if looks_like_proxy_uri(key_text):
            # 整段是代理：逗号分隔形态的直接 token（key,代理 已在此前拆开），
            # 或 JSON 字符串数组里的 "socks5://a,socks5://b" 这类多段写法。
            # 前面没有任何 Key 时不报错、也不建账的两种例外：
            #   - direct / none / off 等直连哨兵维持旧行为当占位 Key 处理
            #     （docker 模板常用 VERTEX_EXPRESS_API_KEY=none 表示未配置）；
            #   - 带 scheme 的代理串多半是配置笔误，旧版会静默生成一把永远
            #     鉴权失败的坏 Key，这里直接报错指明写法。
            if not records:
                if key_text.lower() in DIRECT_TOKENS:
                    records.append({"key": key_text})
                    continue
                raise ValueError("VERTEX_EXPRESS_API_KEYS 的内联代理必须跟在 Key 之后（key,代理）。")
            for proxy in [*inline, key_text]:
                _attach_inline_proxy(records[-1], proxy)
            continue
        record = {"key": key_text}
        if inline:
            record["proxies"] = inline
        records.append(record)
    return records


def parse_cookie_header(value):
    value = str(value or "").strip()
    if value.startswith("["):
        items = json.loads(value)
        if not isinstance(items, list) or not all(isinstance(c, dict) and ("name" in c or "key" in c) for c in items):
            raise ValueError("Cookie-Editor 内容必须是包含 name/value 的数组。")
        return "; ".join(f"{c.get('name') or c.get('key')}={c.get('value', '')}" for c in items)
    return re.sub(r"^cookie:\s*", "", value, flags=re.I)


def parse_cookie_records(value, project_id="", project_ids=""):
    if not value:
        return []
    if isinstance(value, str):
        value = value.strip()
        if value.startswith("["):
            value = json.loads(value)
        else:
            value = [line.strip() for line in value.splitlines() if line.strip()]
    if not isinstance(value, list):
        raise ValueError("GOOGLE_COOKIES 必须是 JSON 数组或每行一个完整 Cookie。")
    # Browser exporters use either name/value or key/value for Cookie fields.
    # Both forms describe one account, including when already decoded by a caller.
    if value and all(isinstance(c, dict) and ("name" in c or "key" in c) for c in value):
        value = [parse_cookie_header(json.dumps(value))]
    projects = []
    if project_ids:
        projects = json.loads(project_ids) if project_ids.strip().startswith("[") else re.split(r"[,\r\n]+", project_ids)
        if not isinstance(projects, list) or len(projects) != len(value):
            raise ValueError("GOOGLE_PROJECT_IDS 数量必须与 GOOGLE_COOKIES 一致。")
    out = []
    for index, item in enumerate(value):
        rec = dict(item) if isinstance(item, dict) else {"cookie": item}
        # cookie,代理 简写：先剥内联代理再清洗 Cookie 头；记录已显式给出
        # proxies / proxy 字段时以内联字段为准（简写只是字符串形态的便捷写法）。
        cookie_text, inline_proxies = split_credential_proxy(rec.get("cookie", ""))
        rec["cookie"] = parse_cookie_header(cookie_text)
        if inline_proxies and "proxies" not in rec and "proxy" not in rec:
            rec["proxies"] = inline_proxies
        rec.setdefault("project_id", str(projects[index]).strip() if projects else project_id)
        if rec["cookie"]:
            out.append(rec)
    return out

"""Parse initial environment credentials without splitting Cookie header fields."""

import json
import re


def parse_key_records(value):
    if not value:
        return []
    if isinstance(value, str):
        value = value.strip()
        value = json.loads(value) if value.startswith("[") else re.split(r"[,\r\n]+", value)
    if not isinstance(value, list):
        raise ValueError("VERTEX_EXPRESS_API_KEYS 必须是 JSON 数组或逗号 / 换行分隔的 Key。")
    return [dict(item) if isinstance(item, dict) else {"key": str(item).strip()} for item in value
            if isinstance(item, dict) or str(item).strip()]


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
        rec["cookie"] = parse_cookie_header(rec.get("cookie", ""))
        rec.setdefault("project_id", str(projects[index]).strip() if projects else project_id)
        if rec["cookie"]:
            out.append(rec)
    return out

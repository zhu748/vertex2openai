#!/usr/bin/env python
"""Cookie 通道结构化输出（response_format）真机探测脚本。

目的：batchGraphql 是私有协议，generationConfig 是否接受
responseMimeType / responseSchema 无法离线验证。本脚本直连上游发三档
探测请求，输出「可开启 / 仅 JSON 模式 / 不可开启」结论，供决定是否打开
控制台的 cookie_structured_output 开关。

三档用例：
  A. json_object            —— 仅 responseMimeType，最保底的能力探测；
  B. json_schema（严格）     —— responseSchema 全链路（要求输出含必需键）；
  C. json_schema（宽松方言） —— 线上 json_patch 复现形态（value:{} 递归），
     验证 sanitize_for_gemini 净化 + UI Schema 转换后的真实可用性。

判定：
  - HTTP 非 200（签名失效/未知字段/参数被拒）→ 不可开启，保持默认拒绝；
  - 200 且 JSON 合法且必需键齐全 → 完全可开启；
  - 200 且 JSON 合法但必需键缺失 → 私有协议只认 responseMimeType、
    忽略 responseSchema（软失败，开启后仅等效 json_object）。

用法（在部署环境/仓库根目录）：
  python scripts/probe_cookie_structured_output.py [--model gemini-3.6-flash] \
      [--cookie "SAPISID=...; SID=..."] [--project-id my-project]

凭证顺序：--cookie/--project-id > 环境变量 GOOGLE_COOKIE/GOOGLE_PROJECT_ID
> 控制台已保存的 Cookie 记录（web_state.json）。
本脚本不修改任何持久化设置（cookie_structured_output 仅在进程内临时放行）。
"""

import argparse
import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app"))

# 独立进程跑探测：STATE_DIR 未设置时用临时目录，避免误碰部署的 web_state；
# 凭证显式来自 CLI/环境变量时同样成立。控制台记录模式由 --use-state 开启。
_USE_STATE = "--use-state" in sys.argv
if not _USE_STATE and "STATE_DIR" not in os.environ:
    os.environ["STATE_DIR"] = tempfile.mkdtemp(prefix="v2o-probe-")

import httpx  # noqa: E402

from cookie_auth import BATCH_GRAPHQL_URL, build_headers  # noqa: E402
from models import OpenAIRequest  # noqa: E402
from upstreams.cookie_request import build_request_body  # noqa: E402
from upstreams.cookie_response import TextSource, extract_events, iter_json_objects  # noqa: E402
from impersonate import open_upstream_client  # noqa: E402
from runtime_state import app_state  # noqa: E402


def _load_credentials(args) -> tuple[str | None, str | None]:
    if args.cookie and args.project_id:
        return args.cookie, args.project_id
    env_cookie = os.environ.get("GOOGLE_COOKIE")
    env_project = os.environ.get("GOOGLE_PROJECT_ID")
    if env_cookie and env_project:
        return env_cookie, env_project
    try:
        from credential_manager import credential_manager

        record = credential_manager.get_cookie_record()
        if record:
            return record.get("cookie"), record.get("project_id")
    except Exception as exc:  # noqa: BLE001 —— 探测脚本：任何凭证装载失败都降级为提示
        print(f"⚠️ 读取控制台 Cookie 记录失败：{exc}")
    return None, None


def _allow_structured_output():
    """进程内临时放行 cookie_structured_output（不落盘 web_state.json）。"""
    original = app_state.get_setting

    def wrapper(key, default=None):
        if key == "cookie_structured_output":
            return True
        return original(key, default)

    return mock.patch.object(app_state, "get_setting", side_effect=wrapper)


STRICT_SCHEMA = {
    "type": "object",
    "properties": {
        "name": {"type": "string", "description": "一个虚构的昵称"},
        "tags": {"type": "array", "items": {"type": "string"}, "minItems": 1},
    },
    "required": ["name", "tags"],
}


def _any_value(depth: int) -> dict:
    if depth <= 0:
        return {}
    return {"anyOf": [
        {"type": "string"}, {"type": "number"}, {"type": "boolean"},
        {"type": "object"}, {"type": "array", "items": _any_value(depth - 1)}, {"type": "null"},
    ]}


LOOSE_JSON_PATCH_SCHEMA = {
    "type": "object",
    "properties": {
        "json_patch": {
            "type": "array",
            "items": {
                "anyOf": [
                    {"type": "object",
                     "properties": {"op": {"type": "string"}, "path": {"type": "string"},
                                    "value": _any_value(3)},
                     "required": ["op", "path"]},
                    {"type": "object",
                     "properties": {"op": {"type": "string"}, "path": {"type": "string"}},
                     "required": ["op", "path"]},
                ]
            },
        }
    },
    "required": ["json_patch"],
}

CASES = [
    ("A. json_object（仅 mime 类型）",
     "请用一段 JSON 描述你自己，任意结构均可。",
     {"type": "json_object"},
     None),
    ("B. json_schema（严格 schema，必需键 name/tags）",
     "给我一个虚构的用户资料。",
     {"type": "json_schema", "json_schema": {"name": "profile", "schema": STRICT_SCHEMA}},
     ("name", "tags")),
    ("C. json_schema（宽松 json_patch 方言，经净化）",
     "为把 a.txt 的 greeting 从 hi 改成 hello 生成一个 JSON Patch。",
     {"type": "json_schema", "json_schema": {"name": "patch", "schema": LOOSE_JSON_PATCH_SCHEMA}},
     ("json_patch",)),
]


async def run_case(client, headers, project_id: str, model: str, title: str,
                   prompt: str, response_format: dict, required_keys) -> dict:
    request = OpenAIRequest(
        model=model, stream=False,
        messages=[{"role": "user", "content": prompt}],
        response_format=response_format,
    )
    with _allow_structured_output():
        body = build_request_body(project_id, model, request)
    gen_config = body["variables"]["generationConfig"]
    print(f"\n=== {title} ===")
    print(f"出站 generationConfig 键: {sorted(gen_config.keys())}")
    if "responseSchema" in gen_config:
        preview = json.dumps(gen_config["responseSchema"], ensure_ascii=False)
        print(f"responseSchema（UI Schema, 截断 400 字）: {preview[:400]}")

    response = await client.post(BATCH_GRAPHQL_URL, headers=headers, json=body)
    if response.status_code != 200:
        snippet = response.text[:500].replace("\n", " ")
        return {"level": "rejected", "detail": f"HTTP {response.status_code}: {snippet}"}

    text_parts, errors = [], []
    async for obj in iter_json_objects(TextSource(response.text)):
        for event_type, data in extract_events(obj):
            if event_type == "text":
                text_parts.append(data)
            elif event_type == "error":
                errors.append(str(data))
    if errors:
        return {"level": "rejected", "detail": f"流内错误事件: {'; '.join(errors)[:400]}"}
    text = "".join(text_parts).strip()
    if not text:
        return {"level": "rejected", "detail": "200 但无正文（可能被安全策略拦截）"}

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        return {"level": "mime_only", "detail": f"输出非 JSON（{exc}）：{text[:160]!r}"}
    if required_keys:
        missing = [key for key in required_keys if key not in (parsed or {})]
        if missing:
            return {"level": "mime_only",
                    "detail": f"JSON 合法但缺必需键 {missing}（schema 被忽略）：{text[:160]!r}"}
    return {"level": "full", "detail": text[:200]}


async def main() -> int:
    parser = argparse.ArgumentParser(description="Cookie 通道结构化输出真机探测")
    parser.add_argument("--model", default="gemini-3.6-flash", help="探测用模型名（默认 gemini-3.6-flash）")
    parser.add_argument("--cookie", help="Google Cookie 字符串（缺省读环境变量/控制台记录）")
    parser.add_argument("--project-id", help="Google Cloud Project ID")
    args = parser.parse_args()

    cookie_str, project_id = _load_credentials(args)
    if not cookie_str or not project_id:
        print("❌ 缺少凭证：请传 --cookie/--project-id，或设置 GOOGLE_COOKIE/GOOGLE_PROJECT_ID，")
        print("   或加 --use-state 读取控制台已保存的 Cookie 记录。")
        return 2
    headers = build_headers(cookie_str)
    if not headers:
        print("❌ Cookie 中未找到 SAPISID 族字段，无法计算认证头。")
        return 2

    print(f"探测模型: {args.model} | 项目: {project_id}")
    print("说明: 出站使用与生产一致的 TLS 指纹伪装（cookie_impersonate 设置）；代理需在环境内可用。")

    results = {}
    client_kwargs = {
        "timeout": httpx.Timeout(connect=30.0, read=180.0, write=30.0, pool=10.0),
        "follow_redirects": False,
        "trust_env": False,
    }
    async with open_upstream_client(client_kwargs) as client:
        for title, prompt, response_format, required_keys in CASES:
            try:
                results[title] = await run_case(
                    client, headers, project_id, args.model,
                    title, prompt, response_format, required_keys)
            except Exception as exc:  # noqa: BLE001 —— 探测脚本：异常即结果
                results[title] = {"level": "rejected", "detail": f"{type(exc).__name__}: {exc}"}

    print("\n" + "=" * 64)
    print("探测结论：")
    levels = set()
    for title, result in results.items():
        level, detail = result["level"], result["detail"]
        levels.add(level)
        icon = {"full": "✅", "mime_only": "🟡", "rejected": "❌"}[level]
        print(f"  {icon} [{level:>9}] {title}")
        print(f"     {detail}")

    print("\n建议：")
    if levels == {"full"}:
        print("  ✅ 私有协议完整接受 responseMimeType + responseSchema。")
        print("     可在控制台打开「Cookie 通道结构化输出」开关（cookie_structured_output=true）。")
        return 0
    if "rejected" in levels:
        print("  ❌ 私有协议拒绝了结构化输出字段（看上方错误形态：签名失效/未知字段/流内错误）。")
        print("     请保持开关关闭并继续使用 Express 通道；若错误形态是新签名，可抓包更新 querySignature。")
        return 1
    print("  🟡 私有协议仅接受 responseMimeType（JSON 模式），responseSchema 被忽略。")
    print("     可按需开启：开启后 json_object 全效，json_schema 仅等效 JSON 模式（无硬约束）。")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

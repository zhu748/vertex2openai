"""Claude 兼容层第三轮适配的 E2E 冒烟：405 形状 / 模型检索 / count_tokens 回退。

用 httpx.ASGITransport 直连 main.app（不占端口、不需要真实上游凭据）：
只验证 HTTP 层的协议形状，上游生成不在本脚本范围。
"""
import asyncio
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
_STATE = tempfile.TemporaryDirectory(prefix="v2o-smoke-r3-")
os.environ["STATE_DIR"] = _STATE.name

import httpx
from main import app

BASE = "http://smoke"
KEY = os.environ.get("VERTEX_API_KEY", "123456")
AUTH = {"x-api-key": KEY, "anthropic-version": "2023-06-01"}
OPENAI_AUTH = {"Authorization": f"Bearer {KEY}"}

PASS = []
FAIL = []


def check(name, condition, detail=""):
    (PASS if condition else FAIL).append(f"{name}{(' | ' + detail) if detail and not condition else ''}")
    print(("  ✅ " if condition else "  ❌ ") + name + (f" | {detail}" if detail and not condition else ""))


async def main():
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url=BASE) as client:
        print("== 方法约束 405（Anthropic 形状） ==")
        for method in ("get", "put", "delete"):
            r = await getattr(client, method)("/v1/messages")
            body = r.json()
            check(f"{method.upper()} /v1/messages → 405 + Anthropic 形状",
                  r.status_code == 405 and body.get("type") == "error"
                  and body.get("error", {}).get("type") == "invalid_request_error"
                  and str(body.get("request_id", "")).startswith("req_"),
                  f"status={r.status_code} body={body}")
        r = await client.get("/v1/messages/count_tokens")
        check("GET /v1/messages/count_tokens → 405",
              r.status_code == 405 and r.json().get("type") == "error", f"status={r.status_code}")
        r = await client.get("/api/hello")
        check("GET /api/hello → 200 ok", r.status_code == 200 and r.json() == {"status": "ok"})
        r = await client.post("/api/hello")
        check("POST /api/hello → 405", r.status_code == 405 and r.json().get("type") == "error")

        print("== GET /v1/models/{id} 双格式检索 ==")
        r = await client.get("/v1/models/claude-sonnet-4-5", headers=AUTH)
        body = r.json()
        check("Anthropic retrieve 别名 → 200 单模型对象",
              r.status_code == 200 and body.get("id") == "claude-sonnet-4-5"
              and body.get("type") == "model" and "created_at" in body,
              f"status={r.status_code} body={body}")
        r = await client.get("/v1/models/no-such-model", headers=AUTH)
        body = r.json()
        check("Anthropic retrieve 未知 → 404 not_found_error",
              r.status_code == 404 and body.get("error", {}).get("type") == "not_found_error"
              and str(body.get("request_id", "")).startswith("req_"),
              f"status={r.status_code} body={body}")
        r = await client.get("/v1/models/claude-sonnet-4-5", headers=OPENAI_AUTH)
        body = r.json()
        check("OpenAI retrieve 别名（不在 OpenAI 目录）→ 404 detail 形状",
              r.status_code == 404 and "detail" in body, f"status={r.status_code} body={body}")

        print("== count_tokens（无 Express Key → 启发式估算回退） ==")
        payload = {"model": "claude-sonnet-4-5",
                   "messages": [{"role": "user", "content": "你好，世界"}],
                   "system": "Be helpful.", "tools": [
                       {"name": "ping", "description": "ping tool",
                        "input_schema": {"type": "object", "properties": {}}}]}
        r = await client.post("/v1/messages/count_tokens", json=payload, headers=AUTH)
        body = r.json()
        check("count_tokens 估算回退 → input_tokens 为正整数",
              r.status_code == 200 and isinstance(body.get("input_tokens"), int)
              and body["input_tokens"] > 0, f"status={r.status_code} body={body}")

        print("== 回归：POST /v1/messages 仍正常路由（上游错误为 Anthropic 形状） ==")
        r = await client.post("/v1/messages", json=dict(payload, max_tokens=64), headers=AUTH)
        body = r.json()
        check("POST /v1/messages 未被 405 guard 劫持（鉴权/上游错误形状）",
              r.status_code != 405 and (body.get("type") == "error" or body.get("type") == "message"),
              f"status={r.status_code} body={body}")
        r = await client.post("/v1/messages", json=payload, headers={"x-api-key": "wrong"})
        check("错误 Key → 401 authentication_error",
              r.status_code == 401 and r.json().get("error", {}).get("type") == "authentication_error")

    print(f"\n结果：{len(PASS)} 通过 / {len(FAIL)} 失败")
    for item in FAIL:
        print("  失败项：", item)
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

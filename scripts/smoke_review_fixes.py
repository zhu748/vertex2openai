"""服务冒烟测试：验证修改后应用可正常启动、前端资源与关键端点可访问。"""

import os
import sys
import tempfile
import threading
import time
import urllib.request
import urllib.error
import json

STATE = tempfile.mkdtemp(prefix="v2o-smoke-")
os.environ["STATE_DIR"] = STATE
os.environ.setdefault("API_KEY", "smoke-test-key")
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "app"))

import uvicorn
from main import app  # noqa: E402


def main():
    config = uvicorn.Config(app, host="127.0.0.1", port=8399, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    for _ in range(60):
        time.sleep(0.5)
        try:
            urllib.request.urlopen("http://127.0.0.1:8399/healthz", timeout=3)
            break
        except urllib.error.URLError:
            continue
    else:
        raise SystemExit("服务未能在 30 秒内启动")

    checks = []

    # 管理控制台使用 cookie session 鉴权：先登录拿会话
    import http.client
    conn = http.client.HTTPConnection("127.0.0.1", 8399, timeout=10)
    conn.request("POST", "/api/login", body=json.dumps({"password": "smoke-test-key"}),
                 headers={"Content-Type": "application/json"})
    login_resp = conn.getresponse()
    login_body = login_resp.read()
    set_cookie = login_resp.getheader("Set-Cookie") or ""
    login_resp.read()
    conn.close()
    session_cookie = set_cookie.split(";")[0] if "v2o_session" in set_cookie else set_cookie.split(";")[0]
    assert login_resp.status == 200 and session_cookie, f"登录失败：{login_resp.status} {login_body[:200]}"

    def get(path, expect=200, token="smoke-test-key"):
        req = urllib.request.Request(f"http://127.0.0.1:8399{path}")
        if token == "session":
            req.add_header("Cookie", session_cookie)
        elif token:
            req.add_header("Authorization", f"Bearer {token}")
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                body = resp.read()
                return resp.status == expect, f"{path} -> {resp.status} ({len(body)}B)"
        except urllib.error.HTTPError as exc:
            return exc.code == expect, f"{path} -> {exc.code}（预期 {expect}）"

    checks.append(get("/healthz", token=None))
    checks.append(get("/api/cookie-accounts", 401, token=None))     # 未登录应 401
    checks.append(get("/api/cookie-accounts", token="session"))     # 登录后 200
    # 前端资源（本轮修改的文件）
    checks.append(get("/static/channel-config.js", token=None))
    checks.append(get("/static/dashboard.js", token=None))
    # Cookie 测试端点存在性（无账号 → 404 语义正确）
    req = urllib.request.Request("http://127.0.0.1:8399/api/cookie-accounts/test",
                                 method="POST", data=b"{}",
                                 headers={"Content-Type": "application/json",
                                          "Cookie": session_cookie})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            checks.append((resp.status == 404, "POST /api/cookie-accounts/test -> 404（无账号，符合预期）"))
    except urllib.error.HTTPError as exc:
        checks.append((exc.code == 404, f"POST /api/cookie-accounts/test -> {exc.code}（预期 404）"))

    server.should_exit = True
    thread.join(timeout=10)

    failed = [msg for ok, msg in checks if not ok]
    for ok, msg in checks:
        print(("✅ " if ok else "❌ ") + msg)
    if failed:
        raise SystemExit(f"{len(failed)} 项检查失败")
    print("冒烟测试全部通过")


if __name__ == "__main__":
    main()

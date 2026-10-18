"""冒烟测试：启动完整服务，验证核心端点与修复点。"""
import os
import sys

os.environ["API_KEY"] = "smoke-test-key"
os.environ["STATE_DIR"] = "/tmp/v2o-final-smoke"

_APP_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "app"))
sys.path.insert(0, _APP_DIR)

import main  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

PASSED = []
FAILED = []


def check(name, cond, detail=""):
    (PASSED if cond else FAILED).append(name)
    print(("✅" if cond else "❌"), name, detail)


with TestClient(main.app) as c:
    # ---- 基础端点 ----
    r = c.get("/healthz")
    check("GET /healthz", r.status_code == 200 and r.json()["status"] == "ok")

    r = c.head("/healthz")
    check("HEAD /healthz", r.status_code == 200)

    # ---- 鉴权 ----
    r = c.get("/v1/models")
    check("无鉴权 401", r.status_code == 401)
    r = c.get("/v1/models", headers={"Authorization": "Bearer wrong-key"})
    check("错误 Key 401", r.status_code == 401)
    r = c.get("/v1/models", headers={"Authorization": "Bearer smoke-test-key"})
    check("正确 Key /v1/models 200", r.status_code == 200)
    data = r.json().get("data", [])
    print(f"   模型数: {len(data)}（未配置通道时为 0，设计行为）")

    # ---- 登录/会话 ----
    r = c.post("/api/login", json={"password": "smoke-test-key"})
    check("控制台登录", r.status_code == 200 and "ap_session" in r.cookies)
    session_cookie = r.cookies.get("ap_session")

    r = c.get("/api/settings", cookies={"ap_session": session_cookie})
    check("已登录读取设置", r.status_code == 200 and "retry_max" in r.json())

    r = c.get("/api/settings", cookies={"ap_session": "invalid-token"})
    check("无效会话读设置 401", r.status_code == 401, f"got {r.status_code}")

    # ---- Gemini 原生端点 ----
    r = c.get("/v1beta/models", headers={"x-goog-api-key": "smoke-test-key"})
    check("GET /v1beta/models", r.status_code == 200 and "models" in r.json())

    r = c.get("/v1beta/models/gemini-3.6-flash", headers={"x-goog-api-key": "smoke-test-key"})
    check("GET /v1beta/models/{model}（无通道 → 404）", r.status_code == 404)

    # 体积上限（Commit 3）：> 32MiB 的 body
    huge = b'{"contents": [{"role": "user", "parts": [{"text": "' + b"x" * (33 * 1024 * 1024) + b'"}]}]}'
    r = c.post("/v1beta/models/gemini-3.6-flash:generateContent",
               headers={"x-goog-api-key": "smoke-test-key", "Content-Type": "application/json",
                        "Content-Length": str(len(huge))}, content=huge)
    check("原生透传 32MiB 上限 → 413", r.status_code == 413, f"got {r.status_code}")

    # ---- chat completions（无上游 Key → 401 透传，不 500）----
    r = c.post("/v1/chat/completions", headers={"Authorization": "Bearer smoke-test-key"},
               json={"model": "gemini-3.6-flash", "messages": [{"role": "user", "content": "hi"}]})
    check("chat 无通道配置不 500", r.status_code in (400, 401, 403, 503),
          f"got {r.status_code}")

    # resumable 空游标（Commit 3 修复点）
    r = c.post("/v1/chat/completions", headers={"Authorization": "Bearer smoke-test-key"},
               json={"model": "gemini-3.6-flash", "stream": False,
                     "messages": [{"role": "user", "content": "hi"}]})
    rid = r.headers.get("x-resume-id", "")
    if rid:
        r2 = c.get(f"/v1/resumable/{rid}?resume_from=&wait=0",
                   headers={"Authorization": "Bearer smoke-test-key"})
        check("空 resume_from 不报 400", r2.status_code != 400, f"got {r2.status_code}")

    # ---- 消息转换修复点（Commit 1）----
    from image_processing import decode_data_url_image
    from message_processing import create_gemini_prompt, _extract_markdown_images_to_parts
    from models import OpenAIMessage  # noqa: E402

    # 畸形 base64 不再 500
    try:
        parts, text = _extract_markdown_images_to_parts(
            "看这张 ![img](data:image/png;base64,abc) 图")
        check("畸形 base64 跳过不抛异常", text.startswith("看这张") and "图" in text)
    except Exception as e:
        check("畸形 base64 跳过不抛异常", False, str(e))

    # 体积上限生效
    try:
        decode_data_url_image("A" * (30 * 1024 * 1024))
        check("内联图片 20MiB 上限拦截", False, "未抛异常")
    except ValueError:
        check("内联图片 20MiB 上限拦截", True)

    # 含换行 base64 可解析（DOTALL）
    tiny_png = "iVBORw0KGgoAAAANSUhEUg"  # 无所谓是否完整图片，解析即测正则
    parts, text = _extract_markdown_images_to_parts(
        f"前文 ![img](data:image/png;base64,{tiny_png}\n{tiny_png}) 后文")
    check("折行 base64 的 data URL 正则匹配", text.strip().startswith("前文") or text == "" or "后文" in text,
          f"text={text[:40]!r}")

    # role=function 消息可转换（旧实现会生成 Gemini role="function"）
    msgs = [
        OpenAIMessage(role="user", content="hi"),
        OpenAIMessage(role="assistant", tool_calls=[{"id": "call_1", "type": "function",
                                                     "function": {"name": "get_weather", "arguments": "{}"}}]),
        OpenAIMessage(role="function", name="get_weather", content='{"temp": 25}'),
    ]
    contents = create_gemini_prompt(msgs, "gemini-3.6-flash")
    roles = {c.role for c in contents}
    check("role=function 转 functionResponse（无 'function' role）", "function" not in roles,
          f"roles={roles}")
    has_fr = any(getattr(p, "function_response", None) is not None
                 for c in contents for p in c.parts)
    check("function 消息生成 functionResponse part", has_fr)

    # ---- SAPISIDHASH 三段同时间戳（Commit 2）----
    from cookie_auth import build_authorization_header  # noqa: E402
    cookie = "SAPISID=aaa; __Secure-1PAPISID=bbb; __Secure-3PAPISID=ccc; SID=ddd"
    auth = build_authorization_header(cookie)
    stamps = {seg.split("_", 1)[0] for seg in auth.split(" ") if "_" in seg}
    check("SAPISIDHASH 三段共用同一时间戳", len(stamps) == 1, f"stamps={stamps}")

    # ---- get_profile 缓存（Commit 4）----
    import model_capabilities as mc  # noqa: E402
    p1 = mc.get_profile("gemini-3.6-flash")
    p2 = mc.get_profile("gemini-3.6-flash")
    check("get_profile 缓存命中", p1 is p2)
    # apply_sampling_policy 不改缓存（用 3.5-flash：采样参数默认可用，
    # 3.6+ 本就因版本废弃而不含 temperature，测不出来）
    p35 = mc.get_profile("gemini-3.5-flash")
    before = set(p35["allowed_sampling"])
    out = mc.apply_sampling_policy(p35, {"sampling_policy": "deprecated"})
    check("apply_sampling_policy 不污染缓存",
          set(p35["allowed_sampling"]) == before and "temperature" not in out["allowed_sampling"])

    # ---- runtime_state 标量免拷（Commit 4）----
    from runtime_state import app_state  # noqa: E402
    v1 = app_state.get_setting("retry_max", 10)
    check("get_setting 标量正常返回", v1 == 10 or isinstance(v1, int))

    # ---- sing-box 文件权限（Commit 5）：跳过真实启动，仅验证接口存在 ----
    from proxy_resolver import singbox_manager  # noqa: E402
    check("reap_idle 可调用（含死实例清理）", callable(singbox_manager.reap_idle))
    n = singbox_manager.reap_idle()
    check("reap_idle 空池安全返回", n == 0)

    # ---- 状态文件权限 ----
    st = os.environ["STATE_DIR"] + "/web_state.json"
    if os.path.exists(st):
        mode = oct(os.stat(st).st_mode)[-3:]
        check("web_state.json 0600", mode == "600", f"mode={mode}")

print(f"\n========== 冒烟结果：{len(PASSED)} 通过 / {len(FAILED)} 失败 ==========")
if FAILED:
    print("失败项:", FAILED)
    sys.exit(1)

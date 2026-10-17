"""Express SDK 主通道指纹探针：四个 UA 档案逐字节对照官方 SDK。

上轮 probe_fingerprint_diff.py 已验证：官方 SDK vs 本项目裸 httpx 出站点
（透传/Key探测）完全一致。但 SDK 主通道注入 node/gcloud/custom 档案后的
最终线序从未实测——本脚本补上这块盲区，并检测「双重标签」混合形态。

构造方式与 app/client_pool.py 完全一致：
    genai.Client(vertexai=True, api_key=..., http_options=get_http_options())
仅在 async_client_args 注入 MockTransport（SDK 原生支持），不发真实网络。

运行：/home/z/my-project/venv/bin/python /home/z/my-project/scripts/probe_sdk_channel_profiles.py
"""

import asyncio
import sys
from pathlib import Path

APP_DIR = Path("/home/z/my-project/vertex2openai/app")
sys.path.insert(0, str(APP_DIR))

import httpx
from httpx import MockTransport
from google import genai
from google.genai import types


def raw_headers(req: httpx.Request) -> list:
    return [(k.decode("latin-1"), v.decode("latin-1")) for k, v in req.headers.raw]


def make_capture() -> tuple:
    box = {"headers": None, "url": None, "body": None}

    async def handler(request: httpx.Request) -> httpx.Response:
        box["headers"] = raw_headers(request)
        box["url"] = str(request.url)
        box["body"] = request.content
        return httpx.Response(200, json={"candidates": []})

    return box, MockTransport(handler)


async def capture_sdk_channel(profile: str, custom_ua: str = "") -> dict:
    """按 client_pool.py 的真实构造捕获 SDK 主通道出站形态（指定档案）。"""
    from runtime_state import app_state
    from http_options import get_http_options

    app_state.update_settings({"express_ua_profile": profile})
    if custom_ua:
        app_state.update_settings({"express_ua_custom": custom_ua})

    box, transport = make_capture()
    opts = get_http_options()
    opts = opts.model_copy() if opts is not None else types.HttpOptions()
    opts.async_client_args = {"transport": transport}
    client = genai.Client(vertexai=True, api_key="TESTKEY", http_options=opts)
    try:
        await client.aio.models.generate_content(model="gemini-2.0-flash", contents="hi")
    except Exception:
        pass
    finally:
        try:
            await client.aio.aclose()
        except AttributeError:
            client.close()
    return box


def strip_volatile(headers: list) -> list:
    """剔除传输层派生头（host/content-length 随载荷与目标主机变化，非指纹项）。"""
    return [(k, v) for k, v in headers if k.lower() not in ("host", "content-length")]


def show_and_check(label: str, box: dict, baseline: list) -> None:
    print(f"\n===== SDK 主通道 [{label}] =====")
    print("URL:", box["url"])
    hdrs = strip_volatile(box["headers"])
    for k, v in hdrs:
        print(f"  {k}: {v[:100]}")
    names_a = [k.lower() for k, _ in strip_volatile(baseline)]
    names_b = [k.lower() for k, _ in hdrs]
    order_ok = names_a == names_b
    print(f"  线序 vs 官方默认: {'✅ 一致' if order_ok else '❌ 不同'}")
    if not order_ok:
        print(f"    官方: {names_a}\n    本档: {names_b}")
    # 大小写对照
    case_a = [k for k, _ in strip_volatile(baseline)]
    case_b = [k for k, _ in hdrs]
    print(f"  大小写: {'✅ 一致' if case_a == case_b else f'❌ 不同 {case_a} vs {case_b}'}")
    # 双重标签检测：同一标签在 UA 里出现两次 = 扎眼异常
    d = {k.lower(): v for k, v in hdrs}
    for h in ("user-agent", "x-goog-api-client"):
        ua = d.get(h, "")
        issues = []
        if ua.count("google-genai-sdk/") > 1:
            issues.append("google-genai-sdk 重复")
        if ua.count("gl-python/") > 1:
            issues.append("gl-python 重复")
        if ua.count("gl-node/") > 1:
            issues.append("gl-node 重复")
        if "gl-node/" in ua and "gl-python/" in ua:
            issues.append("⚠️ gl-node+gl-python 混合（真实客户端不可能的形态）")
        print(f"  {h} 标签体检: {'✅ 无异常' if not issues else ' / '.join(issues)}")
        if h == "user-agent" and ua:
            print(f"    值: {ua}")


async def main() -> None:
    from runtime_state import app_state

    # 基准：官方 SDK 默认形态（不注入任何头）
    box0, t0 = make_capture()
    c0 = genai.Client(
        vertexai=True, api_key="TESTKEY",
        http_options=types.HttpOptions(async_client_args={"transport": t0}),
    )
    try:
        await c0.aio.models.generate_content(model="gemini-2.0-flash", contents="hi")
    except Exception:
        pass
    finally:
        try:
            await c0.aio.aclose()
        except AttributeError:
            c0.close()
    print("===== 基准：官方 google-genai SDK 默认形态 =====")
    print("URL:", box0["url"])
    for k, v in strip_volatile(box0["headers"]):
        print(f"  {k}: {v[:100]}")
    baseline = box0["headers"]

    # 四档案逐一对照
    for profile in ("sdk", "node", "gcloud"):
        box = await capture_sdk_channel(profile)
        show_and_check(profile, box, baseline)
    # custom 无占位符：真实 SDK 对自定义 UA 的变换 = 两标签整体前置
    box = await capture_sdk_channel("custom", custom_ua="my-tool/1.0")
    show_and_check("custom=my-tool/1.0（无占位符）", box, baseline)
    # custom 带占位符（{node} 展开后缺 sdk 标签 → 按真实 SDK 变换前置 sdk+python）
    box = await capture_sdk_channel("custom", custom_ua="my-tool/1.0 {node}")
    show_and_check("custom=my-tool/1.0 {node}", box, baseline)
    # custom 齐活占位符：SDK 应原样放行
    box = await capture_sdk_channel("custom", custom_ua="{sdk} {python} mycorp-gateway/1.0")
    show_and_check("custom={sdk} {python} mycorp-gateway/1.0", box, baseline)

    # 断言区：修复后的期望行为
    from runtime_state import app_state
    from express_fingerprint import (build_sdk_channel_overrides,
                                     build_direct_headers, _build_ua,
                                     sdk_label, python_label, node_label)
    print("\n===== 断言：档案注入策略（修复后） =====")
    ok = True

    def check(name, cond, detail=""):
        nonlocal ok
        print(f"  [{'✅' if cond else '❌'}] {name}" + (f"  {detail}" if detail else ""))
        if not cond:
            ok = False

    app_state.update_settings({"express_ua_profile": "node"})
    check("node 档案 SDK 主通道不注入（纯 python）", build_sdk_channel_overrides() == {})
    dh = build_direct_headers("K")
    check("node 档案裸出站点 = 纯 node 形态",
          dh["user-agent"] == f"{sdk_label()} {node_label()}"
          and "gl-python" not in dh["user-agent"],
          dh["user-agent"])

    app_state.update_settings({"express_ua_profile": "custom", "express_ua_custom": "my-tool/1.0"})
    ua = build_sdk_channel_overrides().get("user-agent", "")
    expect = f"{sdk_label()} {python_label()} my-tool/1.0"
    check("custom 无占位符 = 真实 SDK 变换（sdk+python 整体前置）", ua == expect, ua)

    app_state.update_settings({"express_ua_custom": "my-tool/1.0 {node}"})
    ua2 = build_sdk_channel_overrides().get("user-agent", "")
    expect2 = f"{sdk_label()} {python_label()} my-tool/1.0 {node_label()}"
    check("custom 含 {node} = 真实 SDK 变换（两标签前置 + node 在后）", ua2 == expect2, ua2)

    app_state.update_settings({"express_ua_custom": "{sdk} {python} mycorp-gateway/1.0"})
    ua3 = build_sdk_channel_overrides().get("user-agent", "")
    check("custom 占位符齐活 = 原样放行", ua3 == f"{sdk_label()} {python_label()} mycorp-gateway/1.0", ua3)

    app_state.update_settings({"express_ua_profile": "gcloud"})
    ua4 = build_sdk_channel_overrides().get("user-agent", "")
    check("gcloud 档案标签齐全无追加", ua4 == _build_ua("gcloud") and "gl-node" not in ua4, ua4[:90])

    # 收尾：恢复默认档案
    app_state.update_settings({"express_ua_profile": "sdk", "express_ua_custom": ""})
    print("\n结论：", "全部通过 ✅" if ok else "存在失败项 ❌")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    asyncio.run(main())

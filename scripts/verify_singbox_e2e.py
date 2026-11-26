"""sing-box 多协议代理端到端验证（手动运行，需外网或本地测试目标）。

用法：
    python scripts/verify_singbox_e2e.py "vless://uuid@host:port?..." [target_url]
    python scripts/verify_singbox_e2e.py "socks4://1.2.3.4:1080"

验证链路：分享 URI → proxy_resolver 解析 → sing-box 子进程（本地 socks5 入站）
→ httpx 经本地 socks5 请求目标 URL（默认 aiplatform.googleapis.com）。
同时验证：同 URI 端口稳定复用、sing-box 缺失时的明确报错（子进程隔离验证）、
进程被杀且原端口被占时自动换新端口复活。原生代理只验证稳定解析和实际请求。
退出码 0 = 适用场景全部通过；非 0 = 失败（原因见输出）。
"""
import asyncio
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import proxy_resolver as pr  # noqa: E402
from proxy_config import normalize_proxy_uri  # noqa: E402

DEFAULT_TARGET = "https://aiplatform.googleapis.com/"


async def _verify_revival(uri: str, target: str) -> bool:
    # Only called for a sing-box URI, normalized exactly as the manager key.
    inst = pr.singbox_manager._instances.get(uri)
    if inst is None or not inst.alive():
        print("❌ 无法验证崩溃复活：已转换的 URI 没有运行中的 sing-box 实例。")
        return False
    old_port = inst.port
    inst.process.kill()
    inst.process.wait(timeout=5)
    # 用一个哑监听占住原端口，模拟「端口被其他进程抢走」
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as squatter:
        squatter.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        squatter.bind(("127.0.0.1", old_port))
        squatter.listen(1)
        started = time.monotonic()
        revived = await pr.resolve_proxy_uri_async(uri)
        replacement = pr.singbox_manager._instances.get(uri)
        if (replacement is None or not replacement.alive() or replacement.port == old_port
                or revived != f"socks5://127.0.0.1:{replacement.port}"):
            print(f"❌ 复活未提供新的本地 sing-box 端口（原端口 {old_port} 已被占）。")
            return False
        print(f"✅ 崩溃复活：端口 {old_port}（被占）→ {replacement.port}（{time.monotonic() - started:.2f}s）")
        async with httpx.AsyncClient(proxy=revived, timeout=httpx.Timeout(15.0)) as client:
            resp = await client.get(target)
        print(f"✅ 复活后经代理请求 → HTTP {resp.status_code}")
    return True


def _verify_live_route(uri: str, target: str, converted: bool) -> bool:
    started = time.monotonic()
    local = pr.resolve_proxy_uri(uri)
    print(f"✅ 已解析：{pr.mask_proxy_uri(uri)}（耗时 {time.monotonic() - started:.2f}s）")
    if pr.resolve_proxy_uri(uri) != local:
        print("❌ 同一 URI 两次解析结果不一致。")
        return False
    print("✅ 代理地址稳定复用。")

    started = time.monotonic()
    with httpx.Client(proxy=local, timeout=httpx.Timeout(12.0, connect=12.0),
                      follow_redirects=False) as client:
        resp = client.get(target)
    ms = int((time.monotonic() - started) * 1000)
    print(f"✅ 经代理请求 {target} → HTTP {resp.status_code}（{ms} ms；任意状态码都代表链路可达）")
    if converted:
        return asyncio.run(_verify_revival(uri, target))
    print("ℹ️ 原生代理无需 sing-box；崩溃复活和引擎缺失检查不适用。")
    return True


def _verify_missing_binary(uri: str) -> bool:
    # An invalid SINGBOX_BIN alone does not simulate absence: discovery can
    # still find the installed executable through its standard paths or PATH.
    child_code = "\n".join([
        "import sys",
        "from unittest.mock import patch",
        f"sys.path.insert(0, {str(Path(__file__).resolve().parents[1] / 'app')!r})",
        "import proxy_resolver as pr",
        "with patch.object(pr, 'find_singbox_binary', return_value=None):",
        "    try:",
        "        pr.resolve_proxy_uri(sys.stdin.read())",
        "    except pr.ProxyResolveError as e:",
        "        if 'sing-box' not in str(e): raise",
        "        print('OK: 未发现 sing-box 时明确报错'); sys.exit(0)",
        "    sys.exit('BUG: returned without error')",
    ])
    with tempfile.TemporaryDirectory(prefix="v2o-e2e-") as state_dir:
        child = subprocess.run([sys.executable, "-c", child_code], input=uri,
                               capture_output=True, text=True, timeout=60,
                               env={**os.environ, "STATE_DIR": state_dir})
    print(child.stdout.strip())
    if child.returncode != 0:
        print(f"❌ 缺失降级语义验证失败：{child.stderr}")
        return False
    return True


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    raw_uri = sys.argv[1].strip()
    target = sys.argv[2].strip() if len(sys.argv) > 2 else DEFAULT_TARGET
    kind = pr.classify_proxy_uri(raw_uri)
    if kind not in {"singbox", "native"}:
        print("❌ 本脚本只验证 sing-box 转换型 URI 或原生代理。")
        return 2
    uri = normalize_proxy_uri(raw_uri)
    converted = kind == "singbox"
    if converted:
        binary = pr.find_singbox_binary()
        if not binary:
            print("❌ 未找到 sing-box（设 SINGBOX_BIN 指向可执行文件后重试）。")
            return 1
        print(f"✅ sing-box：{binary}（{pr.singbox_version()}）")

    try:
        if not _verify_live_route(uri, target, converted):
            return 1
    except (pr.ProxyResolveError, httpx.HTTPError, OSError, subprocess.TimeoutExpired) as exc:
        print(f"❌ 代理验证失败：{type(exc).__name__}: {exc}")
        return 1
    finally:
        if converted:
            pr.singbox_manager.shutdown_all()

    if converted and not _verify_missing_binary(uri):
        return 1
    print("\n🎉 端到端验证全部通过。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

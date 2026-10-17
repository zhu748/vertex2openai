"""sing-box 多协议代理端到端验证（手动运行，需外网）。

用法：
    python scripts/verify_singbox_e2e.py "vless://uuid@host:port?..." [target_url]
    python scripts/verify_singbox_e2e.py "socks4://1.2.3.4:1080"

验证链路：分享 URI → proxy_resolver 解析 → sing-box 子进程（本地 socks5 入站）
→ httpx 经本地 socks5 请求目标 URL（默认 aiplatform.googleapis.com）。
同时验证：同 URI 端口稳定复用、sing-box 缺失时的明确报错（子进程隔离验证）。
退出码 0 = 全部通过；非 0 = 失败（原因见输出）。
"""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

DEFAULT_TARGET = "https://aiplatform.googleapis.com/"


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    uri = sys.argv[1].strip()
    target = sys.argv[2].strip() if len(sys.argv) > 2 else DEFAULT_TARGET

    import proxy_resolver as pr
    from proxy_resolver import ProxyResolveError, mask_proxy_uri

    # ---------- 1. sing-box 可用性 ----------
    binary = pr.find_singbox_binary()
    if not binary:
        print("❌ 未找到 sing-box（设 SINGBOX_BIN 指向可执行文件后重试）。")
        return 1
    print(f"✅ sing-box：{binary}（{pr.singbox_version()}）")

    # ---------- 2. 解析 + 启动 ----------
    started = time.time()
    try:
        local = pr.resolve_proxy_uri(uri)
    except ProxyResolveError as e:
        print(f"❌ 解析/转换失败：{e}")
        return 1
    if not local.startswith("socks5://127.0.0.1:"):
        print(f"ℹ️ 该 URI 无需 sing-box（原生协议），解析结果：{mask_proxy_uri(uri)}")
        local = local
    print(f"✅ 已转换：{mask_proxy_uri(uri)} → {local}（耗时 {time.time() - started:.2f}s）")

    # ---------- 3. 端口稳定复用 ----------
    again = pr.resolve_proxy_uri(uri)
    if again != local:
        print(f"❌ 端口不稳定：{local} vs {again}")
        return 1
    print(f"✅ 端口稳定复用：{again}")

    # ---------- 4. 经代理请求目标 ----------
    import httpx
    t0 = time.time()
    try:
        with httpx.Client(proxy=local, timeout=httpx.Timeout(12.0, connect=12.0),
                          follow_redirects=False) as client:
            resp = client.get(target)
        ms = int((time.time() - t0) * 1000)
        print(f"✅ 经代理请求 {target} → HTTP {resp.status_code}（{ms} ms；"
              f"任意状态码都代表链路可达）")
    except Exception as e:
        print(f"❌ 经代理请求失败：{type(e).__name__}: {e}")
        return 1
    finally:
        pr.singbox_manager.shutdown_all()

    # ---------- 5. sing-box 缺失时明确报错（子进程验证，不污染本进程缓存） ----------
    child_code = "\n".join([
        "import sys, os",
        f"sys.path.insert(0, {str(Path(__file__).resolve().parents[1] / 'app')!r})",
        "os.environ['SINGBOX_BIN'] = '/nonexistent/sing-box'",
        "import proxy_resolver as pr",
        "try:",
        f"    pr.resolve_proxy_uri({uri!r})",
        "    print('BUG: returned without error'); sys.exit(1)",
        "except pr.ProxyResolveError as e:",
        "    assert 'sing-box' in str(e)",
        "    print('OK: 缺失时明确报错 —— ' + str(e)[:80] + '…'); sys.exit(0)",
    ])
    child = subprocess.run([sys.executable, "-c", child_code],
                           capture_output=True, text=True, timeout=60,
                           env={**os.environ, "STATE_DIR": tempfile.mkdtemp(prefix="v2o-e2e-")})
    print(child.stdout.strip())
    if child.returncode != 0:
        print(f"❌ 缺失降级语义验证失败：{child.stdout} {child.stderr}")
        return 1

    print("\n🎉 端到端验证全部通过。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

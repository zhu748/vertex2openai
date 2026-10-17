"""第十七轮修复的端到端验证（手动运行，需外网 + sing-box）。

用法：
    SINGBOX_BIN=/path/to/sing-box python scripts/verify_round17.py "vless://..."

覆盖：
  1. resolve_proxy_uri_async 与同步版结果一致（同端口）；
  2. 真实请求经本地 socks5 链路可达；
  3. sing-box 进程被杀 + 原端口被占 → 自动换新端口复活（本轮修复）；
  4. 崩溃且换端口仍失败 → 死条目清理、错误上抛（用假二进制模拟）。
"""
import asyncio
import os
from pathlib import Path
import socket
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

STATE_DIR = tempfile.mkdtemp(prefix="v2o-r17-")
os.environ.setdefault("STATE_DIR", STATE_DIR)


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    uri = sys.argv[1].strip()

    import httpx
    import proxy_resolver as pr
    from proxy_resolver import ProxyResolveError, mask_proxy_uri

    binary = pr.find_singbox_binary()
    if not binary:
        print("❌ 未找到 sing-box（设 SINGBOX_BIN 后重试）。")
        return 1
    print(f"✅ sing-box：{binary}（{pr.singbox_version()}）")

    async def scenario() -> int:
        # ---------- 1. 异步 resolve 与同步版一致 ----------
        local = await pr.resolve_proxy_uri_async(uri)
        if not local.startswith("socks5://127.0.0.1:"):
            print(f"❌ 异步 resolve 结果异常：{local}")
            return 1
        again = pr.resolve_proxy_uri(uri)          # 同步版应命中同一端口
        if again != local:
            print(f"❌ 异步/同步端口不一致：{local} vs {again}")
            return 1
        print(f"✅ 异步 resolve 一致：{mask_proxy_uri(uri)} → {local}")

        # ---------- 2. 经本地 socks5 真实请求 ----------
        port = int(local.rsplit(":", 1)[1])
        try:
            async with httpx.AsyncClient(proxy=local, timeout=httpx.Timeout(15.0)) as client:
                resp = await client.get("https://aiplatform.googleapis.com/")
            print(f"✅ 经代理请求 → HTTP {resp.status_code}（任意状态码都代表链路可达）")
        except Exception as e:
            print(f"❌ 经代理请求失败：{type(e).__name__}: {e}")
            return 1

        # ---------- 3. 杀进程 + 占端口 → 换新端口复活 ----------
        inst = pr.singbox_manager._instances.get(uri)
        if inst is None or not inst.alive():
            print("❌ 前置失败：实例不在运行。")
            return 1
        old_port = inst.port
        inst.process.kill()
        inst.process.wait(timeout=5)
        # 用一个哑监听占住原端口，模拟「端口被其他进程抢走」
        squatter = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        squatter.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        squatter.bind(("127.0.0.1", old_port))
        squatter.listen(1)
        try:
            t0 = time.time()
            revived = await pr.resolve_proxy_uri_async(uri)
            new_port = int(revived.rsplit(":", 1)[1])
            if new_port == old_port:
                print(f"❌ 复活仍用被占端口 {old_port}")
                return 1
            print(f"✅ 崩溃复活：端口 {old_port}（被占）→ {new_port}（{time.time() - t0:.2f}s）")
            async with httpx.AsyncClient(proxy=revived, timeout=httpx.Timeout(15.0)) as client:
                resp = await client.get("https://aiplatform.googleapis.com/")
            print(f"✅ 复活后经代理请求 → HTTP {resp.status_code}")
        except ProxyResolveError as e:
            print(f"❌ 崩溃复活失败：{e}")
            return 1
        finally:
            squatter.close()

        # ---------- 4. 全失败路径：死条目必须被清掉 ----------
        mgr = pr.SingboxManager()
        fake_uri = "vless://deadbeef-0000-0000-0000-000000000000@127.0.0.1:9"
        mgr._instances[fake_uri] = type(inst)(fake_uri, 12345, None, "", "")
        broken = pr.ProxyResolveError("模拟失败")

        def _fail(u, p):
            raise broken

        mgr._launch = _fail               # type: ignore[method-assign]
        try:
            mgr.get_local_socks5(fake_uri)
            print("❌ 全失败路径未抛错")
            return 1
        except ProxyResolveError:
            ok = fake_uri not in mgr._instances
            print(f"{'✅' if ok else '❌'} 全失败后死条目{'已清理' if ok else '仍在'}")
            return 0 if ok else 1

    try:
        return asyncio.run(scenario())
    finally:
        pr.singbox_manager.shutdown_all()


if __name__ == "__main__":
    raise SystemExit(main())

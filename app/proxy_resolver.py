"""Resolve proxy routes and own the lifecycle of local conversion processes.

URI parsing, validation and redaction live in proxies.protocols; the existing
public helper imports remain available here for callers. Native proxies pass
through unchanged. sing-box and TURN/SSTP routes use retained, reusable local
SOCKS listeners, and conversion failure never falls back to direct access.
"""

from __future__ import annotations

import asyncio
import atexit
import glob
import hashlib
import json
import os
import shutil
import socket
import subprocess
import threading
import time
import weakref
from contextvars import ContextVar
from typing import Optional

from proxies.protocols import (
    ALL_PROXY_SCHEMES,
    _uri_scheme,
)
from proxies.protocols import (
    ProxyResolveError as ProxyResolveError,
)
from proxies.protocols import (
    classify_proxy_uri as classify_proxy_uri,
)
from proxies.protocols import (
    mask_proxy_uri as mask_proxy_uri,
)
from proxies.protocols import (
    parse_share_uri_to_outbound as parse_share_uri_to_outbound,
)
from proxies.protocols import (
    validate_proxy_uri as validate_proxy_uri,
)
from proxy_config import (
    DIRECT_TOKENS,
    ProxyFormatError,
    build_proxy_uri,
    normalize_proxy_uri,
    parse_bridge_proxy_uri,
)

current_proxy_usage = ContextVar("current_proxy_usage", default=None)


class ProxyUsage:
    """Keep transports/proxy listeners alive until their request scope closes."""

    def __init__(self, *, temporary=False):
        self._entries = set()
        self._temporary_entries = set()
        self._temporary = temporary
        self._closed = False
        self._lock = threading.Lock()

    @property
    def closed(self):
        with self._lock:
            return self._closed

    def include(self, manager, uri):
        with self._lock:
            entry = (manager, uri)
            if self._closed or entry in self._entries:
                return
            manager.retain(uri, temporary=self._temporary)
            self._entries.add(entry)
            if self._temporary:
                self._temporary_entries.add(entry)

    def close(self):
        with self._lock:
            if self._closed:
                return
            self._closed = True
            for manager, uri in self._entries:
                manager.release(uri)
            self._entries.clear()
        # Probe callers close in a worker: process shutdown can wait. Only
        # retire newly created, unused listeners; never interrupt a generation.
        for manager, uri in self._temporary_entries:
            manager.discard_unused(uri)
        self._temporary_entries.clear()


# ============================================================
# sing-box 二进制探测
# ============================================================

_SINGBOX_CANDIDATE_PATHS = (
    "/usr/local/bin/sing-box",
    "/usr/bin/sing-box",
    "/opt/sing-box/sing-box",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "bin", "sing-box"),
    # 仓库根 bin/（非 Docker 部署时把二进制放进项目根 bin/ 的约定位置）
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "bin", "sing-box"
    ),
)


def find_singbox_binary() -> Optional[str]:
    """按优先级探测 sing-box：环境变量 SINGBOX_BIN → 常见路径 → PATH。

    返回可执行文件绝对路径，找不到返回 None。结果做进程内缓存
    （None=未探测，""=探测过但缺失；部署环境里装上 sing-box 后需重启才会被发现）。
    """
    global _singbox_bin_cache
    if _singbox_bin_cache is not None:
        return _singbox_bin_cache or None
    env_path = str(os.environ.get("SINGBOX_BIN") or "").strip()
    candidates: list[str] = []
    if env_path:
        candidates.append(env_path)
    candidates.extend(_SINGBOX_CANDIDATE_PATHS)
    found = shutil.which("sing-box") if hasattr(shutil, "which") else None
    if found:
        candidates.append(found)
    for path in candidates:
        if path and os.path.isfile(path) and os.access(path, os.X_OK):
            _singbox_bin_cache = path
            return path
    _singbox_bin_cache = ""
    return None


# None=尚未探测；""=探测过但不存在；其他=可执行文件路径
_singbox_bin_cache: Optional[str] = None


def singbox_available() -> bool:
    return find_singbox_binary() is not None


_SINGBOX_VERSION_CACHE: Optional[str] = None


def singbox_version() -> str:
    """sing-box 版本串（探测失败返回空串）。供启动日志与 /healthz 展示。"""
    global _SINGBOX_VERSION_CACHE
    if _SINGBOX_VERSION_CACHE is not None:
        return _SINGBOX_VERSION_CACHE
    binary = find_singbox_binary()
    if not binary:
        _SINGBOX_VERSION_CACHE = ""
        return ""
    try:
        out = subprocess.run(
            [binary, "version"], capture_output=True, text=True, timeout=10
        )
        first = (out.stdout or out.stderr or "").strip().splitlines()
        _SINGBOX_VERSION_CACHE = first[0].strip() if first else ""
    except Exception:
        _SINGBOX_VERSION_CACHE = ""
    return _SINGBOX_VERSION_CACHE


# ============================================================
# sing-box 实例管理器
# ============================================================


def _idle_minutes() -> float:
    try:
        return max(0.0, float(os.environ.get("SINGBOX_IDLE_MINUTES", "30")))
    except ValueError:
        return 30.0


def _state_dir() -> str:
    # 必须返回绝对路径：sing-box 的 -D 参数会先 chdir，随后读取的 -c
    # 配置路径若仍是相对路径（./singbox/…），会被解析到 chdir 之后的位置
    # （./singbox/singbox/…，必然读不到，进程秒退）。STATE_DIR 未设置时
    # 默认落在进程启动时 CWD 下的 singbox/。
    base = os.environ.get("STATE_DIR", ".")
    return os.path.abspath(os.path.join(base, "singbox"))


class _SingboxInstance:
    __slots__ = (
        "uri",
        "port",
        "process",
        "config_path",
        "log_path",
        "started_at",
        "last_used",
    )

    def __init__(self, uri: str, port: int, process, config_path: str, log_path: str):
        self.uri = uri
        self.port = port
        self.process = process
        self.config_path = config_path
        self.log_path = log_path
        self.started_at = time.time()
        self.last_used = time.time()

    def alive(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def log_tail(self, limit: int = 400) -> str:
        if limit <= 0:
            return ""
        try:
            # A long-lived bridge can have a large log; never load it all just
            # to display a few hundred characters. UTF-8 uses at most 4 bytes
            # per character, so this bounded suffix contains the desired tail.
            with open(self.log_path, "rb") as f:
                f.seek(0, os.SEEK_END)
                f.seek(max(0, f.tell() - limit * 4))
                return f.read(limit * 4).decode("utf-8", errors="replace")[-limit:]
        except Exception:
            return ""


def _find_free_port() -> int:
    """让 OS 分配一个空闲端口（bind(0) 后立即关掉，存在微小竞态，可接受）。"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_socks5_ready(
    port: int, timeout: float = 10.0, process=None, cancelled=None
) -> bool:
    """轮询本地端口直到 sing-box 的 socks5 入站完成握手应答。

    只 TCP connect 成功不算就绪——端口可能被其他进程占着（崩溃复活场景：
    绑定失败的 sing-box 要几十毫秒才退出，期间 connect 打到的其实是
    占端口者），必须收到 SOCKS5 方法协商应答（客户端发 \x05\x01\x00、
    服务端回 \x05 开头两字节）才认。
    process 传入时，子进程一退出立即返回（不等满超时）。
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cancelled is not None and cancelled():
            return False
        if process is not None and process.poll() is not None:
            return False  # 进程已退出，不用再等
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.4) as s:
                s.settimeout(0.5)
                s.sendall(b"\x05\x01\x00")  # SOCKS5 greeting：支持无认证
                # TCP may split even a two-byte greeting across packets.
                reply = bytearray()
                while len(reply) < 2:
                    chunk = s.recv(2 - len(reply))
                    if not chunk:
                        break
                    reply.extend(chunk)
                if reply == b"\x05\x00":
                    return True
        except OSError:
            pass
        time.sleep(0.05)
    return False


_MISSING_HINT = (
    "当前环境未找到 sing-box 可执行文件。解决方法任选其一："
    "① 使用本项目的 Docker 镜像（已内置 sing-box）；"
    "② 手动安装 sing-box（https://github.com/SagerNet/sing-box/releases）；"
    "③ 已安装但不在默认路径时，设环境变量 SINGBOX_BIN 指向可执行文件。"
    "在装好之前，vless/vmess/trojan/ss/socks4/hysteria2/tuic 协议的代理无法使用"
    "（为防真实 IP 泄露，不会自动降级直连）。"
)


class ProxyProcessManager:
    """协议转换子进程池：代理 URI → 本地 socks5 入站，惰性启动、稳定复用。

    线程安全；同 URI 只起一个进程（**按 URI 加锁**，不同代理的启动
    互不阻塞）。崩溃的实例在下次取用时自动重启：优先复用原端口
    （客户端池签名不变，httpx 连接池无需重建）；原端口已被其他进程
    抢走时自动换新端口重试，不会卡死在同一个坏端口上。
    """

    engine_name = "代理转换器"

    def __init__(self):
        self._lock = threading.Lock()  # 保护实例、启动锁和使用计数
        self._instances: dict[str, _SingboxInstance] = {}
        self._uri_locks = weakref.WeakValueDictionary()
        self._active_uses: dict[str, int] = {}
        self._probe_only: set[str] = set()
        self._closed = False

    def retain(self, uri, *, temporary=False):
        with self._lock:
            if not temporary:
                self._probe_only.discard(uri)  # Adopted by real traffic.
            elif uri not in self._instances and not self._active_uses.get(uri):
                self._probe_only.add(uri)
            self._active_uses[uri] = self._active_uses.get(uri, 0) + 1

    def release(self, uri):
        with self._lock:
            count = self._active_uses.get(uri, 0) - 1
            if count <= 0:
                self._active_uses.pop(uri, None)
            else:
                self._active_uses[uri] = count
            instance = self._instances.get(uri)
            if instance is not None:
                instance.last_used = time.time()
            elif count <= 0:
                self._probe_only.discard(uri)

    # ---------- 对外 ----------

    def get_local_socks5(self, uri: str) -> str:
        """URI → socks5://127.0.0.1:{port}。首次调用启动对应的协议转换进程。

        阻塞提示：首次启动含子进程拉起与端口就绪等待（典型 ~100ms，
        sing-box 最多等待 10s；SSTP 另含最多 30s 的握手）。异步上下文请先用 resolve_proxy_uri_async()
        预热，避免卡住事件循环。
        """
        uri = self._cache_uri(str(uri or "").strip())
        if not uri:
            raise ProxyResolveError("get_local_socks5 收到空 URI。")
        usage = current_proxy_usage.get()
        if usage is not None:
            usage.include(self, uri)
        if self._startup_cancelled(uri):
            raise ProxyResolveError(
                "请求已结束或服务正在退出，已取消代理启动。", retry_new_port=False
            )
        cached = self._cached_local_socks5(uri)
        if cached is not None:
            return cached

        with self._uri_lock(uri):  # 按 URI 串行：不同代理的启动互不阻塞
            if self._startup_cancelled(uri):
                raise ProxyResolveError(
                    "请求已结束或服务正在退出，已取消代理启动。", retry_new_port=False
                )
            cached = self._cached_local_socks5(uri)
            if cached is not None:
                return cached
            with self._lock:
                inst = self._instances.get(uri)
            stale_port = inst.port if inst is not None else None
            try:
                inst = self._launch(
                    uri, stale_port if stale_port else _find_free_port()
                )
            except ProxyResolveError as exc:
                if stale_port is None or not exc.retry_new_port:
                    raise
                # 崩溃重启偏好复用原端口（保池签名稳定），但原端口可能已被
                # 其他进程抢走——那种情况下不重试会永远失败。换新端口重试
                # 一次；仍失败则清掉死条目再抛（下次取用从全新端口开始，
                # 不会永久卡在同一个坏端口上）。
                try:
                    inst = self._launch(uri, _find_free_port())
                except ProxyResolveError:
                    with self._lock:
                        self._instances.pop(uri, None)
                    raise
            request_closed = usage is not None and usage.closed
            with self._lock:
                closed = self._closed or (
                    request_closed and not self._active_uses.get(uri)
                )
                if not closed:
                    self._instances[uri] = inst
            if closed:  # Do not hold the shared lock while waiting for a process.
                self._terminate(inst)
                raise ProxyResolveError(
                    "请求已结束或服务正在退出，已取消代理启动。", retry_new_port=False
                )
            return f"socks5://127.0.0.1:{inst.port}"

    def _cached_local_socks5(self, uri):
        # Lookup and touch must be atomic with eviction. An unlocked dict.get
        # could hand out a socket whose process the reaper had already removed.
        with self._lock:
            if self._closed:
                raise ProxyResolveError(f"进程正在退出，{self.engine_name} 已停。")
            inst = self._instances.get(uri)
            if inst is not None and inst.alive():
                inst.last_used = time.time()
                return f"socks5://127.0.0.1:{inst.port}"

        return None

    def _startup_cancelled(self, uri):
        usage = current_proxy_usage.get()
        request_closed = usage is not None and usage.closed
        with self._lock:
            # Another generation waiting for this node can take ownership of
            # startup, even if the original request's thread was cancelled.
            return self._closed or (request_closed and not self._active_uses.get(uri))

    def _uri_lock(self, uri: str) -> threading.Lock:
        """Share a startup lock among current waiters; discard it when unused."""
        with self._lock:
            lk = self._uri_locks.get(uri)
            if lk is None:
                lk = threading.Lock()
                self._uri_locks[uri] = lk
            return lk

    def shutdown_all(self) -> None:
        """停机：终止全部协议转换进程（FastAPI shutdown / atexit 双保险）。"""
        with self._lock:
            self._closed = True
            instances = list(self._instances.values())
            self._instances.clear()
            self._probe_only.clear()
        for inst in instances:
            self._terminate(inst)  # 锁外终止：terminate+kill 最多 6s，不阻塞取用

    def reopen(self) -> None:
        """停机后重新启用管理器（测试隔离 / 运行时重启场景）。

        只清除 _closed 哨兵，不复活旧实例（已被终止，重新取用时全新拉起）。
        """
        with self._lock:
            self._closed = False

    def reap_idle(self) -> int:
        """按对应管理器的空闲阈值回收实例，返回回收数量。

        已崩溃的死实例即使空闲回收被关闭（SINGBOX_IDLE_MINUTES=0）也必须
        清理：旧实现直接 return，崩溃子进程的条目永远无人处理（僵尸进程
        要等下一次有人调它的 alive()/poll() 才被收尸，不再被访问就常驻）。
        """
        idle_seconds = self._idle_minutes() * 60.0
        now = time.time()
        with self._lock:
            keys = list(self._instances)
        removed = 0
        for uri in keys:
            lock = self._uri_lock(uri)
            if not lock.acquire(blocking=False):
                continue  # A restart owns this URI; do not discard its port.
            try:
                with self._lock:
                    inst = self._instances.get(uri)
                    if inst is None or self._active_uses.get(uri):
                        continue
                    if inst.alive() and (
                        idle_seconds <= 0 or now - inst.last_used <= idle_seconds
                    ):
                        continue
                    self._instances.pop(uri)
                    self._probe_only.discard(uri)
                # Keep the per-URI lock until the old listener has gone away.
                self._terminate(inst)
                removed += 1
            finally:
                lock.release()
        return removed

    def discard_unused(self, uri):
        """Release a probe-only listener without racing startup or active users."""
        lock = self._uri_lock(uri)
        if not lock.acquire(blocking=False):
            # In-flight startup sees its closed ProxyUsage and disposes itself.
            return
        try:
            with self._lock:
                if self._active_uses.get(uri) or uri not in self._probe_only:
                    return
                instance = self._instances.pop(uri, None)
                self._probe_only.discard(uri)
            if instance is not None:
                self._terminate(instance)
        finally:
            lock.release()

    def stats(self) -> dict:
        """运行概览（供日志 / 控制台展示）。"""
        with self._lock:
            items = list(self._instances.values())
        return {
            **self._runtime_info(),
            "instances": [
                {
                    "port": i.port,
                    "uri": mask_proxy_uri(i.uri),
                    "alive": i.alive(),
                    "started_at": int(i.started_at),
                }
                for i in items
            ],
        }

    def _runtime_info(self):
        raise NotImplementedError

    def _cache_uri(self, uri):
        return uri

    def _idle_minutes(self):
        return _idle_minutes()

    def _launch(self, uri, port):
        raise NotImplementedError

    @staticmethod
    def _terminate(inst: _SingboxInstance) -> None:
        process = inst.process
        if process is None or process.poll() is not None:
            return
        try:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
        except Exception:
            pass


class SingboxManager(ProxyProcessManager):
    engine_name = "sing-box"

    def _runtime_info(self):
        return {
            "available": singbox_available(),
            "binary": find_singbox_binary() or "",
            "version": singbox_version(),
        }

    def _launch(self, uri: str, port: int) -> _SingboxInstance:
        binary = find_singbox_binary()
        if not binary:
            raise ProxyResolveError(_MISSING_HINT)

        try:
            outbound = parse_share_uri_to_outbound(uri)
        except ProxyResolveError:
            raise
        except Exception as e:
            raise ProxyResolveError(f"解析代理链接失败：{type(e).__name__}: {e}")

        outbound = dict(outbound)
        outbound["tag"] = "proxy"

        directory = _state_dir()
        try:
            os.makedirs(directory, exist_ok=True)
        except OSError as e:
            raise ProxyResolveError(f"无法创建 sing-box 状态目录（{directory}）：{e}")
        digest = hashlib.sha1(uri.encode("utf-8")).hexdigest()[:16]
        # 清掉同 URI 的旧实例残留（崩溃重启/端口变化后避免配置与日志文件无限积累）
        try:
            for stale in glob.glob(os.path.join(directory, f"sb-{digest}-*")):
                os.unlink(stale)
        except OSError:
            pass
        config_path = os.path.join(directory, f"sb-{digest}-{port}.json")
        log_path = os.path.join(directory, f"sb-{digest}-{port}.log")

        config = {
            "log": {"level": "warn", "timestamp": True},
            "inbounds": [
                {
                    "type": "socks",
                    "tag": "in",
                    "listen": "127.0.0.1",
                    "listen_port": port,
                }
            ],
            "outbounds": [outbound, {"type": "direct", "tag": "direct"}],
            "route": {"final": "proxy"},
        }
        try:
            # 0600：配置含节点凭据（uuid/密码），默认 umask 的 0644 会让
            # 同机其他本地账号读到完整代理凭证。os.open 的 mode 仅在创建时
            # 生效，文件已存在（残留清理失败等）则靠 chmod 兑底。
            fd = os.open(config_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(config, f, ensure_ascii=False, indent=2)
            try:
                os.chmod(config_path, 0o600)
            except OSError:
                pass
        except OSError as e:
            raise ProxyResolveError(f"无法写入 sing-box 配置文件（{config_path}）：{e}")

        # 日志同样收紧权限（可能带节点信息）；打开失败按既定错误契约抛
        # ProxyResolveError（旧实现裸抛 OSError，调用链只捕本类型会变未分类 500）。
        try:
            log_fd = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            log_file = os.fdopen(log_fd, "ab", buffering=0)
            try:
                os.chmod(log_path, 0o600)
            except OSError:
                pass
        except OSError as e:
            raise ProxyResolveError(f"无法打开 sing-box 日志文件（{log_path}）：{e}")
        try:
            process = subprocess.Popen(
                [binary, "run", "-c", config_path, "-D", directory],
                stdout=log_file,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                start_new_session=True,  # 进程组独立，Ctrl+C 不误杀
            )
        except OSError as e:
            raise ProxyResolveError(f"启动 sing-box 失败（{binary}）：{e}")
        finally:
            # 子进程已继承句柄；父进程立刻关闭（成功/失败路径都不泄漏 fd）。
            log_file.close()

        inst = _SingboxInstance(uri, port, process, config_path, log_path)
        # 协议级就绪检查：SOCKS5 握手应答 + 进程存活双条件（见函数 docstring）
        ready = _wait_socks5_ready(
            port,
            timeout=10.0,
            process=process,
            cancelled=lambda: self._startup_cancelled(uri),
        )
        alive = process.poll() is None
        if not (ready and alive):
            tail = inst.log_tail()
            self._terminate(inst)
            if self._startup_cancelled(uri):
                raise ProxyResolveError(
                    "请求已结束或服务正在退出，已取消代理启动。", retry_new_port=False
                )
            if not alive:
                detail = f"sing-box 启动后即退出（配置或节点参数大概率有误）。日志尾部：{tail or '（无输出）'}"
            else:
                detail = f"sing-box 启动后端口 {port} 未就绪。日志尾部：{tail or '（无输出）'}"
            raise ProxyResolveError(f"代理「{mask_proxy_uri(uri)}」转换失败：{detail}")
        print(f"🛰️ [代理转换] sing-box 已启动：{mask_proxy_uri(uri)} → 127.0.0.1:{port}")
        return inst


def find_bridge_binary() -> Optional[str]:
    candidates = [
        os.environ.get("PROXY_BRIDGE_BIN", ""),
        "/usr/local/bin/vertex-proxy-bridge",
        os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "bin", "vertex-proxy-bridge")
        ),
        shutil.which("vertex-proxy-bridge") or "",
    ]
    return next(
        (
            os.path.abspath(path)
            for path in candidates
            if path and os.path.isfile(path) and os.access(path, os.X_OK)
        ),
        None,
    )


def _wait_bridge_ready(instance, ready_file, timeout, cancelled=None):
    """Require the child's own bind acknowledgment before checking SOCKS5.

    A foreign SOCKS5 process can take a released dynamic port while SSTP is
    still negotiating. Its handshake must never count as our child's readiness.
    """
    deadline = time.monotonic() + timeout
    while instance.alive() and time.monotonic() < deadline:
        if cancelled is not None and cancelled():
            return False
        try:
            with open(ready_file, encoding="ascii") as marker:
                ready = marker.read(16) == "ready\n"
        except OSError:
            ready = False
        if ready:
            return _wait_socks5_ready(
                instance.port,
                timeout=max(0.1, deadline - time.monotonic()),
                process=instance.process,
                cancelled=cancelled,
            )
        time.sleep(0.05)
    return False


class ProxyBridgeManager(ProxyProcessManager):
    engine_name = "TURN/SSTP 转换器"

    def __init__(self):
        super().__init__()
        self._file_namespace = os.urandom(6).hex()
        self._swept = False

    def _runtime_info(self):
        binary = find_bridge_binary()
        return {
            "available": bool(binary),
            "binary": binary or "",
            "version": "1" if binary else "",
        }

    def _sweep_orphan_files(self, directory: str) -> None:
        """Delete bridge state files whose owning process is gone.

        同前缀清理只覆盖本进程实例的重启；SIGKILL / OOM 后残留的
        bridge-{pid}-…-*.log 属于已死进程，永远不会被前缀匹配到，长跑
        部署会无限积累。文件名内嵌 pid：pid 已死且超过宽限期才删除，
        pid 复用只会导致少删（安全方向）。
        """
        if self._swept:
            return
        self._swept = True
        now = time.time()
        try:
            entries = os.listdir(directory)
        except OSError:
            return
        for name in entries:
            parts = name.split("-")
            # bridge-{pid}-{ns}-{digest}-{port}.{json|log|ready}
            if len(parts) < 5 or not parts[0] == "bridge" or not parts[1].isdigit():
                continue
            pid = int(parts[1])
            if pid == os.getpid():
                continue
            try:
                os.kill(pid, 0)
                continue  # 属主仍存活（含僵尸）：保留
            except ProcessLookupError:
                pass  # 已死：孤儿
            except OSError:
                continue  # 无法判定（权限 / 平台差异）：保守保留
            path = os.path.join(directory, name)
            try:
                if now - os.path.getmtime(path) > 3600:  # 1 小时宽限期
                    os.unlink(path)
            except OSError:
                pass

    def _cache_uri(self, uri):
        # Remarks, query order and explicit defaults do not identify a different
        # tunnel. Reuse one SSTP session even when the same node is imported in
        # both RFC and URL forms or renamed in another account's list.
        try:
            cfg = parse_bridge_proxy_uri(uri)
        except ProxyFormatError as exc:
            raise ProxyResolveError(str(exc), retry_new_port=False) from None
        options = {}
        if cfg["scheme"] != "turn":
            if cfg["tls_server_name"] != cfg["server"]:
                options["sni"] = cfg["tls_server_name"]
            if cfg["ca_file"]:
                options["ca"] = cfg["ca_file"]
            if cfg["insecure"]:
                options["insecure"] = True
        if cfg["scheme"] in {"turn", "turns"} and cfg["realm"]:
            options["realm"] = cfg["realm"]
        if cfg["scheme"] == "sstp" and cfg["mtu"] < 1400:
            options["mtu"] = cfg["mtu"]
        return build_proxy_uri(
            cfg["scheme"],
            cfg["server"],
            cfg["server_port"],
            cfg["username"],
            cfg["password"],
            options=options,
        )

    def _idle_minutes(self):
        try:
            return max(0.0, float(os.environ.get("PROXY_BRIDGE_IDLE_MINUTES", "30")))
        except ValueError:
            return 30.0

    def _launch(self, uri, port):
        try:
            config = parse_bridge_proxy_uri(uri)
        except ProxyFormatError as exc:
            raise ProxyResolveError(str(exc), retry_new_port=False) from None
        binary = find_bridge_binary()
        if not binary:
            raise ProxyResolveError(
                "未找到 TURN/SSTP 转换器 vertex-proxy-bridge。请使用包含该组件的新版本 Docker 镜像，"
                "或在 proxybridge/ 目录执行 go build -o ../bin/vertex-proxy-bridge .；"
                "也可通过 PROXY_BRIDGE_BIN 指定可执行文件。不会降级直连。",
                retry_new_port=False,
            )
        config.pop("options", None)
        config["listen_port"] = port
        directory = os.path.abspath(
            os.path.join(os.environ.get("STATE_DIR", "."), "proxy-bridge")
        )
        digest = hashlib.sha256(uri.encode()).hexdigest()[:16]
        prefix = f"bridge-{os.getpid()}-{self._file_namespace}-{digest}-"
        config_path = os.path.join(directory, f"{prefix}{port}.json")
        log_path = os.path.join(directory, f"{prefix}{port}.log")
        ready_path = os.path.join(directory, f"{prefix}{port}.ready")
        config["ready_file"] = ready_path
        inst = None
        try:
            os.makedirs(directory, mode=0o700, exist_ok=True)
            os.chmod(directory, 0o700)
            self._sweep_orphan_files(directory)
            for stale in glob.glob(os.path.join(directory, prefix + "*")):
                os.unlink(stale)
            fd = os.open(config_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as file:
                json.dump(config, file)
            log_fd = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(log_fd, "ab", buffering=0) as log_file:
                process = subprocess.Popen(
                    [binary, "-config", config_path],
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL,
                    start_new_session=True,
                )
            inst = _SingboxInstance(uri, port, process, config_path, log_path)
            timeout = 35.0 if config["scheme"] == "sstp" else 10.0
            if (
                not _wait_bridge_ready(
                    inst,
                    ready_path,
                    timeout,
                    cancelled=lambda: self._startup_cancelled(uri),
                )
                or not inst.alive()
            ):
                self._terminate(inst)
                if self._startup_cancelled(uri):
                    raise ProxyResolveError(
                        "请求已结束或服务正在退出，已取消代理启动。",
                        retry_new_port=False,
                    )
                # Never forward arbitrary child-process output: custom helpers or
                # malformed peers could echo credentials in an error message.
                known = {
                    "ca_read_failed": "无法读取 CA 证书文件",
                    "ca_invalid_pem": "CA 文件不是有效的 PEM 证书",
                    "sstp_custom_ca_requires_linux": "SSTP 自定义 CA 当前需要 Linux",
                    "sstp_auth_failed_requires_mschapv2": "SSTP 认证失败，需正确的 MS-CHAPv2 账号密码",
                    "sstp_handshake_failed_check_tls_credentials_and_mschapv2": "SSTP TLS / PPP 握手失败",
                    "sstp_handshake_timeout": "SSTP 握手超时（包含 TCP / TLS / PPP 的 30 秒总预算）",
                    "sstp_ipv4_address_missing": "SSTP 服务器未分配可用的 IPv4 地址",
                    "sstp_network_stack_failed": "SSTP 用户态网络栈初始化失败",
                    "turn_probe_failed_check_server": "TURN 服务器探测失败：连接或分配请求无响应",
                    "listen_failed": "本地端口绑定失败",
                    "ready_write_failed": "无法写入转换器就绪标记",
                    "invalid_config": "转换器配置无效",
                }
                code = next(
                    (
                        line.strip()
                        for line in reversed(inst.log_tail().splitlines())
                        if line.strip() in known
                    ),
                    "",
                )
                detail = known.get(code, "启动超时或子进程退出")
                raise ProxyResolveError(
                    f"{config['scheme'].upper()} 转换失败：{detail}。"
                    "请检查服务器连通性、账号密码、TLS 证书，以及 TURN RFC6062 / SSTP MS-CHAPv2 支持。",
                    retry_new_port=code == "listen_failed",
                )
        except OSError:
            if inst is not None:
                self._terminate(inst)
            raise ProxyResolveError(
                "无法创建或启动 TURN/SSTP 转换器，请检查状态目录权限和可执行文件。"
            ) from None
        finally:
            # The helper reads the private configuration once. Remove credentials
            # from transient files after startup, on both success and failure.
            for path in (config_path, ready_path):
                try:
                    os.unlink(path)
                except OSError:
                    pass
        print(
            f"🛰️ [代理转换] {config['scheme'].upper()} 已启动：{mask_proxy_uri(uri)} → 127.0.0.1:{port}"
        )
        return inst


singbox_manager = SingboxManager()
bridge_manager = ProxyBridgeManager()


def _reaper_loop() -> None:
    """后台空闲回收线程（每 5 分钟扫一次，daemon）。"""
    while True:
        time.sleep(300)
        for manager in (singbox_manager, bridge_manager):
            try:
                n = manager.reap_idle()
                if n:
                    print(
                        f"♻️ [代理转换] 已回收 {n} 个空闲/已退出的 {manager.engine_name} 实例。"
                    )
            except Exception as e:
                print(
                    f"⚠️ [代理转换] {manager.engine_name} 空闲回收异常（已忽略）："
                    f"{type(e).__name__}: {e}"
                )


_reaper_started = False
_reaper_lock = threading.Lock()


def ensure_reaper() -> None:
    """惰性启动空闲回收线程（首次成功创建转换实例时调用）。"""
    global _reaper_started
    with _reaper_lock:
        if _reaper_started:
            return
        _reaper_started = True
        threading.Thread(
            target=_reaper_loop, daemon=True, name="singbox-reaper"
        ).start()


atexit.register(singbox_manager.shutdown_all)
atexit.register(bridge_manager.shutdown_all)


# ============================================================
# 统一入口：resolve / validate / mask
# ============================================================


def resolve_proxy_uri(uri: Optional[str]) -> str:
    """任意代理 URI → httpx / curl_cffi 可直接使用的代理 URL。

    - 空 / direct 哨兵 → ""（直连）；
    - http/https/socks5/socks5h → 原样返回；
    - socks4/socks4a/vless/vmess/trojan/ss/hysteria2/tuic → socks5://127.0.0.1:{port}
      （首次使用时惰性启动 sing-box；之后纯缓存查找，开销≈0）；
    - 其他 → 抛 ProxyResolveError（含可操作指引，绝不静默直连）。

    正常复用期间端口稳定，客户端池签名不会因重复 resolve 而抖动；
    回收、崩溃或端口冲突后可能重新分配。
    """
    try:
        value = normalize_proxy_uri(str(uri or ""))
    except ProxyFormatError as exc:
        raise ProxyResolveError(str(exc)) from None
    if not value or value.lower() in DIRECT_TOKENS:
        return ""
    kind = classify_proxy_uri(value)
    if kind == "native":
        return value
    if kind == "singbox":
        url = singbox_manager.get_local_socks5(value)
        ensure_reaper()
        return url
    if kind == "bridge":
        url = bridge_manager.get_local_socks5(value)
        ensure_reaper()
        return url
    scheme = _uri_scheme(value)
    raise ProxyResolveError(
        f"不支持的代理协议「{scheme or '（无协议）'}」。"
        f"支持：{'/'.join(sorted(ALL_PROXY_SCHEMES))} 或 direct（强制直连）。"
        f"收到的是：{mask_proxy_uri(value)}"
    )


async def resolve_proxy_uri_async(uri: Optional[str]) -> str:
    """resolve_proxy_uri 的异步版：sing-box 首次启动放线程池，不阻塞事件循环。

    FastAPI 处理器 / SDK 通道等异步上下文应优先用这个入口，或先用它
    预热、再调用内部会同步 resolve 的池化代码（池签名命中缓存，零阻塞）。
    结果与同步版完全一致（同 URI 端口映射进程内稳定）。
    """
    kind = classify_proxy_uri(uri)
    if kind not in {"singbox", "bridge"}:
        return resolve_proxy_uri(uri)  # Native proxies never need a worker thread.
    manager = bridge_manager if kind == "bridge" else singbox_manager
    value = manager._cache_uri(normalize_proxy_uri(uri))
    usage = current_proxy_usage.get()
    if usage is not None:
        usage.include(manager, value)
    if manager._startup_cancelled(value):
        raise ProxyResolveError(
            "请求已结束或服务正在退出，已取消代理启动。", retry_new_port=False
        )
    cached = manager._cached_local_socks5(value)
    if cached is not None:
        return cached
    return await asyncio.to_thread(resolve_proxy_uri, value)

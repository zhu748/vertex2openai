"""出站保活服务（移植自 gcli2api 的 keeplive 模块，适配本项目约定）。

定期向配置的 URL 发送 GET 请求，典型用途：
- 保活本服务自己的公网部署：Render / HuggingFace Space 等免费平台会
  冻结闲置实例，把 URL 填成自己的 ``https://你的域名/healthz`` 即可
  每分钟自我唤醒一次；
- 或驱动外部监控端点（UptimeRobot 等）。

配置（环境变量仅作**初始值**，控制台「运行设置」页可热改，保存即生效）：
- ``KEEPALIVE_URL``：保活目标 URL；留空 = 不启动任何任务（零开销），
  必须以 http:// 或 https:// 开头。
- ``KEEPALIVE_INTERVAL``：请求间隔秒数，默认 60（一分钟一次），下限 5。

保活请求**直连**出站、不走出站代理：目标通常是公网监控端点或本服务
自身，跟随 Google 上游代理反而会在代理不可用时让保活一并失效。
单请求超时 30 秒；失败只记日志，绝不影响服务本身。
"""

import asyncio
import ipaddress
from typing import Optional

import httpx

import config as app_config
from runtime_state import app_state

# 单次保活请求的超时（秒）。
_REQUEST_TIMEOUT_SECONDS = 30.0
# 间隔下限（秒）：防止误配成秒级高频请求打爆保活目标。
_MIN_INTERVAL_SECONDS = 5
_MAX_REDIRECTS = 5


# 已知的云元数据域名（静态可判）：域名型元数据端点无法用地址段判段。
_METADATA_HOSTS = frozenset({"metadata.google.internal", "metadata.goog"})


def keepalive_private_targets_allowed(override=None) -> bool:
    """SEC-P2-6：是否允许私网/环回保活目标（控制台设置，默认关）。

    override 用于设置保存校验：同一补丁里同时改 keepalive_url 和
    keepalive_allow_private 时，以补丁里的新值为准。
    """
    if override is not None:
        return bool(override)
    try:
        return bool(app_state.get_setting("keepalive_allow_private", False))
    except Exception:
        return False


def _keepalive_host_allowed(host: str, allow_private: bool = False) -> bool:
    """保活目标的地址段裁决（2026-11 S-3；SEC-P2-6 收紧）。

    云元数据 / 链路本地段（169.254.169.254、metadata.google.internal 等）
    无论如何都不是合法保活目标：会话被伪造后可借保活任务盲探元数据端点
    收割凭据。环回与私网段需显式开启 keepalive_allow_private：被劫持的
    控制台会话可用保活任务对内网管理面稳定重放 GET 并经 SSE 日志回读
    状态码（探测 oracle），默认拒绝、自我保活用户显式确认后放行。
    """
    if host.lower() in _METADATA_HOSTS:
        return False
    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        return True  # 普通域名无法静态判段；解析交给出站层
    if addr.is_link_local or addr.is_multicast or addr.is_reserved or addr.is_unspecified:
        return False
    if addr.is_loopback or addr.is_private:
        return allow_private
    return True


def validate_keepalive_url(url: str, allow_private: bool = False) -> bool:
    if not url:
        return True
    try:
        parsed = httpx.URL(url)
    except (httpx.InvalidURL, ValueError):
        return False
    if not (parsed.scheme in ("http", "https") and bool(parsed.host)):
        return False
    if any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in url):
        return False
    if parsed.port is not None and not 1 <= parsed.port <= 65535:
        return False
    # 2026-11 S-3 加固：
    # 1) 拒绝内嵌凭据（userinfo）的 URL：保活没有鉴权语义，凭据只会在
    #    启动/运行日志里原样泄露（GET {url}）。
    authority = url.split("://", 1)[-1].split("/", 1)[0]
    if "@" in authority:
        return False
    # 2) 链路本地 / 云元数据地址段拒绝（见 _keepalive_host_allowed）。
    #    重定向目标经本函数复检，同一口径；私网/环回需 allow_private
    #    显式放行（SEC-P2-6）。
    return _keepalive_host_allowed(parsed.host, allow_private=allow_private)


class KeepAliveService:
    """保活服务：定期向指定 URL 发送 GET 请求（未配置 URL 时零开销）。"""

    def __init__(self):
        self._task: Optional[asyncio.Task] = None
        self._lifecycle_lock = asyncio.Lock()

    def _current_config(self) -> tuple[str, int]:
        """读取当前生效配置（控制台保存值 > 环境变量初始值 > 内置默认）。"""
        url = str(app_state.get_setting("keepalive_url", "") or "").strip()
        interval = app_state.get_setting("keepalive_interval", 60)
        return url, int(interval)

    async def _run(self, url: str, interval: int):
        # 私网/环回裁决在任务启动时固定（SEC-P2-6）：运行中改设置需重启任务
        # 生效（settings_api 的 restart 已覆盖）。
        allow_private = keepalive_private_targets_allowed()
        print(f"💓 [保活] 任务启动：GET {url}，间隔 {interval}s。")
        try:
            # 客户端随任务生命周期创建/关闭：重启（改配置）会换新客户端，
            # 取消时 async with 保证连接池不悬挂。
            async with httpx.AsyncClient(
                timeout=_REQUEST_TIMEOUT_SECONDS, follow_redirects=False, trust_env=False,
                verify=app_config.SSL_CERT_FILE or True,
            ) as client:
                while True:
                    try:
                        # Only response headers are needed. A large or endless
                        # body must not consume memory or hold the task forever.
                        async with asyncio.timeout(_REQUEST_TIMEOUT_SECONDS):
                            current = httpx.URL(url)
                            for redirect in range(_MAX_REDIRECTS + 1):
                                async with client.stream("GET", current) as response:
                                    if response.has_redirect_location:
                                        if redirect == _MAX_REDIRECTS:
                                            raise ValueError("保活重定向次数超过上限。")
                                        current = current.join(response.headers["location"])
                                        if not validate_keepalive_url(str(current), allow_private=allow_private):
                                            raise ValueError("保活重定向地址无效。")
                                        continue
                                    print(f"💓 [保活] GET {url} -> {response.status_code}")
                                    break
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        # 单次失败（网络抖动/目标临时不可用）只记日志，下一轮照常。
                        print(f"⚠️ [保活] GET {url} 失败：{type(e).__name__} - {e}")
                    await asyncio.sleep(interval)
        finally:
            print("💓 [保活] 任务已停止。")

    async def start(self):
        """按当前配置启动保活任务；未配置 URL / 配置非法时不启动。"""
        async with self._lifecycle_lock:
            self._start()

    def _start(self):
        if self._task and not self._task.done():
            # 已有任务在运行，不重复启动（重启走 restart）。
            return

        try:
            url, interval = self._current_config()
        except (TypeError, ValueError, OverflowError):
            print("⚠️ [保活] 间隔配置无效，保活未启动。")
            return
        if not url:
            # 未配置：静默跳过，不在每次启动时给不使用保活的用户刷日志。
            return
        if not validate_keepalive_url(url, allow_private=keepalive_private_targets_allowed()):
            if validate_keepalive_url(url, allow_private=True):
                print("⚠️ [保活] 目标是私网/环回地址：被劫持的控制台会话可借保活任务探测内网。"
                      "确属自我保活/内网监控场景时，请在控制台「运行设置」开启 keepalive_allow_private 后重启。")
            else:
                print("⚠️ [保活] URL 必须是包含主机名的合法 HTTP(S) 地址，保活未启动。")
            return
        if interval < _MIN_INTERVAL_SECONDS:
            print(f"⚠️ [保活] 间隔 {interval}s 低于下限 {_MIN_INTERVAL_SECONDS}s，保活未启动。")
            return
        self._task = asyncio.create_task(self._run(url, interval), name="keepalive_service")

    async def stop(self):
        """停止保活任务（幂等；未运行时为空操作）。"""
        async with self._lifecycle_lock:
            await self._stop()

    async def _stop(self):
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def restart(self):
        """重启保活服务：控制台改配置后调用，按最新配置决定启停。"""
        async with self._lifecycle_lock:
            await self._stop()
            self._start()

    @property
    def is_running(self) -> bool:
        """当前保活任务是否在运行。"""
        return self._task is not None and not self._task.done()


# 全局保活服务实例（lifespan 启停、settings_api 热重启共用）。
keepalive_service = KeepAliveService()

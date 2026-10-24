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
from typing import Optional

import httpx

import config as app_config
from runtime_state import app_state

# 单次保活请求的超时（秒）。
_REQUEST_TIMEOUT_SECONDS = 30.0
# 间隔下限（秒）：防止误配成秒级高频请求打爆保活目标。
_MIN_INTERVAL_SECONDS = 5
_MAX_REDIRECTS = 5


def validate_keepalive_url(url: str) -> bool:
    if not url:
        return True
    try:
        parsed = httpx.URL(url)
        return (parsed.scheme in ("http", "https") and bool(parsed.host)
                and not any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in url)
                and (parsed.port is None or 1 <= parsed.port <= 65535))
    except (httpx.InvalidURL, ValueError):
        return False


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
                                        if not validate_keepalive_url(str(current)):
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
        if not validate_keepalive_url(url):
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

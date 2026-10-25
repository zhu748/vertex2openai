"""Cookie 短期令牌自动续期（accounts.google.com/RotateCookies）。

背景：Cookie 通道的长效凭证（SID/HSID/SAPISID 等）在浏览器不退出登录时
长期有效，但 Google 会持续轮换一批**短期令牌**（__Secure-1/3PSIDTS 族、
SIDCC 族等）。这批令牌过期后，batchGraphql 会把请求当作未授权身份处理，
返回 gRPC 7 PERMISSION_DENIED（"Permission 'aiplatform.endpoints.predict'
denied ..."）——探测随之判 invalid，用户只能手动重抓整条 Cookie。

浏览器本身会周期性调用 RotateCookies 完成同样的续期（实测响应体
`[["identity.hfcr",600],...]` 中的 600 即约 10 分钟的轮换节奏提示）。
本模块把该动作搬到服务端：按账号定期携带完整 Cookie 调用 RotateCookies，
把响应 Set-Cookie 下发的短期令牌合并回存储的 Cookie 并原子落盘。只要
账号会话本身没有被登出/改密/主动失效，Cookie 即可长期保活。

注意：请求走该账号自己的代理链与浏览器伪装档案（与生成流量同出口）；
每次只合并短期令牌白名单内的字段，其余 Set-Cookie 一律忽略，避免把
浏览器侧的会话差异（DIVERSION_ID、__utmz 等）写坏。
"""

import asyncio
import time
from typing import Iterable

import httpx

import config
from http_io import read_limited_bytes
from impersonate import apply_browser_headers, open_upstream_client
from proxy_failover import ProxyChain
from request_tracking import redact
from runtime_state import app_state

ROTATE_COOKIES_URL = "https://accounts.google.com/RotateCookies"

# 允许自动回写的短期令牌白名单：只merge"确认会轮换"的字段。
# 长效凭证（SID/HSID/SSID/APISID/SAPISID/OSID 族）绝不触碰。
REFRESH_COOKIE_NAMES = (
    "__Secure-1PSIDTS", "__Secure-3PSIDTS",
    "__Secure-1PSIDRTS", "__Secure-3PSIDRTS",
    "SIDCC", "__Secure-1PSIDCC", "__Secure-3PSIDCC",
    "NID",
)

# 轮换间隔边界：Google 实测节奏约 600s；下限 300s 防误配成高频打点。
MIN_REFRESH_INTERVAL_SECONDS = 300
DEFAULT_REFRESH_INTERVAL_SECONDS = 600

# 单次 RotateCookies 请求预算（含代理切换）；循环轮内账号间稍作间隔。
_ROTATE_TIMEOUT = httpx.Timeout(connect=15.0, read=30.0, write=15.0, pool=15.0)
ROTATE_TOTAL_TIMEOUT_SECONDS = 45.0
MAX_ROTATE_RESPONSE_BYTES = 64 * 1024
_ACCOUNT_GAP_SECONDS = 1.0
_STARTUP_DELAY_SECONDS = 15

# 自动和手动共用锁，避免重复轮换同一批凭证。
_loop_lock = asyncio.Lock()
_settings_changed = asyncio.Event()


class RefreshInProgressError(RuntimeError):
    """A manual or automatic refresh round already owns the credentials."""


def notify_settings_changed() -> None:
    """Wake the loop on its owning event loop after a console settings save."""
    _settings_changed.set()


def refresh_interval_seconds() -> float:
    """当前生效的轮换间隔（秒），下限钳位到 MIN_REFRESH_INTERVAL_SECONDS。"""
    try:
        raw = app_state.get_setting("cookie_refresh_interval_seconds",
                                    DEFAULT_REFRESH_INTERVAL_SECONDS)
        value = float(raw)
    except Exception:
        value = float(DEFAULT_REFRESH_INTERVAL_SECONDS)
    if value != value or value in (float("inf"), float("-inf")):  # NaN/Inf
        value = float(DEFAULT_REFRESH_INTERVAL_SECONDS)
    return max(MIN_REFRESH_INTERVAL_SECONDS, value)


def auto_refresh_enabled() -> bool:
    try:
        return bool(app_state.get_setting("cookie_auto_refresh", True))
    except Exception:
        return False


def parse_set_cookie_header(value: str) -> tuple[str, str]:
    """从单条 Set-Cookie 头提取 (name, value)（取首属性，忽略 Path/Secure 等）。"""
    head = (value or "").split(";", 1)[0].strip()
    if "=" not in head:
        return "", ""
    name, val = head.split("=", 1)
    return name.strip(), val.strip()


def merge_refreshed_cookies(cookie_str: str, set_cookie_values: Iterable[str]) -> tuple[str, list[str]]:
    """把 Set-Cookie 下发的短期令牌合并回 Cookie 字符串（纯函数，供离线回归）。

    规则：
    - 仅处理 REFRESH_COOKIE_NAMES 白名单内的字段，其余忽略；
    - 已存在则原位替换（保持字段顺序与分号格式），不存在则追加到末尾；
    - 上游值缺失/与现值相同不产生变更。

    返回 (新 cookie 字符串, 实际变化的字段名列表)。
    """
    if not cookie_str:
        return cookie_str, []
    updates: dict[str, str] = {}
    for raw in set_cookie_values or ():
        name, val = parse_set_cookie_header(raw)
        if name in REFRESH_COOKIE_NAMES and val:
            updates[name] = val
    if not updates:
        return cookie_str, []

    # 逐字段原位替换：保持原始顺序、空隙与结尾分号形态。
    parts = cookie_str.split(";")
    changed: list[str] = []
    seen: set[str] = set()
    for index, part in enumerate(parts):
        if "=" not in part:
            continue
        name = part.split("=", 1)[0].strip()
        if name in updates:
            new_val = updates[name]
            if part.split("=", 1)[1].strip() != new_val:
                lead = part[:len(part) - len(part.lstrip())]
                parts[index] = f"{lead}{name}={new_val}"
                changed.append(name)
            seen.add(name)

    # 白名单内、原串没有的字段：追加（浏览器同样会种下这些新字段），
    # 且计入 changed——否则"新种令牌"会被误判为无变更而不落盘。
    appended = [n for n in updates if n not in seen]
    cookie_out = ";".join(parts)
    if appended:
        tail = cookie_out.rstrip()
        trailing_semicolon = tail.endswith(";")
        extra = "; ".join(f"{n}={updates[n]}" for n in appended)
        cookie_out = f"{tail.rstrip('; ')}; {extra}" if tail else extra
        if trailing_semicolon:
            cookie_out += ";"
        changed = changed + appended
    return cookie_out, changed


def _extract_set_cookie_list(headers) -> list[str]:
    """兼容 httpx.Headers 与 curl_cffi Headers 的多值 Set-Cookie 提取。"""
    getter = getattr(headers, "get_list", None)
    if callable(getter):
        try:
            values = getter("set-cookie")
            if values:
                return [str(v) for v in values]
        except Exception:
            pass
    try:
        return [str(v) for k, v in headers.multi_items()
                if str(k).lower() == "set-cookie"]
    except Exception:
        return []


async def rotate_account(record: dict) -> dict:
    """对单个 Cookie 账号执行一次 RotateCookies 续期。

    返回 {"id", "label", "status": refreshed|unchanged|error,
          "changed": [...], "detail", "elapsed_ms"}。
    探测口径之外的独立网络动作：失败不隔离账号、不影响生成流量。
    """
    started = time.monotonic()
    account_id = str(record.get("id", "") or "")
    label = str(record.get("note", "") or "") or account_id
    cookie = str(record.get("cookie", "") or "")
    if not account_id or not cookie:
        return {"id": account_id, "label": label, "status": "error",
                "changed": [], "detail": "账号缺少 ID 或 Cookie，跳过。", "elapsed_ms": 0}

    # 账号专属代理（空列表则继承全局），与生成流量同一 ProxyChain 语义。
    chain = ProxyChain(record)
    options = {"timeout": _ROTATE_TIMEOUT, "follow_redirects": False,
               "trust_env": False, "_proxy_chain": chain}
    if config.SSL_CERT_FILE:
        options["verify"] = config.SSL_CERT_FILE

    headers = apply_browser_headers({
        "content-type": "application/json",
        "cookie": cookie,
        "origin": "https://accounts.google.com",
        "referer": "https://accounts.google.com/",
    })

    try:
        # Bound all proxy attempts and the decoded body, including curl's
        # streaming path. A read timeout alone does not stop a trickling peer.
        async with asyncio.timeout(ROTATE_TOTAL_TIMEOUT_SECONDS):
            async with open_upstream_client(options) as client:
                async with client.stream("POST", ROTATE_COOKIES_URL, headers=headers, json=[]) as response:
                    status_code = response.status_code
                    set_cookies = _extract_set_cookie_list(response.headers)
                    if status_code == 200:
                        await read_limited_bytes(response, MAX_ROTATE_RESPONSE_BYTES)
    except Exception as exc:
        return {"id": account_id, "label": label, "status": "error", "changed": [],
                "detail": redact(f"网络/代理错误：{type(exc).__name__}: {exc}")[:400],
                "elapsed_ms": round((time.monotonic() - started) * 1000)}

    if status_code != 200:
        return {"id": account_id, "label": label, "status": "error", "changed": [],
                "detail": f"RotateCookies 返回 HTTP {status_code}，本轮跳过。",
                "elapsed_ms": round((time.monotonic() - started) * 1000)}

    new_cookie, changed = merge_refreshed_cookies(cookie, set_cookies)
    elapsed = round((time.monotonic() - started) * 1000)
    if not changed:
        return {"id": account_id, "label": label, "status": "unchanged", "changed": [],
                "detail": "HTTP 200，上游未下发新的短期令牌。",
                "elapsed_ms": elapsed}

    # 健康表按凭证摘要绑定：凭证更新后，旧凭证的失败计数/冷却会被
    # _prune_cookie_health_locked 在下次访问时自动清理，无需手动重置。
    saved = await asyncio.to_thread(
        app_state.update_cookie_account_cookie, account_id, new_cookie, expected_cookie=cookie)
    if not saved:
        return {"id": account_id, "label": label, "status": "error", "changed": [],
                "detail": "新令牌未写回：账号已删除或凭证已被更新，请使用当前凭证重试。",
                "elapsed_ms": elapsed}
    return {"id": account_id, "label": label, "status": "refreshed", "changed": changed,
            "detail": f"已续期 {len(changed)} 个短期令牌：{', '.join(changed)}。",
            "elapsed_ms": elapsed}


async def refresh_once(records: list | None = None) -> list[dict]:
    """手动/单轮入口：对所有启用账号各续期一次，返回逐账号结果。"""
    if _loop_lock.locked():
        raise RefreshInProgressError("Cookie 正在刷新，请等待当前任务完成后再试。")
    async with _loop_lock:
        return await _refresh_records(records)


async def _refresh_records(records: list | None) -> list[dict]:
    if records is None:
        records = [r for r in app_state.get_cookie_accounts() if r.get("enabled")]
    results = []
    for index, record in enumerate(records):
        if index:
            await asyncio.sleep(_ACCOUNT_GAP_SECONDS)
        try:
            results.append(await rotate_account(record))
        except Exception as exc:
            results.append({"id": str(record.get("id", "") or ""), "label": str(record.get("note", "") or ""),
                            "status": "error", "changed": [],
                            "detail": redact(f"续期异常：{type(exc).__name__}: {exc}")[:400],
                            "elapsed_ms": 0})
    _log_round(results)
    return results


def _log_round(results: list) -> None:
    for r in results:
        mark = {"refreshed": "🔄", "unchanged": "✅", "error": "⚠️"}.get(r.get("status"), "•")
        print(f"{mark} [Cookie 续期] {r.get('label', '')}：{r.get('detail', '')}"
              f"（{r.get('elapsed_ms', 0)} ms）" if r.get("status") != "unchanged" else
              f"{mark} [Cookie 续期] {r.get('label', '')}：上游未下发新的短期令牌。")


async def refresh_loop() -> None:
    """后台常驻循环：启动后先做一轮，此后按 cookie_refresh_interval_seconds 周期续期。"""
    # 启动先小憩，避开 lifespan 高峰；首轮尽快验证配置是否可用。
    await asyncio.sleep(_STARTUP_DELAY_SECONDS)
    while True:
        _settings_changed.clear()
        if auto_refresh_enabled():
            accounts = [r for r in app_state.get_cookie_accounts() if r.get("enabled")]
            if accounts:
                try:
                    await refresh_once(accounts)
                except RefreshInProgressError:
                    pass  # A manual round is already refreshing the accounts.
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    print(f"⚠️ [Cookie 续期] 本轮异常（不影响服务）：{type(exc).__name__}: {exc}")
        try:
            async with asyncio.timeout(refresh_interval_seconds()):
                await _settings_changed.wait()
        except TimeoutError:
            pass

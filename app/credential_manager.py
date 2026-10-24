"""Cookie and mixed-pool selection; each request retains one credential record.

Cookie 账号带进程内健康隔离：连续鉴权类失败（401/403/permission，见
cookie_proxy 的 cookie_error 分类）达到阈值后进入冷却期，选择器跳过该
账号；冷却到期自动回归，任何一次成功立即清零。Express 通道的
is_auth_or_billing_error 已做当轮隔离，这里补齐 Cookie 多账号池的对等
机制——失效账号不再持续承接 1/N 的流量反复报错。
"""

import random
import threading
import time

from runtime_state import app_state

_random = random.SystemRandom()

# 连续鉴权失败多少次进入冷却；冷却时长（秒）。进程内状态，重启即清零
# （Cookie 失效是会话级事实，重启不影响判定；持久化反而会挡住换新 Cookie）。
COOKIE_AUTH_FAILURE_THRESHOLD = 3
COOKIE_COOLDOWN_SECONDS = 600


class CredentialManager:
    def __init__(self):
        self._lock = threading.Lock()
        self._indices = {}
        # cookie 账号 id -> {"failures": int, "cooldown_until": epoch秒}
        self._cookie_health = {}

    def _select(self, records, pool):
        if not records:
            return None
        if app_state.get_setting("roundrobin", False):
            with self._lock:
                index = self._indices.get(pool, 0) % len(records)
                self._indices[pool] = index + 1
        else:
            index = _random.randrange(len(records))
        return dict(records[index])

    # ===== Cookie 账号健康隔离 =====

    def _cookie_cooldown_remaining(self, account_id) -> float:
        with self._lock:
            entry = self._cookie_health.get(account_id)
            if not entry:
                return 0.0
            return max(0.0, entry.get("cooldown_until", 0.0) - time.time())

    def record_cookie_auth_failure(self, account_id: str) -> None:
        """一次鉴权类失败（401/403/permission）。达到阈值进入冷却。"""
        if not account_id:
            return
        with self._lock:
            entry = self._cookie_health.setdefault(account_id, {"failures": 0, "cooldown_until": 0.0})
            if entry.get("cooldown_until", 0.0) > time.time():
                return  # 冷却中重复失败不累计，避免续期无限延长
            entry["failures"] = entry.get("failures", 0) + 1
            if entry["failures"] >= COOKIE_AUTH_FAILURE_THRESHOLD:
                entry["cooldown_until"] = time.time() + COOKIE_COOLDOWN_SECONDS
                entry["failures"] = 0  # 冷却结束后重新计数
                print(f"🚑 [凭证隔离] Cookie 账号 {account_id} 连续 {COOKIE_AUTH_FAILURE_THRESHOLD} 次"
                      f"鉴权失败，冷却 {COOKIE_COOLDOWN_SECONDS // 60} 分钟内不再选中。")

    def record_cookie_success(self, account_id: str) -> None:
        """一次成功调用：清零失败计数、解除冷却。"""
        if not account_id:
            return
        with self._lock:
            self._cookie_health.pop(account_id, None)

    def cookie_health_snapshot(self) -> dict:
        """各账号健康状态（控制台展示用）：{id: {"cooldown_remaining": 秒, "failures": n}}。"""
        now = time.time()
        with self._lock:
            return {
                account_id: {
                    "cooldown_remaining": round(max(0.0, entry.get("cooldown_until", 0.0) - now), 1),
                    "failures": entry.get("failures", 0),
                }
                for account_id, entry in self._cookie_health.items()
            }

    def get_cookie_record(self):
        # 优先选 project_id 完整的账号：凭证接口允许保存空 project_id 的启用账号
        # （与 get_mixed_record 的 `r["project_id"]` 过滤口径一致）。旧实现不过滤，
        # 多账号池里选中缺 project 的账号会在 cookie 通道给出 400，其余账号正常，
        # 表现为按比例间歇失败（2026-10 修复）。
        # 全部都缺 project 时保留旧行为（选中后由通道给出明确的 400 配置提示）。
        records = [dict(r, channel="cookie") for r in app_state.get_cookie_accounts() if r["enabled"]]
        if not records:
            return None
        complete = [r for r in records if r["project_id"]]
        pool = complete or records
        # 冷却中的账号跳过；全部冷却时退回全量（宁可再试也不拒服务——
        # 冷却是概率性优化，不是硬性准入）。
        now = time.time()
        with self._lock:
            healthy = [r for r in pool
                       if not self._cookie_health.get(r["id"])
                       or self._cookie_health[r["id"]].get("cooldown_until", 0.0) <= now]
        return self._select(healthy or pool, "cookie")

    def get_mixed_record(self, key_manager, *, express_only=False):
        records = [dict(r, channel="express") for r in key_manager.get_records()]
        if not express_only:
            now = time.time()
            cookie_records = []
            for r in app_state.get_cookie_accounts():
                if not (r["enabled"] and r["project_id"]):
                    continue
                with self._lock:
                    entry = self._cookie_health.get(r["id"])
                if entry and entry.get("cooldown_until", 0.0) > now:
                    continue
                cookie_records.append(dict(r, channel="cookie"))
            records += cookie_records
        return self._select(records, "mixed-express" if express_only else "mixed")


credential_manager = CredentialManager()

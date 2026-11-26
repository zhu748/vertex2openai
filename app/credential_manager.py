"""Cookie and mixed-pool selection; each request retains one credential record.

Cookie 账号带进程内健康隔离：连续鉴权类失败（401/403/permission，见
cookie_proxy 的 cookie_error 分类）达到阈值后进入冷却期，选择器跳过该
账号；冷却到期自动回归，任何一次成功立即清零。Express 通道的
is_auth_or_billing_error 已做当轮隔离，这里补齐 Cookie 多账号池的对等
机制——失效账号不再持续承接 1/N 的流量反复报错。
"""

import hashlib
import random
import threading
import time

from runtime_state import app_state

_random = random.SystemRandom()

# 连续鉴权失败多少次进入冷却；冷却时长（秒）。进程内状态，重启即清零
# （Cookie 失效是会话级事实，重启不影响判定；持久化反而会挡住换新 Cookie）。
COOKIE_AUTH_FAILURE_THRESHOLD = 3
COOKIE_COOLDOWN_SECONDS = 600
MAX_COOKIE_HEALTH_ENTRIES = 10_000


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

    @staticmethod
    def _credential_identity(record) -> str:
        # Health belongs to a credential/project pair, not an editable row ID.
        return hashlib.sha256(
            f"{record.get('cookie', '')}\0{record.get('project_id', '')}".encode()
        ).hexdigest()

    def _health_accounts_locked(self):
        # A pending edit is not a replacement. Compare against the published
        # snapshot under the health lock so a delayed read cannot prune newer feedback.
        records = app_state.get_cookie_accounts(committed=True)
        if not self._cookie_health:
            return records
        current = {record["id"]: record for record in records}
        for account_id, entry in list(self._cookie_health.items()):
            identity = entry.get("credential")
            if identity is not None and (
                account_id not in current
                or identity != self._credential_identity(current[account_id])
            ):
                self._cookie_health.pop(account_id, None)
        return records

    def _current_health_identity_locked(self, account_id, identity):
        return any(record["id"] == account_id and self._credential_identity(record) == identity
                   for record in self._health_accounts_locked())

    def _cookie_cooldown_remaining(self, account_id) -> float:
        with self._lock:
            entry = self._cookie_health.get(account_id)
            if not entry:
                return 0.0
            return max(0.0, entry.get("cooldown_until", 0.0) - time.time())

    def record_cookie_auth_failure(self, account_id: str, *, credential=None) -> None:
        """一次鉴权类失败（401/403/permission）。达到阈值进入冷却。"""
        if not account_id:
            return
        identity = self._credential_identity(credential) if credential is not None else None
        now = time.time()
        with self._lock:
            if identity is not None and not self._current_health_identity_locked(account_id, identity):
                return  # An old in-flight request must not quarantine a replacement.
            if account_id not in self._cookie_health:
                while len(self._cookie_health) >= MAX_COOKIE_HEALTH_ENTRIES:
                    self._cookie_health.pop(next(iter(self._cookie_health)))
            entry = self._cookie_health.setdefault(account_id, {"failures": 0, "cooldown_until": 0.0})
            if identity is not None:
                entry["credential"] = identity
            if entry.get("cooldown_until", 0.0) > now:
                return  # 冷却中重复失败不累计，避免续期无限延长
            entry["failures"] = entry.get("failures", 0) + 1
            if entry["failures"] >= COOKIE_AUTH_FAILURE_THRESHOLD:
                entry["cooldown_until"] = now + COOKIE_COOLDOWN_SECONDS
                entry["failures"] = 0  # 冷却结束后重新计数
                print(f"🚑 [凭证隔离] Cookie 账号 {account_id} 连续 {COOKIE_AUTH_FAILURE_THRESHOLD} 次"
                      f"鉴权失败，冷却 {COOKIE_COOLDOWN_SECONDS // 60} 分钟内不再选中。")

    def record_cookie_success(self, account_id: str, *, credential=None) -> None:
        """一次成功调用：清零失败计数、解除冷却。"""
        if not account_id:
            return
        with self._lock:
            entry = self._cookie_health.get(account_id)
            if not entry:
                return
            if credential is not None:
                identity = self._credential_identity(credential)
                if (entry.get("credential") not in (None, identity)
                        or not self._current_health_identity_locked(account_id, identity)):
                    return  # A stale success must not clear the new credential's failures.
            self._cookie_health.pop(account_id, None)

    def cookie_health_snapshot(self, *, for_records: list | None = None) -> dict:
        """各账号健康状态（控制台展示用）：{id: {"cooldown_remaining": 秒, "failures": n}}。"""
        now = time.time()
        with self._lock:
            if not self._cookie_health:
                return {}
            committed = self._health_accounts_locked()
            visible = self._cookie_health.keys()
            if for_records is not None:
                # A list-save receipt may describe an older ID's Cookie/project.
                # Only attach live health where the displayed identity still matches.
                shown = {r["id"]: r for r in for_records if r["id"] in visible}
                visible = {r["id"] for r in committed if (other := shown.get(r["id"])) is not None
                           and (r["cookie"], r["project_id"]) == (other["cookie"], other["project_id"])}
            return {
                account_id: {
                    "cooldown_remaining": round(max(0.0, entry.get("cooldown_until", 0.0) - now), 1),
                    "failures": entry.get("failures", 0),
                }
                for account_id, entry in self._cookie_health.items() if account_id in visible
            }

    def has_cookie_record(self) -> bool:
        """Can Cookie dispatch select a row, without reserving a round-robin/random turn?

        get_cookie_record prefers a complete, healthy row, but its existing
        all-incomplete and all-cooling fallbacks both keep the enabled pool.
        Only actual dispatch should rank the rows or update selector state.
        """
        return any(record["enabled"] for record in app_state.get_cookie_accounts(committed=True))

    def get_cookie_record(self):
        # 优先选 project_id 完整的账号：凭证接口允许保存空 project_id 的启用账号
        # （与 get_mixed_record 的 `r["project_id"]` 过滤口径一致）。旧实现不过滤，
        # 多账号池里选中缺 project 的账号会在 cookie 通道给出 400，其余账号正常，
        # 表现为按比例间歇失败（2026-10 修复）。
        # 全部都缺 project 时保留旧行为（选中后由通道给出明确的 400 配置提示）。
        # 冷却中的账号跳过；全部冷却时退回全量（宁可再试也不拒服务——
        # 冷却是概率性优化，不是硬性准入）。
        now = time.time()
        with self._lock:
            records = [dict(r, channel="cookie") for r in self._health_accounts_locked() if r["enabled"]]
            complete = [r for r in records if r["project_id"]]
            pool = complete or records
            healthy = [r for r in pool
                       if not self._cookie_health.get(r["id"])
                       or self._cookie_health[r["id"]].get("cooldown_until", 0.0) <= now]
        return self._select(healthy or pool, "cookie")

    def get_mixed_record(self, key_manager, *, express_only=False):
        records = [dict(r, channel="express") for r in key_manager.get_records()]
        if not express_only:
            now = time.time()
            with self._lock:
                cookie_records = [dict(r, channel="cookie") for r in self._health_accounts_locked()
                                  if r["enabled"] and r["project_id"]
                                  and self._cookie_health.get(r["id"], {}).get("cooldown_until", 0.0) <= now]
            records += cookie_records
        return self._select(records, "mixed-express" if express_only else "mixed")


credential_manager = CredentialManager()

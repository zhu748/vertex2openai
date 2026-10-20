"""Cookie and mixed-pool selection; each request retains one credential record."""

import random
import threading

from runtime_state import app_state

_random = random.SystemRandom()


class CredentialManager:
    def __init__(self):
        self._lock = threading.Lock()
        self._indices = {}

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
        return self._select(complete or records, "cookie")

    def get_mixed_record(self, key_manager, *, express_only=False):
        records = [dict(r, channel="express") for r in key_manager.get_records()]
        if not express_only:
            records += [dict(r, channel="cookie") for r in app_state.get_cookie_accounts()
                        if r["enabled"] and r["project_id"]]
        return self._select(records, "mixed-express" if express_only else "mixed")


credential_manager = CredentialManager()

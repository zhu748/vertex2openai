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
        records = [dict(r, channel="cookie") for r in app_state.get_cookie_accounts() if r["enabled"]]
        return self._select(records, "cookie")

    def get_mixed_record(self, key_manager, *, express_only=False):
        records = [dict(r, channel="express") for r in key_manager.get_records()]
        if not express_only:
            records += [dict(r, channel="cookie") for r in app_state.get_cookie_accounts()
                        if r["enabled"] and r["project_id"]]
        return self._select(records, "mixed-express" if express_only else "mixed")


credential_manager = CredentialManager()

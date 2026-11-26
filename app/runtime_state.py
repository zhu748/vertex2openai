import copy
import hashlib
import json
import math
import os
import tempfile
import threading
import time
from contextlib import contextmanager
from functools import lru_cache
from typing import Optional

import config as app_config
from config import state_dir as _state_dir_env  # QUA-P2-7：STATE_DIR 单点定义
import state_crypto
from model_capabilities import _strip_known_suffixes
from proxy_config import parse_proxy_list

# S-3：允许把状态落到挂载卷，避免 docker compose 重建后设置与 Cookie 全部丢失。
STATE_DIR = _state_dir_env()  # QUA-P2-7：默认值与解析口径收敛到 config.state_dir
STATE_FILE = os.path.join(STATE_DIR, "web_state.json")
_recovery_lock = threading.Lock()


class StatePersistenceError(RuntimeError):
    """A management update could not be durably committed."""


class CredentialRevisionConflictError(ValueError):
    """A console editor is attempting to overwrite a newer snapshot."""


class CookieAccountConflictError(ValueError):
    """A Cookie edit cannot safely identify unique saved accounts."""


class SettingsConflictError(ValueError):
    def __init__(self, field):
        self.field = field
        super().__init__(f"{field} 已被另一页面修改，请重新加载后检查草稿再保存。")


def _saved_model_map(state: dict) -> dict:
    settings = state.get("settings")
    overrides = settings.get("model_overrides") if isinstance(settings, dict) else None
    return overrides if isinstance(overrides, dict) else {}


def model_override_receipt(model_name: str, saved: dict) -> dict:
    """A single model's effective defaults and the fields actually owned by its editor."""
    own = saved if isinstance(saved, dict) else {}
    builtin = app_config.DEFAULT_SETTINGS["model_overrides"].get(model_name.strip(), {})
    return {"override": copy.deepcopy({**builtin, **own}), "saved_override": copy.deepcopy(own)}


def _check_expected_settings(current, expected):
    if expected is None:
        return
    for key, snapshot in expected.items():
        present = key in current
        if present != snapshot["present"] or (present and current[key] != snapshot.get("value")):
            raise SettingsConflictError(key)


class CredentialsRepository:
    """Express Key 与 Cookie 账号的 CRUD（QUA-P2-6 自 AppState 拆出）。

    存储与持久化仍归宿主 AppState：_state / _lock / _save / legacy 的
    get_project_id 经宿主动态取用（reload 会整体替换 _state，不能快照
    引用）；本类只承载凭证域的读取 / 归一 / 修订校验 / 写入编排。
    AppState 以同名方法委托转发，36 个依赖方与测试 patch 面零改动。
    """

    def __init__(self, host: "AppState"):
        self._host = host

    # ---------- Express API Key（多账号，可热编辑） ----------

    @staticmethod
    def _account_proxy_fields(item) -> dict:
        proxies = parse_proxy_list(item.get("proxies") if "proxies" in item else item.get("proxy", ""))
        return {"proxies": proxies, "proxy": proxies[0] if proxies else "",
                "proxy_fallback": bool(item.get("proxy_fallback", True))}


    @staticmethod
    def _normalize_express_record(item) -> dict:
        if isinstance(item, str):
            item = {"key": item}
        if not isinstance(item, dict):
            return {}
        key = str(item.get("key", "") or "").strip()
        if not key:
            return {}
        source = str(item.get("project_source", "") or "").strip().lower()
        if source not in ("auto", "manual"):
            source = ""
        detected = item.get("detected_at", 0)
        if (isinstance(detected, bool) or not isinstance(detected, (int, float))
                or detected < 0 or (isinstance(detected, float) and not math.isfinite(detected))):
            detected = 0
        return {
            "key": key,
            "project_id": str(item.get("project_id", "") or "").strip(),
            "project_source": source,
            "detected_at": detected,
            "note": str(item.get("note", "") or "").strip(),
            "enabled": bool(item.get("enabled", True)),
            # Key 级出站代理：支持 http/https/socks5/socks4/vless/vmess/trojan/ss/
            # hysteria2/tuic 等 URI（见 proxy_resolver.py）；空 = 跟随全局，direct = 强制直连。
            **CredentialsRepository._account_proxy_fields(item),
        }


    def _express_records(self, state: dict) -> list:
        stored = state.get("express_keys")
        if not isinstance(stored, list):
            return []
        return [rec for item in stored if (rec := self._normalize_express_record(item))]

    def get_express_keys(self) -> list:
        with self._host._lock:
            return self._express_records(self._host._committed)

    def _cookie_records(self, state: dict) -> list:
        return [rec for item in self._cookie_items(state) if (rec := self._normalize_cookie_record(item))]

    def _snapshot_from(self, state: dict, kind: str) -> tuple[list, str]:
        records = self._express_records(state) if kind == "keys" else self._cookie_records(state)
        revision_data = records
        if kind == "cookies":
            # Automatic rotation leaves metadata drafts valid; a manual edit increments the epoch.
            revision_data = [state.get("cookie_accounts_revision", 0),
                             [{k: v for k, v in rec.items() if k != "cookie"} for rec in records]]
        revision = hashlib.sha256(json.dumps(revision_data, sort_keys=True, ensure_ascii=False,
                                             separators=(",", ":")).encode()).hexdigest()
        return records, revision

    def credential_snapshot(self, kind: str, *, committed=True) -> tuple[list, str]:
        with self._host._lock:
            return self._snapshot_from(self._host._committed if committed else self._host._state, kind)

    def _check_credential_revision(self, kind: str, expected_revision: str | None) -> None:
        # Called while the mutation lock is held; optional for legacy API clients.
        if expected_revision is not None and self.credential_snapshot(kind, committed=False)[1] != expected_revision:
            raise CredentialRevisionConflictError("服务器凭证列表已更新，当前草稿已保留。请重新加载列表并合并修改后再保存。")


    def set_express_keys(self, records: list, *, expected_revision: str | None = None) -> tuple[list, str]:
        """整表覆盖保存（控制台的增删改都走这里）。

        同时打上 express_keys_initialized 标记：**用户保存空列表后，
        启动时不得再从环境变量把 Key 导回来**（否则删不掉）。
        """
        clean = self._clean_express_records(records)
        saved = []
        with self._host._write_transaction(on_saved=saved.append) as commit:
            self._check_credential_revision("keys", expected_revision)
            self._host._state["express_keys"] = clean
            self._host._state["express_keys_initialized"] = True
            commit()
        print(f"🔑 [状态管理器] 已保存 {len(clean)} 个 Express API Key（明文存于 web_state.json，权限 0600）。")
        return self._snapshot_from(saved[0], "keys")


    @staticmethod
    def _clean_express_records(records: list) -> list:
        """批量归一化 Express Key 记录并按 key 去重（保持顺序，空记录丢弃）。"""
        clean, seen = [], set()
        for item in (records or []):
            rec = CredentialsRepository._normalize_express_record(item)
            if rec and rec["key"] not in seen:
                seen.add(rec["key"])
                clean.append(rec)
        return clean


    def update_express_key_project(self, key: str, project_id: str, source: str = "auto", *,
                                   expected: dict | None = None, with_revision: bool = False) -> bool | tuple[list, str]:
        """写回某个 Key 的 Project ID（自动探测或人工覆盖）。"""
        key = (key or "").strip()
        if not key:
            return False
        project_id = (project_id or "").strip()
        source = source if source in ("auto", "manual") else ""
        saved = []
        with self._host._write_transaction(on_saved=saved.append) as commit:
            stored = self._host._state.get("express_keys")
            if not isinstance(stored, list):
                return False
            changed = False
            new_list = []
            for item in stored:
                rec = self._normalize_express_record(item)
                if not rec:
                    continue
                if rec["key"] == key:
                    if expected is not None and rec != expected:
                        return False
                    rec["project_id"] = project_id
                    rec["project_source"] = source
                    rec["detected_at"] = time.time()
                    changed = True
                new_list.append(rec)
            if not changed:
                return False
            self._host._state["express_keys"] = new_list
            commit()
        # Reload may supersede the whole snapshot. Only the two fields this
        # editor owns determine a conflict; return the winning list/revision.
        receipt = self._snapshot_from(saved[0], "keys") if with_revision else None
        records = receipt[0] if receipt is not None else self._express_records(saved[0])
        current = next((row for row in records if row["key"] == key), None)
        if not current or (current["project_id"], current["project_source"]) != (project_id, source):
            if with_revision:
                raise CredentialRevisionConflictError("Key 的项目配置已更新，请重新加载列表后再保存。")
            return False  # Background probe lost its write; keep its CAS/boolean contract.
        return receipt if receipt is not None else True


    def import_env_express_keys_once(self, env_keys: list) -> int:
        """把环境变量 VERTEX_EXPRESS_API_KEY 做**一次性**初始导入。

        只在从未保存过（没有 express_keys_initialized 标记）时执行，
        因此用户在控制台把 Key 全删光并保存后，重启也不会被环境变量重新灌回来。
        """
        result = 0
        with self._host._write_transaction() as commit:
            if self._host._state.get("express_keys_initialized"):
                return 0
            existing = self._host._state.get("express_keys")
            if isinstance(existing, list) and existing:
                self._host._state["express_keys_initialized"] = True
                result = 0
            else:
                clean = self._clean_express_records(env_keys)
                self._host._state["express_keys"] = clean
                self._host._state["express_keys_initialized"] = True
                result = len(clean)
                if clean:
                    print(f"📥 [状态管理器] 已从环境变量 VERTEX_EXPRESS_API_KEY 一次性导入 {len(clean)} 个 Key，"
                          "此后以控制台保存的列表为准。")
            commit()
        return result


    @staticmethod
    def _normalize_cookie_record(item) -> dict:
        if isinstance(item, str):
            item = {"cookie": item}
        if not isinstance(item, dict):
            return {}
        cookie = str(item.get("cookie", "") or "").strip()
        if not cookie:
            return {}
        record_id = str(item.get("id", "") or "").strip()
        if not record_id:
            record_id = _cookie_record_id(cookie)
        return {"id": record_id, "cookie": cookie,
                "project_id": str(item.get("project_id", "") or "").strip(),
                "note": str(item.get("note", "") or "").strip(),
                "enabled": bool(item.get("enabled", True)),
                **CredentialsRepository._account_proxy_fields(item)}


    @staticmethod
    def _cookie_items(state: dict) -> list:
        stored = state.get("cookie_accounts")
        if isinstance(stored, list):
            return stored
        project = state.get("google_project_id", app_config.GOOGLE_PROJECT_ID or "")
        if "google_cookie" in state:
            return [{"cookie": state["google_cookie"], "project_id": project}]
        return app_config.initial_cookie_accounts(project)

    def get_cookie_accounts(self, *, committed=True) -> list:
        with self._host._lock:
            return self._cookie_records(self._host._committed if committed else self._host._state)

    def _enabled_cookie_by_id(self, state: dict, account_id: str) -> dict | None:
        if account_id:
            for item in self._cookie_items(state):
                # Stored normalized rows have explicit IDs; don't repeatedly parse
                # unrelated proxy lists. Legacy rows derive their ID from the Cookie.
                if isinstance(item, dict) and (saved_id := str(item.get("id") or "").strip()) and saved_id != account_id:
                    continue
                rec = self._normalize_cookie_record(item)
                if rec.get("id") == account_id:
                    return rec if rec["enabled"] else None
        return None

    def get_cookie_dispatch_account(self, account_id: str) -> dict | None:
        """A queued job carries an identity, never authority to send a stale credential."""
        with self._host._lock:
            saved = self._enabled_cookie_by_id(self._host._committed, account_id)
            if saved and self._host._inflight and saved != self._enabled_cookie_by_id(self._host._state, account_id):
                # An edit of THIS account is pending. Neither its old nor provisional
                # credentials should start new traffic, and we never wait on disk I/O.
                return None
            return saved

    def _clean_cookie_accounts(self, records: list, *, reject_duplicates: bool = False) -> list:
        clean, ids, cookies = [], set(), set()
        for item in records:
            rec = self._normalize_cookie_record(item)
            if not rec:
                continue
            if rec["id"] in ids or rec["cookie"] in cookies:
                # Legacy environment imports still coalesce duplicate entries.
                # Never silently discard a row from an authoritative list save.
                if reject_duplicates:
                    raise CookieAccountConflictError("Cookie 账号 ID 或凭证重复，请检查列表后再保存。")
                continue
            ids.add(rec["id"])
            cookies.add(rec["cookie"])
            clean.append(rec)
        return clean

    def _store_cookie_accounts(self, clean: list) -> None:
        """Replace records and their revision together inside a write transaction."""
        self._host._state["cookie_accounts"] = clean
        epoch = self._host._state.get("cookie_accounts_revision", 0)
        self._host._state["cookie_accounts_revision"] = (epoch if isinstance(epoch, int) else 0) + 1
        self._host._state["cookie_accounts_initialized"] = True
        self._host._state.pop("google_cookie", None)

    def set_legacy_cookie(self, cookie: str, project_id: str = "", *, clear: bool = False) -> bool:
        """Edit the first enabled account under the same lock as manual list saves."""
        with self._host._write_transaction() as commit:
            records = self.get_cookie_accounts(committed=False)
            active = next((rec for rec in records if rec["enabled"]), None)
            if clear:
                if active is not None:
                    records.remove(active)
                if active is not None or "cookie_accounts" not in self._host._state:
                    self._store_cookie_accounts(records)
                    commit()
                return True
            if active is None:
                if not cookie:
                    return False
                if records:
                    raise CookieAccountConflictError("没有启用的 Cookie 账号，请在账号列表中选择并更新。")
                records.append(self._normalize_cookie_record(
                    {"cookie": cookie, "project_id": project_id or self._host.get_project_id(committed=False)}))
                changed = True
            else:
                changed = False
                if cookie and cookie != active["cookie"]:
                    if any(rec["cookie"] == cookie for rec in records):
                        raise CookieAccountConflictError("该 Cookie 已属于其他账号，请在账号列表中检查。")
                    active["cookie"] = cookie
                    changed = True
                if project_id and project_id != active["project_id"]:
                    active["project_id"] = project_id
                    changed = True
            if changed:
                # Manual token replacement must invalidate old full-list drafts;
                # background rotation uses update_cookie_account_cookie, not this epoch.
                self._store_cookie_accounts(records)
                commit()
            if project_id and project_id != self._host._state.get("google_project_id"):
                self._host._state["google_project_id"] = project_id
                commit()
            return True

    def set_cookie_accounts(self, records: list, *, preserve_cookie_ids: set[str] | None = None,
                            expected_revision: str | None = None) -> tuple[list, str]:
        # A missing ID is derived from the submitted Cookie. Compare the normalized
        # identities before applying blank-cookie preservation by ID or touching state.
        clean = self._clean_cookie_accounts(records, reject_duplicates=True)
        saved = []
        with self._host._write_transaction(on_saved=saved.append) as commit:
            self._check_credential_revision("cookies", expected_revision)
            if preserve_cookie_ids:
                current = {rec["id"]: rec for rec in self.get_cookie_accounts(committed=False)}
                for rec in clean:
                    if rec["id"] in preserve_cookie_ids:
                        if rec["id"] not in current:
                            raise CookieAccountConflictError("Cookie 账号已被删除，请重新加载列表后再保存。")
                        # Resolve blank-cookie preservation inside the commit
                        # lock, after any rotation or manual replacement.
                        rec["cookie"] = current[rec["id"]]["cookie"]
                if len({rec["cookie"] for rec in clean}) != len(clean):
                    raise CookieAccountConflictError("Cookie 凭证已更新并产生重复，请重新加载列表后再保存。")
            self._store_cookie_accounts(clean)
            commit()
        return self._snapshot_from(saved[0], "cookies")


    def import_env_cookie_accounts_once(self) -> int:
        with self._host._write_transaction() as commit:
            if self._host._state.get("cookie_accounts_initialized"):
                return 0
            records = self._clean_cookie_accounts(self.get_cookie_accounts(committed=False))
            self._store_cookie_accounts(records)
            commit()
        return len(records)


    def update_cookie_account_cookie(self, account_id: str, new_cookie: str,
                                    *, expected_cookie: str | None = None) -> bool:
        """只更新某个 Cookie 账号的凭证字符串（短期令牌自动续期用）。

        其余字段（project_id/代理/开关/备注）一律不动；其他字段一并改写
        必须走控制台的整表保存接口。锁内完成内存替换，_save 落盘在锁外
        （与既有方法口径一致）；账号不存在或与 expected_cookie 快照不符返回
        False。比较与替换在同一把锁内，迟到的续期不得覆盖手动更换的新凭证。
        """
        new_cookie = str(new_cookie or "").strip()
        if not account_id or not new_cookie:
            return False
        with self._host._write_transaction() as commit:
            records = self._host._state.get("cookie_accounts")
            if not isinstance(records, list) or not records:
                return False
            hit = False
            cleaned = []
            for item in records:
                rec = self._normalize_cookie_record(item)
                if not rec:
                    continue
                if rec["id"] == account_id:
                    if expected_cookie is not None and rec["cookie"] != expected_cookie:
                        return False
                    rec["cookie"] = new_cookie
                    hit = True
                cleaned.append(rec)
            if not hit:
                return False
            # A normal token refresh can converge two records, even across
            # projects. Enforce the same whole-Cookie uniqueness as manual saves
            # under this transaction; never delete/merge the other account.
            if any(rec["id"] != account_id and rec["cookie"] == new_cookie for rec in cleaned):
                raise CookieAccountConflictError("新令牌与另一已保存账号的 Cookie 重复；未更改任何账号。")
            self._host._state["cookie_accounts"] = cleaned
            self._host._state["cookie_accounts_initialized"] = True
            commit()
        return True


class AppState:
    """运行态管理器（内存优先 + 写时落盘）。

    P1-4 的改动要点：
      - 旧实现每个 getter 都调 `_load_state()` 同步读盘。全项目有 20+ 处
        get_settings/get_setting/get_effective_settings 调用，且全在 async 请求路径上
        （每压缩一张图片还要再读一次），高并发下会把事件循环串行化。
        现在只在启动和显式 reload() 时读盘。
      - 落盘改为「临时文件 + os.replace」，避免进程崩溃时把 web_state.json 写坏。
      - getter 返回深拷贝，防止调用方无意间改到 model_overrides 这层嵌套字典。
      - 文件权限 0600：里面存着完整的 Google 会话 Cookie。
    """

    def __init__(self):
        self._lock = threading.RLock()
        # 磁盘 IO 专用锁：与状态锁分离，fsync 卡顿不再阻塞事件循环上的读设置。
        self._io_lock = threading.Lock()
        # 写入序号：并发 _save 时防止旧快照覆盖新快照（见 _save）。
        self._save_seq = 0
        self._persisted_seq = 0
        # Only mutators wait for a pending transaction. Runtime and console
        # readers use the published snapshot, never a draft still awaiting I/O.
        self._saved_all = threading.Condition(self._lock)
        self._inflight: set = set()
        self._state = self._read_from_disk() or {}
        self._clean_orphan_temp_files()
        self._committed = copy.deepcopy(self._state)
        # PRF-P1-3：get_effective_settings 的修订号 + 每模型缓存。旧实现
        # 在每个请求上被调 2-4 次，base 为空时每次都全量 deepcopy 含
        # model_overrides 的整棵设置树（约 90µs，全部发生在 RLock 内，
        # Cookie 路径还在媒体线程再次竞争同一把锁）。设置只在控制台保存
        # 时变化：命中缓存直接返回浅拷贝，未命中才全量合成。
        # Publish/invalidate this model cache only when the durable settings
        # change, so a pending write or its failure has no effect on readers.
        self._settings_rev = 0
        self._effective_cache: dict[str, dict] = {}
        # QUA-P2-6：凭证 CRUD 委托给仓库（存储/持久化仍归本类）。
        self._credentials = CredentialsRepository(self)

    def _invalidate_effective_cache_locked(self) -> None:
        """设置变更时作废每模型生效设置缓存（调用方需已持 self._lock）。"""
        self._settings_rev += 1
        self._effective_cache.clear()

    # ---------- 持久化 ----------

    def _read_from_disk(self):
        """读盘并返回字典，不改动当前内存态。

        文件缺失/损坏时**不改 _state**并返回 None：调用方（reload）需要
        区分「读到了什么」与「内存里还有什么」，不能拿内存态充当磁盘真相
        （2026-11 C-11）。
        SEC-P2-2：支持可选静态加密——密文信封需 STATE_ENCRYPTION_KEY 才能读；
        有信封而无密钥/密钥错误时拒绝启动（RuntimeError），不进损坏备份路径：
        加密文件本身没坏，降级启动反而会把加密状态覆盖丢失。
        """
        # 启动自检：已设密钥但缺 cryptography 时尽早失败，别等到第一次保存。
        _crypto_error = state_crypto.self_check()
        if _crypto_error is not None:
            print(f"⛔ [状态管理器] {_crypto_error}")
            raise RuntimeError(_crypto_error)
        # Do not infer absence from a stat that can fail (or race with open).
        try:
            try:
                handle = open(STATE_FILE, encoding="utf-8")
            except FileNotFoundError:
                return None
            with handle:
                observed = os.fstat(handle.fileno())
                try:
                    data = json.load(handle)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    data = None
        except OSError as exc:
            raise StatePersistenceError("状态文件暂时无法读取；请检查存储后重试，原文件未改动。") from exc
        if not isinstance(data, dict):
            self._quarantine_malformed_state(observed)
            return None
        if state_crypto.is_encryption_envelope(data):
            try:
                data = state_crypto.decrypt_state_json(data)
            except state_crypto.StateCryptoError as exc:
                # The envelope is not disposable just because we cannot decrypt it.
                raise RuntimeError(str(exc)) from exc
        elif state_crypto.encryption_enabled():
            print("🔐 [状态管理器] 已设置 STATE_ENCRYPTION_KEY：本次保存起 "
                  "web_state.json 将以密文落盘（旧明文文件会在首次保存时被替换）。")
        try:
            os.chmod(STATE_FILE, 0o600)
        except OSError:
            pass
        return data

    @staticmethod
    def _reserve_corrupt_backup() -> str:
        # Protect existing recovery copies, including same-clock saves or a clock
        # rollback. Numbered names also preserve recency for two-copy retention.
        directory = os.path.dirname(STATE_FILE) or "."
        prefix = os.path.basename(STATE_FILE) + ".corrupt-"
        existing = (name[len(prefix):] for name in os.listdir(directory) if name.startswith(prefix))
        stamp = max(time.time_ns(), 1 + max((int(n) for n in existing if n.isascii() and n.isdigit()), default=0))
        while True:
            backup = os.path.join(directory, f"{prefix}{stamp:019d}")
            try:
                fd = os.open(backup, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            except FileExistsError:
                stamp += 1
                continue
            os.close(fd)
            return backup

    def _quarantine_malformed_state(self, observed: os.stat_result) -> None:
        with _recovery_lock:
            try:
                current = os.stat(STATE_FILE, follow_symlinks=False)
                if (not os.path.samestat(observed, current) or observed.st_mtime_ns != current.st_mtime_ns
                        or observed.st_size != current.st_size):
                    raise StatePersistenceError("状态文件在读取期间已变化；请重试，未移动或替换当前文件。")
                # The atomic rename preserves the source's mode. Secure it before
                # moving it, or leave it in place instead of creating a public backup.
                os.chmod(STATE_FILE, 0o600)
                backup = self._reserve_corrupt_backup()
                try:
                    os.replace(STATE_FILE, backup)
                except OSError:
                    os.unlink(backup)  # only our exclusive, empty reservation
                    raise
            except OSError as exc:
                raise StatePersistenceError("无法安全备份损坏的状态文件；请检查存储后重试，未覆盖原文件。") from exc
            self._prune_corrupt_backups(backup)
            print(f"⚠️ [状态管理器] 配置文件内容无效，已备份为 {os.path.basename(backup)}，本次使用内存模式。")

    def _prune_corrupt_backups(self, keep: str, limit: int = 2) -> None:
        """只保留最近 limit 份损坏备份：状态卷持续故障时会半写入循环，
        .corrupt-* 每次启动叠加一份且每份都含完整 Cookie，无限累积既是
        磁盘垃圾也是数据残留面（2026-10 补齐）。"""
        target_dir = os.path.dirname(STATE_FILE) or "."
        prefix = os.path.basename(STATE_FILE) + ".corrupt-"
        try:
            backups = sorted(
                (name for name in os.listdir(target_dir)
                 if name.startswith(prefix) and name != os.path.basename(keep)),
                reverse=True)  # 名含时间戳，字典序即时间序
            for name in backups[limit - 1:]:
                try:
                    os.unlink(os.path.join(target_dir, name))
                except OSError:
                    pass
        except OSError:
            pass

    def _clean_orphan_temp_files(self) -> None:
        """清理崩溃残留的临时文件：进程在 mkstemp 与 replace 之间被 kill 时
        .web_state-*.tmp 会永久滞留状态目录（含完整 Cookie），多次崩溃后
        累积成数据残留面（2026-10 补齐）。"""
        target_dir = os.path.dirname(STATE_FILE) or "."
        try:
            for name in os.listdir(target_dir):
                if name.startswith(".web_state-") and name.endswith(".tmp"):
                    try:
                        os.unlink(os.path.join(target_dir, name))
                    except OSError:
                        pass
        except OSError:
            pass

    @contextmanager
    def _write_transaction(self, *, on_saved=None):
        """Reserve the snapshot under the mutation lock; waiters never inherit a failed edit."""
        dirty = False

        def commit() -> None:
            nonlocal dirty
            dirty = True

        with self._saved_all:
            self._saved_all.wait_for(lambda: not self._inflight)
            yield commit
            if not dirty:
                return
            snapshot = copy.deepcopy(self._state)
            self._save_seq += 1
            seq = self._save_seq
            self._inflight.add(seq)
        try:
            superseding = self._save(seq, snapshot)
            if on_saved is not None:
                # A reload may have won before this snapshot reached the I/O lock.
                # Both snapshots are detached from mutable in-memory drafts.
                on_saved(snapshot if superseding is None else superseding)
        finally:
            # Also release on an early exception or a test-injected persistence backend.
            self._finish_write(seq)

    def _finish_write(self, seq: int) -> None:
        with self._saved_all:
            if seq in self._inflight:
                self._inflight.remove(seq)
                self._saved_all.notify_all()

    def _save(self, seq=None, snapshot=None) -> dict | None:
        """Publish success or roll back before admitting the next writer; reads never wait on I/O."""
        if seq is None:
            # Explicit flush (legacy callers); ordinary mutators already reserved a snapshot.
            with self._write_transaction() as commit:
                commit()
            return
        with self._io_lock:
            try:
                if seq <= self._persisted_seq:
                    # A newer reload supersedes this pending snapshot.
                    return self._committed
                self._write_snapshot(snapshot)
                with self._lock:
                    if snapshot.get("settings") != self._committed.get("settings"):
                        self._invalidate_effective_cache_locked()
                    self._persisted_seq = seq
                    self._committed = snapshot
            except Exception as exc:
                with self._lock:
                    self._state = copy.deepcopy(self._committed)
                print(f"⚠️ [状态管理器] 无法保存状态到磁盘: {exc}")
                raise StatePersistenceError(
                    "配置保存失败，请检查状态目录权限和磁盘空间；本次更新已回滚。") from exc
            finally:
                # Once the commit/rollback is visible other mutators can proceed, even
                # when this caller is delayed on the way out of the I/O section.
                self._finish_write(seq)

    @staticmethod
    def _write_snapshot(snapshot: dict) -> None:
        target_dir = os.path.dirname(STATE_FILE) or "."
        os.makedirs(target_dir, exist_ok=True)
        # SEC-P2-2：已设 STATE_ENCRYPTION_KEY 时密文落盘；
        # 加密失败（依赖缺失等）走既有回滚路径，内存不会与磁盘分叉。
        disk_payload = state_crypto.encrypt_state_json(snapshot)
        fd, tmp_path = tempfile.mkstemp(prefix=".web_state-", suffix=".tmp", dir=target_dir)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(disk_payload, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, STATE_FILE)
            # fsync 目录条目：POSIX 下 rename 只保证运行时原子，
            # 断电时目录项可能未持久化——重启后会退回旧版本且无
            # 感知；对目录 fsync 一次补齐崩溃一致性（失败不致命）。
            try:
                dir_fd = os.open(target_dir, os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except OSError:
                pass
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
        try:
            os.chmod(STATE_FILE, 0o600)   # 里面有完整 Google Cookie
        except OSError:
            pass

    def reload(self) -> None:
        """显式从磁盘重载（外部改了文件时用）。"""
        with self._io_lock:
            loaded = self._read_from_disk()
            with self._lock:
                if loaded is not None:
                    self._state = loaded
                # 提交基线取「刚读到的磁盘数据」而非内存态（2026-11 C-11）：
                # 文件缺失/损坏时不改 _state，旧写法会把含
                # 未落盘变更的内存态发布为已提交基线——此后任一次保存失败
                # 的回滚都会复活这些幽灵变更（从未真正落盘过）。读不到文件
                # 时磁盘真相为空，基线同置空，与启动时的空态一致。
                self._committed = copy.deepcopy(loaded if loaded is not None else {})
                # 作废重载前已拍下、尚未落盘的快照，避免旧保存覆盖外部编辑。
                self._save_seq += 1
                self._persisted_seq = self._save_seq
                # PRF-P1-3：磁盘重载可能换了整套设置，作废每模型缓存。
                self._invalidate_effective_cache_locked()

    def set_cookie_credentials(self, cookie: str = "", project_id: str = "") -> bool:
        return self._credentials.set_legacy_cookie(cookie, project_id)

    # ---------- 通道开关与凭证 ----------

    def enable_web_proxy(self, enabled: bool):
        self.set_channel_mode("web_proxy" if enabled else "api_key")

    def set_channel_mode(self, mode: str) -> str:
        return self._set_channel_mode(mode, initialize=False)

    def _set_channel_mode(self, mode: str, *, initialize: bool) -> str:
        if mode not in ("api_key", "web_proxy", "mixed"):
            raise ValueError("通道必须是 api_key、web_proxy 或 mixed。")
        saved = []
        with self._write_transaction(on_saved=saved.append) as commit:
            if initialize and ("upstream_mode" in self._state or "use_web_proxy" in self._state):
                return self._stored_channel_mode(self._state)
            self._state["upstream_mode"] = mode
            self._state["use_web_proxy"] = mode == "web_proxy"
            commit()
        applied = self._stored_channel_mode(saved[0])
        print(f"🔄 [状态管理器] 当前上游调度模式：{applied}")
        return applied

    @staticmethod
    def _stored_channel_mode(state: dict) -> str:
        mode = state.get("upstream_mode")
        if mode in ("api_key", "web_proxy", "mixed"):
            return mode
        if "use_web_proxy" in state:
            return "web_proxy" if state["use_web_proxy"] else "api_key"
        return app_config.UPSTREAM_MODE

    def get_channel_mode(self, *, committed=True) -> str:
        with self._lock:
            return self._stored_channel_mode(self._committed if committed else self._state)

    def initialize_channel_mode_once(self):
        self._set_channel_mode(app_config.UPSTREAM_MODE, initialize=True)

    def is_web_proxy_enabled(self) -> bool:
        with self._lock:
            return self.get_channel_mode() == "web_proxy"

    def set_google_cookie(self, cookie_str: str):
        self._credentials.set_legacy_cookie(cookie_str, clear=not cookie_str)
        print("🔄 [状态管理器] 谷歌独立 Cookie 已保存到运行状态")

    def get_google_cookie(self) -> str:
        with self._lock:
            return next((r["cookie"] for r in self.get_cookie_accounts() if r["enabled"]), "")

    def set_project_id(self, project_id: str):
        with self._write_transaction() as commit:
            self._state["google_project_id"] = project_id
            commit()
        print(f"🔄 [状态管理器] 项目 ID 已保存: {project_id}")

    def get_project_id(self, *, committed=True) -> str:
        with self._lock:
            state = self._committed if committed else self._state
            return state.get("google_project_id", app_config.GOOGLE_PROJECT_ID or "")

    # ---------- 凭证 CRUD（QUA-P2-6：实现拆至 CredentialsRepository，此处委托转发） ----------

    def get_express_keys(self) -> list:
        return self._credentials.get_express_keys()

    def credential_snapshot(self, kind: str, *, committed=True) -> tuple[list, str]:
        return self._credentials.credential_snapshot(kind, committed=committed)

    def set_express_keys(self, records: list, *, expected_revision: str | None = None,
                         with_revision: bool = False) -> list | tuple[list, str]:
        receipt = self._credentials.set_express_keys(records, expected_revision=expected_revision)
        return receipt if with_revision else receipt[0]

    def update_express_key_project(self, key: str, project_id: str, source: str = "auto", *,
                                   expected: dict | None = None, with_revision: bool = False) -> bool | tuple[list, str]:
        return self._credentials.update_express_key_project(
            key, project_id, source, expected=expected, with_revision=with_revision)

    def import_env_express_keys_once(self, env_keys: list) -> int:
        return self._credentials.import_env_express_keys_once(env_keys)

    def get_cookie_accounts(self, *, committed=True) -> list:
        return self._credentials.get_cookie_accounts(committed=committed)

    def get_cookie_dispatch_account(self, account_id: str) -> dict | None:
        return self._credentials.get_cookie_dispatch_account(account_id)

    def get_cookie_refresh_account(self, account_id: str) -> dict | None:
        return self.get_cookie_dispatch_account(account_id)

    def set_cookie_accounts(self, records: list, *, preserve_cookie_ids: set[str] | None = None,
                            expected_revision: str | None = None, with_revision: bool = False) -> list | tuple[list, str]:
        receipt = self._credentials.set_cookie_accounts(
            records, preserve_cookie_ids=preserve_cookie_ids, expected_revision=expected_revision)
        return receipt if with_revision else receipt[0]

    def import_env_cookie_accounts_once(self) -> int:
        return self._credentials.import_env_cookie_accounts_once()

    def update_cookie_account_cookie(self, account_id: str, new_cookie: str,
                                     *, expected_cookie: str | None = None) -> bool:
        return self._credentials.update_cookie_account_cookie(
            account_id, new_cookie, expected_cookie=expected_cookie)

    @staticmethod
    def _normalize_express_record(item) -> dict:
        # 类级静态别名：tests/test_wire_and_startup_audit 直接经
        # AppState._normalize_express_record 调用（QUA-P2-6 拆分保兼容）。
        return CredentialsRepository._normalize_express_record(item)

    @staticmethod
    def _normalize_cookie_record(item) -> dict:
        # 保留既有类级归一化接口。
        return CredentialsRepository._normalize_cookie_record(item)

    # ---------- 控制台可调设置 ----------

    def get_settings(self, *, committed=True) -> dict:
        """完整设置（内置默认 + 持久化覆盖），保证所有键都存在；返回深拷贝。"""
        with self._lock:
            return self._merged_settings(self._committed if committed else self._state)

    @staticmethod
    def _merged_settings(state: dict) -> dict:
        merged = copy.deepcopy(app_config.DEFAULT_SETTINGS)
        stored = state.get("settings")
        if isinstance(stored, dict):
            for k, v in stored.items():
                if k in merged:
                    if k == "model_overrides":
                        # Saved model settings override individual built-in
                        # fields. A legacy override map (or an unrelated
                        # sampling override) must not erase model defaults.
                        if isinstance(v, dict):
                            for model, override in v.items():
                                if isinstance(override, dict):
                                    merged[k].setdefault(model, {}).update(copy.deepcopy(override))
                    else:
                        merged[k] = copy.deepcopy(v)
            if "proxy_urls" not in stored and "proxy_url" in stored:
                merged["proxy_urls"] = parse_proxy_list(stored["proxy_url"])
            # Migrate legacy switches before model overrides are applied.
            # An explicitly saved modern mode always wins, including request.
            if "native_thinking_mode" not in stored:
                if stored.get("hide_thoughts"):
                    merged["native_thinking_mode"] = "off"
                elif stored.get("thinking_force_console"):
                    merged["native_thinking_mode"] = "console"
        return merged

    @staticmethod
    def _setting_from(state, key, default=None):
        stored = state.get("settings")
        value = (stored.get(key, app_config.DEFAULT_SETTINGS.get(key, default))
                 if isinstance(stored, dict) else app_config.DEFAULT_SETTINGS.get(key, default))
        # Fast scalar path for hot runtime controls; never lend a mutable tree.
        return value if value is None or isinstance(value, (str, int, float, bool)) else copy.deepcopy(value)

    def get_setting(self, key: str, default=None, *, committed=True):
        with self._lock:
            return self._setting_from(self._committed if committed else self._state, key, default)

    def get_committed_setting(self, key: str, default=None):
        """Live runtime control; never expose a draft awaiting disk I/O."""
        with self._lock:
            return self._setting_from(self._committed, key, default)

    def get_committed_settings(self, *keys: str) -> dict:
        """Read a consistent group of last-durable settings without waiting on disk I/O."""
        with self._lock:
            return {key: self._setting_from(self._committed, key) for key in keys}

    def get_raw_stored_setting(self, key: str, default=None):
        """只取**用户在控制台持久化保存过**的原始值，不做任何回退。

        与 get_setting 的区别：get_setting 在“从未保存过”时会落回
        DEFAULT_SETTINGS（其初始值可能来自环境变量），无法区分
        “保存为空”和“从未保存”。出站代理必须区分这两种情况：
        用户清空代理保存 = 明确要直连，绝不能被环境变量 PROXY_URL 抢回。
        从未保存过（返回 default/None）= 跟随环境变量初始值。
        """
        with self._lock:
            stored = self._committed.get("settings")
            if isinstance(stored, dict) and key in stored:
                return copy.deepcopy(stored[key])
            return default

    def update_settings(self, patch: dict, *, expected=None) -> dict:
        """合并更新设置，只接受已知键，返回更新后的完整设置。"""
        updated, _ = self.update_settings_with_changes(patch, (), expected=expected)
        return updated

    def update_settings_with_changes(self, patch: dict, watch, *, expected=None) -> tuple[dict, bool]:
        """Compare selected settings at serialized admission and durable completion."""
        if not isinstance(patch, dict):
            return self.get_settings(), False
        # Return this write's durable view, never a subsequently staged draft.
        saved = []
        with self._write_transaction(on_saved=saved.append) as commit:
            _check_expected_settings(self.get_settings(committed=False), expected)
            previous = self.get_committed_settings(*watch) if watch else {}
            current = self._state.get("settings")
            current = dict(current) if isinstance(current, dict) else {}
            accepted = 0
            for k, v in patch.items():
                if k in app_config.DEFAULT_SETTINGS and k != "model_overrides":
                    # 深拷贝入参：写侧存引用、读侧深拷贝的不对称会让复用可变
                    # dict/list 的调用方静默污染内存态（不触发落盘），防御性
                    # 对齐读侧口径（2026-10 补齐）。
                    current[k] = copy.deepcopy(v)
                    accepted += 1
            if "proxy_urls" in patch or "proxy_url" in patch:
                proxies = parse_proxy_list(patch.get("proxy_urls") if "proxy_urls" in patch else patch["proxy_url"])
                current["proxy_urls"] = proxies
                current["proxy_url"] = proxies[0] if proxies else ""
            self._state["settings"] = current
            commit()
        print(f"🔧 [状态管理器] 已更新 {accepted} 项运行时设置。")
        updated = self._merged_settings(saved[0])
        return updated, any(updated[key] != value for key, value in previous.items())

    # ---------- 按模型参数覆盖 ----------

    def get_model_overrides(self) -> dict:
        """Model-specific built-ins plus saved field overrides, matching execution/UI."""
        return self.get_settings()["model_overrides"]

    def get_saved_model_overrides(self) -> dict:
        """User-owned fields only; distinguish inheritance from built-in defaults."""
        with self._lock:
            return copy.deepcopy(_saved_model_map(self._committed))

    def get_model_editor_settings(self) -> tuple[dict, dict]:
        """Capabilities and both override maps must describe the same durable settings."""
        with self._lock:
            return self.get_settings(committed=True), copy.deepcopy(_saved_model_map(self._committed))

    def get_model_override_receipt(self, model_name: str) -> dict:
        with self._lock:
            saved = _saved_model_map(self._committed).get(model_name.strip(), {})
            return model_override_receipt(model_name, saved)

    def set_model_override(self, model_name: str, patch: dict, *, inherit=(), expected=None) -> dict:
        """Existing in-process callers expect just the merged override map."""
        return self.save_model_override(model_name, patch, inherit=inherit, expected=expected)["override"]

    def save_model_override(self, model_name: str, patch: dict, *, inherit=(), expected=None) -> dict:
        model_name = (model_name or "").strip()
        if not model_name or not isinstance(patch, dict):
            return model_override_receipt("", {})
        clean = {k: v for k, v in patch.items() if k in app_config.PER_MODEL_KEYS}
        applied = []
        with self._write_transaction(on_saved=applied.append) as commit:
            settings = self._state.get("settings")
            settings = dict(settings) if isinstance(settings, dict) else {}
            overrides = settings.get("model_overrides")
            overrides = dict(overrides) if isinstance(overrides, dict) else {}
            previous = overrides.get(model_name)
            previous = dict(previous) if isinstance(previous, dict) else {}
            _check_expected_settings(previous, expected)
            # Removal is explicit: null remains a valid 'do not inject' value.
            for key in inherit:
                if key in app_config.PER_MODEL_KEYS:
                    previous.pop(key, None)
            previous.update(copy.deepcopy(clean))
            if previous:
                overrides[model_name] = previous
            else:
                overrides.pop(model_name, None)
            settings["model_overrides"] = overrides
            self._state["settings"] = settings
            commit()
        receipt = model_override_receipt(model_name, _saved_model_map(applied[0]).get(model_name, {}))
        if receipt["saved_override"] != previous:
            # Explicit reload won before I/O. Keep the editor's draft and require
            # a fresh comparison instead of acknowledging a write that never landed.
            raise SettingsConflictError(model_name)
        print(f"🔧 [状态管理器] 已保存模型 {model_name} 的专属参数（{len(clean)} 项）。")
        return receipt

    def clear_model_override(self, model_name: str, *, expected: dict | None = None) -> bool:
        model_name = (model_name or "").strip()
        applied = []
        with self._write_transaction(on_saved=applied.append) as commit:
            settings = self._state.get("settings")
            overrides = settings.get("model_overrides") if isinstance(settings, dict) else None
            # A whole-model reset must also notice fields *added* after the
            # editor loaded. Compare user-owned values, not merged defaults.
            saved = overrides.get(model_name, {}) if isinstance(overrides, dict) else {}
            if expected is not None and saved != expected:
                raise SettingsConflictError(model_name)
            if not isinstance(overrides, dict) or model_name not in overrides:
                return False
            overrides.pop(model_name, None)
            settings["model_overrides"] = overrides
            self._state["settings"] = settings
            commit()
        if _saved_model_map(applied[0]).get(model_name):
            raise SettingsConflictError(model_name)
        print(f"🔧 [状态管理器] 已清除模型 {model_name} 的专属参数。")
        return True

    def get_effective_settings(self, model_name: str, base: Optional[dict] = None) -> dict:
        """该模型生效的设置：全局默认叠加该模型专属覆盖（仅 PER_MODEL_KEYS）。

        base 参数（可选）：传入 get_settings() 的结果可免掉每次全量深拷贝——
        /v1/models、/api/capabilities 这类逐模型循环调用的场景里，旧实现
        每个模型都 deepcopy 一次完整设置树（含全部 model_overrides）。
        本方法只做**顶层标量**赋值，浅拷贝入参即安全，不会改到调用方的基础。

        若不传 base，使用已提交的每模型设置。命中直接返回浅拷贝，未命中
        才复制已提交的设置树（PRF-P1-3），且不会等写盘结束。

        覆盖键查找做后缀归一：控制台按基础名（如 gemini-3.6-flash）保存的
        专属参数，在 -search 等别名后缀的请求下也要生效——旧实现拿原始
        请求名做精确查表，gemini-3.6-flash-search 匹配不到按基础名存的
        覆盖，专属采样默认 / 生图尺寸 / 比例全部静默落空（Express 通道
        经 generation_config.create_generation_config 触发；Cookie 通道自己剥了
        后缀所以没踩到）。先精确匹配（照顾手动存过后缀键的极端场景），
        再退回剥掉已知别名后缀（-search / -1k/-2k/-4k/-512 / -think-*）
        的基础名。
        """
        key = (model_name or "").strip()
        revision = None
        if base is None:
            with self._lock:
                cached = self._effective_cache.get(key)
                if cached is not None:
                    return dict(cached)
                revision, committed = self._settings_rev, self._committed
            # Committing swaps the snapshot; it never mutates a published one.
            base = self._merged_settings(committed)
        else:
            base = dict(base)
        # Legacy global transport defaults must not publish every model's
        # aliases. Only an explicit per-model choice (including built-ins) does.
        base["anti_truncation_mode"] = "off"
        overrides = base.get("model_overrides") or {}
        ov = overrides.get(key)
        if not isinstance(ov, dict):
            # 顶部导入（无循环依赖）：此函数在每请求/每模型路径上执行，
            # 旧写法的函数内 import 在热路径上反复做模块查找。
            stripped = _strip_known_suffixes(key)
            if stripped != key:
                ov = overrides.get(stripped)
        if isinstance(ov, dict):
            for k in app_config.PER_MODEL_KEYS:
                if k in ov:
                    base[k] = ov[k]
        if revision is not None:
            with self._lock:
                if revision == self._settings_rev and key not in self._effective_cache:
                    if len(self._effective_cache) >= 512:
                        self._effective_cache.pop(next(iter(self._effective_cache)))
                    # As on cache hits, callers own the top-level dict.
                    self._effective_cache[key] = dict(base)
        return base


# 单例模式导出
app_state = AppState()


# Cookie 账号去 ID 的 sha256 结果缓存（2026-11 O-9）：无 id 记录每次
# get_cookie_accounts（每请求路径）都做一次全长 sha256（50-200µs，账号
# 多时线性叠加）；以 cookie 串为键做小 LRU，哈希一次终身复用。
@lru_cache(maxsize=256)
def _cookie_record_id(cookie: str) -> str:
    return "cookie-" + hashlib.sha256(cookie.encode()).hexdigest()[:24]


# ============================================================
# 出站代理统一读取口（热改支持）。新调用使用账号候选列表 + ProxyChain；
# 两个单值 getter 保留给客户端池构造和旧调用方读取第一条。
# ============================================================
# 全局代理解析缓存（2026-11 O-3）：parse_proxy_list 的正则分段在每请求
# 路径上花 0.3~2ms 事件循环 CPU，而输入只在控制台保存设置时变化。按
# 「输入原值」（存储值或环境值）为键做单槽缓存——值相同直接复用，保存
# 动作或运行期改写 config.PROXY_URL/PROXY_URLS（测试口子）自然换键失效。
# 返回副本防调用方改写。
_PROXY_PARSE_CACHE: tuple[str | tuple[str, ...] | None, tuple[str, ...]] = (None, ())


def _cached_parse_proxy_list(stored) -> list:
    global _PROXY_PARSE_CACHE
    if not isinstance(stored, (str, list)) and stored is not None:
        return parse_proxy_list(stored)
    key = tuple(stored) if isinstance(stored, list) else stored
    cached_key, parsed = _PROXY_PARSE_CACHE
    if key != cached_key:
        if isinstance(key, tuple):
            stored = list(key)  # Parse the same snapshot used for the key.
            if not all(isinstance(item, str) for item in key):
                return parse_proxy_list(stored)  # Rare structured imports: never cache mutable objects.
        parsed = tuple(parse_proxy_list(stored))
        # Publish only a complete, successful parse. Concurrent misses may do
        # duplicate work; every caller returns its own matching snapshot.
        _PROXY_PARSE_CACHE = (key, parsed)
    return list(parsed)


def get_effective_proxy_url() -> str:
    """Legacy first-entry view of the global list; saved empty means direct."""
    proxies = get_effective_proxy_urls()
    return proxies[0] if proxies else ""


def get_effective_proxy_urls() -> list[str]:
    """Ordered global candidates; a saved empty list explicitly restores direct access."""
    stored = app_state.get_raw_stored_setting("proxy_urls")
    if stored is None:
        stored = app_state.get_raw_stored_setting("proxy_url")
    if stored is None:
        stored = app_config.PROXY_URLS if app_config.PROXY_URLS is not None else app_config.PROXY_URL
    return _cached_parse_proxy_list(stored)


def get_effective_proxy_uris_for_account(record=None) -> list[str]:
    """Account candidates precede globals. Never add an implicit direct fallback.

    An account with no proxies inherits globals. A sole direct sentinel retains
    the old force-direct meaning, regardless of the fallback checkbox.
    """
    global_proxies = get_effective_proxy_urls()
    if not isinstance(record, dict):
        return global_proxies or [""]
    own = parse_proxy_list(record.get("proxies") if "proxies" in record else record.get("proxy", ""))
    if not own:
        return global_proxies or [""]
    if own == ["direct"] or not record.get("proxy_fallback", True):
        return own
    return list(dict.fromkeys(own + global_proxies))


def get_effective_proxy_uri_for_key(key_record=None) -> str:
    """某次请求实际使用的出站代理原始 URI（Key 级优先，全局兑底）。

    优先级：
      ① Key 记录里的 proxy 字段（http/socks5/vless/vmess/trojan/ss/… 任一 URI）；
      ② 填 direct/none/直连 → 强制直连（返回空串，全局代理不生效）；
      ③ 留空 / 无记录 → 跟随全局 get_effective_proxy_url()。
    返回的仍是原始 URI（未做 sing-box 转换），消费方再交给
    proxy_resolver.resolve_proxy_uri() 拿到 httpx 可用的 URL。
    """
    value = get_effective_proxy_uris_for_account(key_record)[0]
    return "" if value == "direct" else value


def mask_proxy_url(url: str) -> str:
    """代理 URI 脱敏（多协议版）：凭据（密码/UUID）整体打码，保留协议与 host:port。

    同时覆盖标准 URL（http/socks5）与分享链接（vless/trojan/ss/…）两种形态；
    vmess（整体 base64）退化为长度标识，解析失败退化为哈希指纹。
    用于启动日志、/healthz 等对外输出，避免凭据落进日志采集或监控截图。
    """
    try:
        from proxy_resolver import mask_proxy_uri
        return mask_proxy_uri(url)
    except Exception:
        return "(已配置，格式无法解析)"


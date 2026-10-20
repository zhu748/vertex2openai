import copy
import hashlib
import json
import os
import tempfile
import threading
import time
from typing import Optional

import config as app_config
from credential_config import parse_cookie_records
from proxy_config import parse_proxy_list

# S-3：允许把状态落到挂载卷，避免 docker compose 重建后设置与 Cookie 全部丢失。
STATE_DIR = os.environ.get("STATE_DIR", ".")
STATE_FILE = os.path.join(STATE_DIR, "web_state.json")


class StatePersistenceError(RuntimeError):
    """A management update could not be durably committed."""


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
        self._state = {}
        self._load_from_disk()
        self._committed = copy.deepcopy(self._state)

    # ---------- 持久化 ----------

    def _load_from_disk(self) -> None:
        if not os.path.exists(STATE_FILE):
            return
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                # 顶层不是 JSON 对象（如被截断成数组/字符串）同样按损坏处理，
                # 否则降级内存模式后的下一次 _save() 会把它静默覆盖掉。
                raise ValueError("文件顶层不是 JSON 对象")
            self._state.update(data)
            # 旧版本创建的文件可能是 0644（里面有完整 Google Cookie），
            # 加载后立即收紧；否则要等到下一次 _save 才被 chmod 修复。
            try:
                os.chmod(STATE_FILE, 0o600)
            except OSError:
                pass
        except Exception as e:
            # 损坏/不可读：先把原文件改名备份再降级为内存模式。不备份的话，
            # 用户在控制台改任何一项设置都会触发 _save() 用近空状态原子替换
            # 掉原文件——里面可能手工可修的 Key/Cookie 将无声丢失。
            backup = f"{STATE_FILE}.corrupt-{int(time.time())}"
            try:
                os.replace(STATE_FILE, backup)
                print(f"⚠️ [状态管理器] 配置文件无法读取（{e}），已备份为 "
                      f"{os.path.basename(backup)}，本次启动降级为内存模式。")
            except OSError:
                print(f"⚠️ [状态管理器] 配置文件无法读取且备份失败（{e}）；"
                      f"注意：下次成功保存将覆盖该文件。")

    def _save(self) -> None:
        """原子写：先写同目录临时文件再 os.replace（同一文件系统内是原子操作）。"""
        try:
            target_dir = os.path.dirname(STATE_FILE) or "."
            os.makedirs(target_dir, exist_ok=True)
            fd, tmp_path = tempfile.mkstemp(prefix=".web_state-", suffix=".tmp", dir=target_dir)
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(self._state, f, ensure_ascii=False, indent=2)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp_path, STATE_FILE)
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
            self._committed = copy.deepcopy(self._state)
        except Exception as e:
            print(f"⚠️ [状态管理器] 无法保存状态到磁盘: {e}")
            self._state = copy.deepcopy(self._committed)
            raise StatePersistenceError("配置保存失败，请检查状态目录权限和磁盘空间；本次更新已回滚。") from e

    def reload(self) -> None:
        """显式从磁盘重载（外部改了文件时用）。"""
        with self._lock:
            self._load_from_disk()
            self._committed = copy.deepcopy(self._state)

    def set_cookie_credentials(self, cookie: str = "", project_id: str = "") -> None:
        """Commit both credential fields together, so partial updates cannot leak."""
        with self._lock:
            if cookie:
                self._state["google_cookie"] = cookie
            if project_id:
                self._state["google_project_id"] = project_id
            if isinstance(self._state.get("cookie_accounts"), list):
                records = self.get_cookie_accounts()
                if records:
                    if cookie:
                        records[0]["cookie"] = cookie
                    if project_id:
                        records[0]["project_id"] = project_id
                elif cookie:
                    records = [self._normalize_cookie_record({"cookie": cookie, "project_id": project_id})]
                self._state["cookie_accounts"] = records
            self._save()

    # ---------- 通道开关与凭证 ----------

    def enable_web_proxy(self, enabled: bool):
        self.set_channel_mode("web_proxy" if enabled else "api_key")

    def set_channel_mode(self, mode: str):
        if mode not in ("api_key", "web_proxy", "mixed"):
            raise ValueError("通道必须是 api_key、web_proxy 或 mixed。")
        with self._lock:
            self._state["upstream_mode"] = mode
            self._state["use_web_proxy"] = mode == "web_proxy"
            self._save()
            print(f"🔄 [状态管理器] 上游调度模式已更新：{mode}")

    def get_channel_mode(self) -> str:
        with self._lock:
            mode = self._state.get("upstream_mode")
            if mode in ("api_key", "web_proxy", "mixed"):
                return mode
            if "use_web_proxy" in self._state:
                return "web_proxy" if self._state["use_web_proxy"] else "api_key"
            return app_config.UPSTREAM_MODE

    def initialize_channel_mode_once(self):
        with self._lock:
            if "upstream_mode" not in self._state and "use_web_proxy" not in self._state:
                self.set_channel_mode(app_config.UPSTREAM_MODE)

    def is_web_proxy_enabled(self) -> bool:
        with self._lock:
            return self.get_channel_mode() == "web_proxy"

    def set_google_cookie(self, cookie_str: str):
        with self._lock:
            self._state["google_cookie"] = cookie_str
            if isinstance(self._state.get("cookie_accounts"), list):
                records = self.get_cookie_accounts()
                if records:
                    if cookie_str:
                        records[0]["cookie"] = cookie_str
                    else:
                        records.pop(0)
                elif cookie_str:
                    records = [self._normalize_cookie_record({"cookie": cookie_str, "project_id": self.get_project_id()})]
                self._state["cookie_accounts"] = records
            self._save()
            print("🔄 [状态管理器] 谷歌独立 Cookie 已保存到运行状态")

    def get_google_cookie(self) -> str:
        with self._lock:
            return next((r["cookie"] for r in self.get_cookie_accounts() if r["enabled"]), "")

    def set_project_id(self, project_id: str):
        with self._lock:
            self._state["google_project_id"] = project_id
            self._save()
            print(f"🔄 [状态管理器] 项目 ID 已保存: {project_id}")

    def get_project_id(self) -> str:
        with self._lock:
            return self._state.get("google_project_id", app_config.GOOGLE_PROJECT_ID or "")

    # ---------- Express API Key（多账号，可热编辑） ----------
    # 记录形态：{"key":..., "project_id":..., "project_source":"auto|manual|", 
    #            "detected_at": 时间戳, "note":..., "enabled": True,
    #            "proxy": "该 Key 专属出站代理 URI（空=跟随全局，direct=强制直连）"}
    # 一个 Key 对应一个谷歌账号/项目，所以 Project ID 必须**跟着 Key 走**，
    # 不能共用 Cookie 通道那份全局 Project ID（见 express_sdk.resolve_express_model_path）。
    # proxy 同理：多账号常来自不同网络环境，各自挂各自的出口。

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
        return {
            "key": key,
            "project_id": str(item.get("project_id", "") or "").strip(),
            "project_source": source,
            "detected_at": item.get("detected_at") or 0,
            "note": str(item.get("note", "") or "").strip(),
            "enabled": bool(item.get("enabled", True)),
            # Key 级出站代理：支持 http/https/socks5/socks4/vless/vmess/trojan/ss/
            # hysteria2/tuic 等 URI（见 proxy_resolver.py）；空 = 跟随全局，direct = 强制直连。
            **AppState._account_proxy_fields(item),
        }

    def get_express_keys(self) -> list:
        with self._lock:
            stored = self._state.get("express_keys")
            if not isinstance(stored, list):
                return []
            out = []
            for item in stored:
                rec = self._normalize_express_record(item)
                if rec:
                    out.append(rec)
            return out

    def set_express_keys(self, records: list) -> list:
        """整表覆盖保存（控制台的增删改都走这里）。

        同时打上 express_keys_initialized 标记：**用户保存空列表后，
        启动时不得再从环境变量把 Key 导回来**（否则删不掉）。
        """
        clean, seen = [], set()
        for item in (records or []):
            rec = self._normalize_express_record(item)
            if not rec or rec["key"] in seen:
                continue
            seen.add(rec["key"])
            clean.append(rec)
        with self._lock:
            self._state["express_keys"] = clean
            self._state["express_keys_initialized"] = True
            self._save()
            print(f"🔑 [状态管理器] 已保存 {len(clean)} 个 Express API Key（明文存于 web_state.json，权限 0600）。")
        return clean

    def update_express_key_project(self, key: str, project_id: str, source: str = "auto", *,
                                   expected: dict | None = None) -> bool:
        """写回某个 Key 的 Project ID（自动探测或人工覆盖）。"""
        key = (key or "").strip()
        if not key:
            return False
        with self._lock:
            stored = self._state.get("express_keys")
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
                    rec["project_id"] = (project_id or "").strip()
                    rec["project_source"] = source if source in ("auto", "manual") else ""
                    rec["detected_at"] = time.time()
                    changed = True
                new_list.append(rec)
            if changed:
                self._state["express_keys"] = new_list
                self._save()
            return changed

    def import_env_express_keys_once(self, env_keys: list) -> int:
        """把环境变量 VERTEX_EXPRESS_API_KEY 做**一次性**初始导入。

        只在从未保存过（没有 express_keys_initialized 标记）时执行，
        因此用户在控制台把 Key 全删光并保存后，重启也不会被环境变量重新灌回来。
        """
        with self._lock:
            if self._state.get("express_keys_initialized"):
                return 0
            existing = self._state.get("express_keys")
            if isinstance(existing, list) and existing:
                self._state["express_keys_initialized"] = True
                self._save()
                return 0
            clean, seen = [], set()
            for item in (env_keys or []):
                rec = self._normalize_express_record(item)
                if not rec or rec["key"] in seen:
                    continue
                seen.add(rec["key"])
                clean.append(rec)
            self._state["express_keys"] = clean
            self._state["express_keys_initialized"] = True
            self._save()
            if clean:
                print(f"📥 [状态管理器] 已从环境变量 VERTEX_EXPRESS_API_KEY 一次性导入 {len(clean)} 个 Key，"
                      "此后以控制台保存的列表为准。")
            return len(clean)

    # ---------- Cookie accounts (legacy singleton credentials are migrated once) ----------

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
            record_id = "cookie-" + hashlib.sha256(cookie.encode()).hexdigest()[:24]
        return {"id": record_id, "cookie": cookie,
                "project_id": str(item.get("project_id", "") or "").strip(),
                "note": str(item.get("note", "") or "").strip(),
                "enabled": bool(item.get("enabled", True)),
                **AppState._account_proxy_fields(item)}

    def get_cookie_accounts(self) -> list:
        with self._lock:
            stored = self._state.get("cookie_accounts")
            if not isinstance(stored, list):
                if "google_cookie" in self._state:
                    stored = [{"cookie": self._state["google_cookie"], "project_id": self.get_project_id()}]
                elif app_config.GOOGLE_COOKIES is not None:
                    stored = app_config.ENV_COOKIE_ACCOUNTS
                elif app_config.GOOGLE_COOKIE:
                    stored = parse_cookie_records(app_config.GOOGLE_COOKIE, self.get_project_id())
                else:
                    stored = app_config.ENV_COOKIE_ACCOUNTS
            return [rec for item in stored if (rec := self._normalize_cookie_record(item))]

    def set_cookie_accounts(self, records: list) -> list:
        clean, ids, cookies = [], set(), set()
        for item in records:
            rec = self._normalize_cookie_record(item)
            if not rec or rec["id"] in ids or rec["cookie"] in cookies:
                continue
            ids.add(rec["id"])
            cookies.add(rec["cookie"])
            clean.append(rec)
        with self._lock:
            self._state["cookie_accounts"] = clean
            self._state["cookie_accounts_initialized"] = True
            # Old credentials must not resurrect after deleting every account.
            self._state.pop("google_cookie", None)
            self._save()
        return clean

    def import_env_cookie_accounts_once(self) -> int:
        with self._lock:
            if self._state.get("cookie_accounts_initialized"):
                return 0
            records = self.get_cookie_accounts()
            self.set_cookie_accounts(records)
            return len(records)

    # ---------- 控制台可调设置 ----------

    def get_settings(self) -> dict:
        """完整设置（内置默认 + 持久化覆盖），保证所有键都存在；返回深拷贝。"""
        with self._lock:
            merged = copy.deepcopy(app_config.DEFAULT_SETTINGS)
            stored = self._state.get("settings")
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
            return merged

    def get_setting(self, key: str, default=None):
        with self._lock:
            stored = self._state.get("settings")
            if isinstance(stored, dict) and key in stored:
                value = stored[key]
            elif key in app_config.DEFAULT_SETTINGS:
                value = app_config.DEFAULT_SETTINGS[key]
            else:
                return default
            # 不可变标量直接返回（深拷贝无意义）；容器值仍深拷贝，防外部
            # 改动污染状态树。旧实现无差别深拷贝：热路径上每个 SSE 帧的
            # 限额读取、每个 chunk 的开关查询都在锁内做递归拷贝。
            # 注：bool 是 int 子类，isinstance 一并覆盖；float 含 nan 也安全。
            if value is None or isinstance(value, (str, int, float, bool)):
                return value
            return copy.deepcopy(value)

    def get_raw_stored_setting(self, key: str, default=None):
        """只取**用户在控制台持久化保存过**的原始值，不做任何回退。

        与 get_setting 的区别：get_setting 在“从未保存过”时会落回
        DEFAULT_SETTINGS（其初始值可能来自环境变量），无法区分
        “保存为空”和“从未保存”。出站代理必须区分这两种情况：
        用户清空代理保存 = 明确要直连，绝不能被环境变量 PROXY_URL 抢回。
        从未保存过（返回 default/None）= 跟随环境变量初始值。
        """
        with self._lock:
            stored = self._state.get("settings")
            if isinstance(stored, dict) and key in stored:
                return copy.deepcopy(stored[key])
            return default

    def update_settings(self, patch: dict) -> dict:
        """合并更新设置，只接受已知键，返回更新后的完整设置。"""
        if not isinstance(patch, dict):
            return self.get_settings()
        with self._lock:
            current = self._state.get("settings")
            current = dict(current) if isinstance(current, dict) else {}
            accepted = 0
            for k, v in patch.items():
                if k in app_config.DEFAULT_SETTINGS and k != "model_overrides":
                    current[k] = v
                    accepted += 1
            if "proxy_urls" in patch or "proxy_url" in patch:
                proxies = parse_proxy_list(patch.get("proxy_urls") if "proxy_urls" in patch else patch["proxy_url"])
                current["proxy_urls"] = proxies
                current["proxy_url"] = proxies[0] if proxies else ""
            self._state["settings"] = current
            self._save()
            print(f"🔧 [状态管理器] 已更新 {accepted} 项运行时设置。")
        return self.get_settings()

    # ---------- 按模型参数覆盖 ----------

    def get_model_overrides(self) -> dict:
        """Model-specific built-ins plus saved field overrides, matching execution/UI."""
        return self.get_settings()["model_overrides"]

    def set_model_override(self, model_name: str, patch: dict) -> dict:
        model_name = (model_name or "").strip()
        if not model_name or not isinstance(patch, dict):
            return {}
        clean = {k: v for k, v in patch.items() if k in app_config.PER_MODEL_KEYS}
        with self._lock:
            settings = self._state.get("settings")
            settings = dict(settings) if isinstance(settings, dict) else {}
            overrides = settings.get("model_overrides")
            overrides = dict(overrides) if isinstance(overrides, dict) else {}
            previous = overrides.get(model_name)
            previous = dict(previous) if isinstance(previous, dict) else {}
            previous.update(copy.deepcopy(clean))
            overrides[model_name] = previous
            settings["model_overrides"] = overrides
            self._state["settings"] = settings
            self._save()
            print(f"🔧 [状态管理器] 已保存模型 {model_name} 的专属参数（{len(clean)} 项）。")
            return self.get_model_overrides().get(model_name, {})

    def clear_model_override(self, model_name: str) -> bool:
        model_name = (model_name or "").strip()
        with self._lock:
            settings = self._state.get("settings")
            if not isinstance(settings, dict):
                return False
            overrides = settings.get("model_overrides")
            if not isinstance(overrides, dict) or model_name not in overrides:
                return False
            overrides.pop(model_name, None)
            settings["model_overrides"] = overrides
            self._state["settings"] = settings
            self._save()
            print(f"🔧 [状态管理器] 已清除模型 {model_name} 的专属参数。")
            return True

    def get_effective_settings(self, model_name: str, base: Optional[dict] = None) -> dict:
        """该模型生效的设置：全局默认叠加该模型专属覆盖（仅 PER_MODEL_KEYS）。

        base 参数（可选）：传入 get_settings() 的结果可免掉每次全量深拷贝——
        /v1/models、/api/capabilities 这类逐模型循环调用的场景里，旧实现
        每个模型都 deepcopy 一次完整设置树（含全部 model_overrides）。
        本方法只做**顶层标量**赋值，浅拷贝入参即安全，不会改到调用方的基础。

        覆盖键查找做后缀归一：控制台按基础名（如 gemini-3.6-flash）保存的
        专属参数，在 -search 等别名后缀的请求下也要生效——旧实现拿原始
        请求名做精确查表，gemini-3.6-flash-search 匹配不到按基础名存的
        覆盖，专属采样默认 / 生图尺寸 / 比例全部静默落空（Express 通道
        经 generation_config.create_generation_config 触发；Cookie 通道自己剥了
        后缀所以没踩到）。先精确匹配（照顾手动存过后缀键的极端场景），
        再退回剥掉已知别名后缀（-search / -1k/-2k/-4k/-512 / -think-*）
        的基础名。
        """
        if base is None:
            base = self.get_settings()
        else:
            base = dict(base)
        # Legacy global transport defaults must not publish every model's
        # aliases. Only an explicit per-model choice (including built-ins) does.
        base["anti_truncation_mode"] = "off"
        overrides = base.get("model_overrides") or {}
        key = (model_name or "").strip()
        ov = overrides.get(key)
        if not isinstance(ov, dict):
            try:
                from model_capabilities import _strip_known_suffixes
                stripped = _strip_known_suffixes(key)
                if stripped != key:
                    ov = overrides.get(stripped)
            except Exception:
                ov = None
        if isinstance(ov, dict):
            for k in app_config.PER_MODEL_KEYS:
                if k in ov:
                    base[k] = ov[k]
        return base


# 单例模式导出
app_state = AppState()


# ============================================================
# 出站代理统一读取口（热改支持）。新调用使用账号候选列表 + ProxyChain；
# 两个单值 getter 保留给客户端池构造和旧调用方读取第一条。
# ============================================================
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
    return parse_proxy_list(stored)


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


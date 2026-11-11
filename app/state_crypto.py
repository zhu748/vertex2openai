"""web_state.json 的可选静态加密（SEC-P2-2，2026-11）。

动机
----
web_state.json 里明文存放完整的 Google 会话 Cookie 与 Express API Key——
主机 root、卷快照与备份系统都能直接读走长效会话，泄漏后除改密码外无
轮换手段。设置环境变量 ``STATE_ENCRYPTION_KEY`` 后，该文件改为密文
落盘：攻击面从「拿到文件即拿到 Google 账号」收敛为「必须同时拿到密钥
与文件」。README 同步标注：该文件（及加密密钥）等同 Google 账号密码，
请分开保管。

算法与格式
----------
- 加密：Fernet（AES-128-CBC + HMAC-SHA256，经 ``cryptography`` 包）。
- 密钥派生：PBKDF2-HMAC-SHA256（16 字节盐，200,000 轮）。
  口令无需预格式化；换口令 = 解密后用新口令重新保存。
- 文件格式（JSON 信封）::

      {"v": 1, "alg": "fernet-pbkdf2-sha256",
       "salt": "<b64>", "iters": 200000, "payload": "<b64 Fernet token>"}

兼容与失败语义
--------------
- 明文文件 + 已设密钥：正常读入，下次保存自动升级为密文（平滑迁移）。
- 密文信封 + 未设密钥：**拒绝启动**（StateCryptoError → RuntimeError）。
  静默降级会让一次无密钥启动把加密状态覆盖成空/明文文件。
- 密文信封 + 错误密钥：同样拒绝启动。
- ``cryptography`` 未安装且已设密钥：启动即报错并给出安装指引
  （依赖可选，未设密钥的部署完全不需要它）。

性能
----
PBKDF2 派生约 100ms，每进程至多一次：盐进程级固定（首个信封的盐被
「认领」后，重加密复用同一盐），Fernet 对象按 (口令, 盐, 轮数) 缓存。
"""

from __future__ import annotations

import base64
import json
import os

from config import env_str  # QUA-P2-7：环境读取集中层（config 不回导本模块，无环）

__all__ = [
    "StateCryptoError",
    "StateCryptoKeyError",
    "StateCryptoUnavailableError",
    "adopt_salt",
    "decrypt_state_json",
    "encrypt_state_json",
    "encryption_enabled",
    "is_encryption_envelope",
    "self_check",
]

_ALG = "fernet-pbkdf2-sha256"
_ITERATIONS = 200_000
_SALT_BYTES = 16


class StateCryptoError(Exception):
    """状态加密/解密失败（含未设密钥、依赖缺失等可修复的配置类错误）。"""


class StateCryptoUnavailableError(StateCryptoError):
    """已设置 STATE_ENCRYPTION_KEY 但环境缺 cryptography 包。"""


class StateCryptoKeyError(StateCryptoError):
    """密文信封无法解密（未设密钥 / 密钥不正确 / 信封损坏）。"""


def encryption_enabled() -> bool:
    """是否配置了静态加密密钥（不校验 cryptography 可用性）。"""
    return bool(env_str("STATE_ENCRYPTION_KEY"))


def is_encryption_envelope(data) -> bool:
    """磁盘 dict 是否为本模块写入的密文信封。"""
    return (isinstance(data, dict)
            and data.get("alg") == _ALG
            and data.get("v") == 1
            and isinstance(data.get("payload"), str))


def self_check() -> str | None:
    """启动自检：返回需要失败的错误消息，None = 正常。

    - 未设密钥：永远通过（加密纯可选）。
    - 已设密钥但缺 cryptography：给出安装指引——与其在第一次保存时
      才以回滚告终，不如启动即失败。
    """
    if not encryption_enabled():
        return None
    try:
        from cryptography.fernet import Fernet  # noqa: F401
    except ImportError:
        return (
            "已设置 STATE_ENCRYPTION_KEY 但当前环境缺少 cryptography 包，"
            "无法进行状态加密。请安装它（pip install cryptography）或取消该环境变量。"
        )
    return None


# (口令, 盐, 轮数) -> Fernet 进程级缓存；盐进程级固定（见 adopt_salt）。
_fernet_cache: dict[tuple[str, bytes, int], object] = {}
_process_salt: bytes | None = None


def adopt_salt(salt_b64: str) -> None:
    """认领已解密信封里的盐：后续重加密复用，PBKDF2 每进程只派生一次。

    复用同一盐是安全可接受的（盐的作用是防跨部署彩虹表复用，它本身就
    明文存于信封内）；换来的是每次保存不再重跑 100ms 的密钥派生。
    """
    global _process_salt
    try:
        salt = base64.urlsafe_b64decode(salt_b64)
        if len(salt) >= 8:
            _process_salt = salt
    except Exception:
        pass


def _current_salt() -> bytes:
    global _process_salt
    if _process_salt is None or len(_process_salt) < 8:
        _process_salt = os.urandom(_SALT_BYTES)
    return _process_salt


def _get_fernet(passphrase: str, salt: bytes, iterations: int):
    try:
        from cryptography.fernet import Fernet
        from cryptography.hazmat.primitives.hashes import SHA256
        from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    except ImportError as exc:
        raise StateCryptoUnavailableError(
            "已设置 STATE_ENCRYPTION_KEY 但缺少 cryptography 包，请先安装：pip install cryptography"
        ) from exc
    cache_key = (passphrase, salt, iterations)
    cached = _fernet_cache.get(cache_key)
    if cached is not None:
        return cached
    kdf = PBKDF2HMAC(algorithm=SHA256(), length=32, salt=salt, iterations=iterations)
    fernet = Fernet(base64.urlsafe_b64encode(kdf.derive(passphrase.encode("utf-8"))))
    _fernet_cache[cache_key] = fernet
    return fernet


def encrypt_state_json(snapshot: dict) -> dict:
    """把状态 dict 序列化并加密为信封 dict（未设密钥时原样返回）。"""
    if not encryption_enabled():
        return snapshot
    passphrase = env_str("STATE_ENCRYPTION_KEY")
    salt = _current_salt()
    fernet = _get_fernet(passphrase, salt, _ITERATIONS)
    plaintext = json.dumps(snapshot, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    token = fernet.encrypt(plaintext)
    return {
        "v": 1,
        "alg": _ALG,
        "salt": base64.urlsafe_b64encode(salt).decode("ascii"),
        "iters": _ITERATIONS,
        "payload": token.decode("ascii"),
    }


def decrypt_state_json(envelope: dict) -> dict:
    """解密信封还原状态 dict；任何配置/密钥问题都以 StateCryptoError 拒绝。"""
    if not is_encryption_envelope(envelope):
        raise StateCryptoKeyError("文件不是合法的状态加密信封。")
    passphrase = env_str("STATE_ENCRYPTION_KEY")
    if not passphrase:
        raise StateCryptoKeyError(
            "web_state.json 已是加密状态，但本次启动未设置 STATE_ENCRYPTION_KEY。"
            "请设置当初加密时使用的密钥后重启；服务拒绝在无密钥时启动，"
            "以防一次降级启动把加密状态覆盖丢失。"
        )
    try:
        salt = base64.urlsafe_b64decode(envelope["salt"])
        iterations = int(envelope.get("iters", _ITERATIONS))
        token = envelope["payload"].encode("ascii")
    except Exception as exc:
        raise StateCryptoKeyError(f"加密信封字段损坏：{exc}") from exc
    if iterations < 1 or iterations > 10_000_000:
        raise StateCryptoKeyError(f"加密信封的派生轮数非法：{iterations}")
    # 信封内可携带不同于当前默认的轮数（未来上调时旧文件仍可解）。
    fernet = _get_fernet(passphrase, salt, iterations)
    try:
        plaintext = fernet.decrypt(token)
        data = json.loads(plaintext.decode("utf-8"))
    except Exception as exc:
        raise StateCryptoKeyError(
            f"web_state.json 解密失败（STATE_ENCRYPTION_KEY 不正确或文件损坏）：{type(exc).__name__}"
        ) from exc
    if not isinstance(data, dict):
        raise StateCryptoKeyError("解密后的状态不是 JSON 对象，拒绝加载。")
    # 认领盐：下次保存复用，免去重复派生。
    adopt_salt(envelope["salt"])
    return data

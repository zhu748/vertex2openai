"""pytest 会话级测试基建：统一环境与共享单例隔离。

背景（2026-11 全面代码审查·测试基线章节）：30 余个测试文件在**模块导入期**
写 os.environ（STATE_DIR / API_KEY / MODELS_CONFIG_URL 各不相同），而
config.API_KEY、runtime_state.app_state、main.app、客户端池等都是进程级
单例、在首次导入时固化。pytest 先收集（导入）全部测试模块再执行，于是
「哪个文件先被导入」决定单例取值：全量运行与单文件运行行为不同，曾造成
17 项顺序依赖失败（test_audit_optimization 与 test_continued_review 的
API_KEY 互踩、console 类测试把 proxy_urls / cookie_structured_output 等
设置残留给后续模块等）。

本文件提供三层隔离，全部是测试基建改动，不触碰任何生产代码路径：

1. 会话启动（先于一切测试模块导入）：写入一组**规范环境变量**并抢先导入
   app 单例——单例取值自此不再取决于测试模块的导入顺序；
2. 收集完成后：把被测试模块导入期覆盖掉的环境变量复位为规范值，
   运行期动态读取 os.environ 的代码看到的是一致的世界；
3. 每个测试模块开始前：把共享单例（app_state、凭证管理器、Express 轮询器、
   客户端池、模型清单缓存、控制台会话、统计计数）恢复为出厂快照，
   使「全量运行中模块 N 的初始状态」等价于「单独运行模块 N」。

有意保留的语义：同一模块**内部**的跨测试全局态（文件里前一个测试留下的
设置/缓存）不做清理——那与单文件运行的行为一致，属于测试作者自己的职责。
"""
import copy
import os
import sys
import tempfile
from pathlib import Path

_APP_DIR = str(Path(__file__).resolve().parent / "app")
if _APP_DIR not in sys.path:
    sys.path.insert(0, _APP_DIR)

# ---------------------------------------------------------------------------
# 1. 规范环境：必须在导入任何 app 模块之前生效。
#    API_KEY 统一为 TEST_API_KEY（历史上有三个文件各造一个口令，是顺序
#    依赖失败的直接根源之一，已同步改为该值）。
# ---------------------------------------------------------------------------
TEST_API_KEY = "test-key"
_STATE_DIR = tempfile.mkdtemp(prefix="v2o-conftest-state-")
_CANONICAL_ENV = {
    "STATE_DIR": _STATE_DIR,
    "API_KEY": TEST_API_KEY,
    "MODELS_CONFIG_URL": "",
}


def _apply_canonical_env() -> None:
    for key, value in _CANONICAL_ENV.items():
        os.environ[key] = value


_apply_canonical_env()

# 抢先导入：config / main / runtime_state 等单例自此固定为规范环境取值，
# 测试模块导入期的 os.environ 覆盖不再影响它们。
import config  # noqa: E402,F401  isort:skip
import main  # noqa: E402,F401  isort:skip

import client_pool  # noqa: E402  isort:skip
import console_auth  # noqa: E402  isort:skip
import credential_manager  # noqa: E402  isort:skip
import impersonate  # noqa: E402  isort:skip
import logger  # noqa: E402  isort:skip
import model_loader  # noqa: E402  isort:skip
import runtime_state  # noqa: E402  isort:skip

# ---------------------------------------------------------------------------
# 2. 出厂快照：会话此时没有任何测试执行过，单例即出厂状态。
# ---------------------------------------------------------------------------
_LIVE = object()  # 深拷贝失败（锁/条件变量/事件循环绑定对象）：恢复时保留现场对象


def _snapshot_obj(obj) -> dict:
    snap = {}
    for key, value in list(obj.__dict__.items()):
        try:
            snap[key] = copy.deepcopy(value)
        except Exception as exc:
            # ENG-P2-3（2026-11 审查）：兜底触发必须显式可见——静默保留现场
            # 对象会让「跨模块隔离」退化为 no-op，2026-11 修复前的 17 项
            # 顺序依赖失败会以最难排查的方式回归。单例新增不可拷贝字段时，
            # 应显式加入本函数的白名单处理，而不是依赖这里静默吞掉。
            print(f"⚠️ [conftest] 单例字段快照失败：{obj.__class__.__name__}.{key} "
                  f"保留现场对象（{type(exc).__name__}: {exc}）。"
                  f"若该字段可安全重建，请在此显式处理，避免隔离静默失效。")
            snap[key] = _LIVE
    return snap


def _restore_obj(obj, snap: dict) -> None:
    live = obj.__dict__
    for key in list(live):
        if key not in snap:
            live.pop(key)
    for key, value in snap.items():
        if value is _LIVE:
            continue
        live[key] = copy.deepcopy(value)


_APP_STATE = runtime_state.app_state
_SNAPSHOT_APP_STATE = copy.deepcopy(_APP_STATE._state)
_SNAPSHOT_STATS = _snapshot_obj(logger.stats)
_SNAPSHOT_SINGLETONS = tuple(
    (obj, _snapshot_obj(obj))
    for obj in (
        credential_manager.credential_manager,
        main.app.state.express_key_manager,
        client_pool.express_sdk_pool,
        client_pool.passthrough_client_pool,
        client_pool._closer,
        impersonate._upstream_client_pool,
    )
)

# AppState 落盘文件族（含损坏备份与崩溃残留临时文件）。
_STATE_FILE_BASENAMES = ("web_state.json", ".web_state-")


def _clean_state_files() -> None:
    """清掉上一模块落盘的状态残留：显式 reload() 的测试必须读到出厂值。"""
    try:
        names = os.listdir(_STATE_DIR)
    except OSError:
        return
    for name in names:
        if name in ("web_state.json",) or (
            name.startswith(".web_state-") and name.endswith(".tmp")
        ) or name.startswith("web_state.json.corrupt-"):
            try:
                os.unlink(os.path.join(_STATE_DIR, name))
            except OSError:
                pass


def _reset_shared_singletons() -> None:
    _apply_canonical_env()
    # app_state：内存态回出厂（锁与序号计数保留现场对象，避免跨线程/任务引用失效）。
    with _APP_STATE._lock:
        _APP_STATE._state = copy.deepcopy(_SNAPSHOT_APP_STATE)
        _APP_STATE._committed = copy.deepcopy(_SNAPSHOT_APP_STATE)
        _APP_STATE._save_seq = 0
        _APP_STATE._persisted_seq = 0
        _APP_STATE._inflight = set()
        # PRF-P1-3：每模型生效设置缓存随内存态一并回出厂——直写 _state
        # 的复位不经过 mutator，不会触发修订号失效；不清会把上一模块的
        # 生效设置泄漏给下一模块（跨模块隔离静默退化）。
        _APP_STATE._effective_cache.clear()
        _APP_STATE._settings_rev = 0
    _clean_state_files()
    # 凭证 / 轮询器 / 客户端池 / 统计：实例字段整体回出厂快照。
    for obj, snap in _SNAPSHOT_SINGLETONS:
        _restore_obj(obj, snap)
    _restore_obj(logger.stats, _SNAPSHOT_STATS)
    # 模型清单缓存：强制下一模块重新加载（单文件运行同样从冷缓存开始）。
    model_loader._model_cache = None
    model_loader._next_refresh_allowed = 0.0
    model_loader._refresh_task = None
    # 控制台登录会话与失败计数。
    console_auth._sessions.clear()
    console_auth._login_failures.clear()


# ---------------------------------------------------------------------------
# 3. pytest 钩子。
# ---------------------------------------------------------------------------
_current_module_key = None


def pytest_collection_modifyitems(session, config, items):  # noqa: A002 —— 钩子签名固定
    # 收集（导入）阶段结束：复位被测试模块导入期覆盖的环境变量。
    _apply_canonical_env()


def pytest_runtest_setup(item):
    # 模块切换时重置共享单例。unittest 风格的用例拿不到 pytest fixture，
    # 用钩子实现「每个模块开始前 = 单独运行该模块」的初始状态。
    global _current_module_key
    try:
        key = str(item.fspath)
    except Exception:
        key = repr(item)
    if key != _current_module_key:
        _current_module_key = key
        _reset_shared_singletons()

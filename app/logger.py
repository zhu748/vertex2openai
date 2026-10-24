import atexit
import builtins
import io
import queue
import sys
import time
import asyncio
import re
import threading
from typing import List, Optional
from request_tracking import current_trace, redact

original_print = builtins.print
ANSI_ESCAPE = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')


class ProxyStats:
    def __init__(self):
        self.start_time = time.time()
        self.total_requests = 0
        self.success_requests = 0
        self.error_requests = 0
        self.retry_counts = 0
        self.prompt_tokens = 0
        self.completion_tokens = 0
        self.lock = threading.Lock()

    def increment_total(self):
        with self.lock:
            self.total_requests += 1

    def add_error(self):
        with self.lock:
            self.error_requests += 1

    def add_retry(self):
        with self.lock:
            self.retry_counts += 1
        if trace := current_trace.get():
            trace.add_retry()

    def add_success(self):
        """直接计一次成功请求（Cookie / Gemini 原生透传不走 SDK token 统计）。"""
        with self.lock:
            self.success_requests += 1

    def add_tokens(self, p_tokens, c_tokens):
        with self.lock:
            self.prompt_tokens += p_tokens
            self.completion_tokens += c_tokens
            self.success_requests += 1

    def get_json_stats(self):
        with self.lock:
            return {
                "uptime": round(time.time() - self.start_time, 2),
                "total": self.total_requests,
                "success": self.success_requests,
                "error": self.error_requests,
                "retries": self.retry_counts,
                "prompt_tokens": self.prompt_tokens,
                "completion_tokens": self.completion_tokens,
            }


stats = ProxyStats()


class SSELogger:
    """把运行日志推给控制台的 SSE 订阅者。

    P1-5 修复的三个隐患：
      - `asyncio.Queue.put_nowait` 不是线程安全的，而 push() 可能从任意线程被调用
        （图片压缩线程、to_thread 里的消息转换等）。改为 loop.call_soon_threadsafe。
      - 订阅者列表与事件序号用同一把锁，增删订阅和多线程发布保持有序。
      - 队列原本无界，慢客户端会把内存撑爆。改为有界 + 满则丢最旧。
    """

    def __init__(self, max_history: int = 100, queue_size: int = 500):
        self.max_history = max_history
        self.queue_size = queue_size
        self.history: List[tuple] = []   # [(seq, formatted_msg)]，seq 全局递增
        self._seq = 0
        self._subscribers: List[tuple] = []       # [(queue, loop)]
        self._lock = threading.Lock()

    # ---- 订阅管理 ----

    def subscribe(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=self.queue_size)
        loop = loop or asyncio.get_running_loop()
        with self._lock:
            self._subscribers.append((q, loop))
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        with self._lock:
            self._subscribers = [(sq, sl) for (sq, sl) in self._subscribers if sq is not q]

    def snapshot_history(self) -> List[tuple]:
        """返回 [(seq, msg)]；seq 供 SSE id 字段与 Last-Event-ID 增量重放使用。"""
        with self._lock:
            return list(self.history)

    # ---- 推送 ----

    @staticmethod
    def _offer(q: asyncio.Queue, msg: tuple) -> None:
        """在事件循环线程里执行：队列满时丢最旧的一条，保证新日志能进来。"""
        try:
            q.put_nowait(msg)
        except asyncio.QueueFull:
            try:
                q.get_nowait()
                q.put_nowait(msg)
            except Exception:
                pass

    def push(self, plain_text: str) -> None:
        # 超长打印截断后再推流：控制台日志面板不需要完整巨型文本，
        # 保留原长度提示便于在 Docker stdout 侧定位完整内容。
        if len(plain_text) > _MAX_PUSH_CHARS:
            plain_text = plain_text[:_MAX_PUSH_CHARS] + f"…（已截断，原长度 {len(plain_text)} 字符）"
        timestamp = time.strftime("%H:%M:%S")
        formatted_msg = f"[{timestamp}] {plain_text}"

        with self._lock:
            # 每条日志带全局递增 seq：SSE 下发 id 字段后，浏览器断线重连会
            # 自动回传 Last-Event-ID，服务端据此只重放缺失部分（旧实现重连
            # 后全量重放 100 条历史，前端无条件 append 导致日志成片重复）。
            self._seq += 1
            seq = self._seq
            self.history.append((seq, formatted_msg))
            if len(self.history) > self.max_history:
                self.history.pop(0)
            # Schedule under the sequence lock: concurrent worker threads must
            # not enqueue seq N+1 before N, or reconnect deduplication loses N.
            for q, loop in self._subscribers:
                try:
                    if not loop.is_closed():
                        loop.call_soon_threadsafe(self._offer, q, (seq, formatted_msg))
                except RuntimeError:
                    pass  # Event loop stopped while publishing.


rt_logger = SSELogger()

# 防止 print 钩子内部再触发 print 导致递归
_in_hook = threading.local()

# 推入 SSE 日志流的单条消息长度上限：trace.log 有 2000 字符截断，但
# rt_logger.push 没有——一条带完整响应体/traceback 的巨型打印会同时驻留
# history（100 条）与每个订阅者队列（500 槽），内存按订阅者数放大。
_MAX_PUSH_CHARS = 8 * 1024


# ---------------- stdout 后台写线程 ----------------
# 为什么：Docker/journald 场景下 stdout 是管道（64KB 缓冲）；当读端
# （docker logs 消费者/日志采集器）变慢或停住时，同步 print 会无限期
# **阻塞事件循环线程**——所有并发流式请求一起停摆，这是唯一具备
# “整体停机”尾风险的路径。专用写线程承担背压；队列满时丢最旧
#（与 SSELogger 的有界队列同策略）。
# 兼容性：redirect_stdout/capsys 活跃时（sys.stdout 不是导入时捕获的
# 真实 stdout）仍走同步路径，测试的输出断言时序完全不变。
_REAL_STDOUT = sys.stdout
_STDOUT_QUEUE: "queue.Queue" = queue.Queue(maxsize=1000)
_MAX_STDOUT_CHARS = 16 * 1024
_STDOUT_SENTINEL = object()
_writer_thread: Optional[threading.Thread] = None
_writer_lock = threading.Lock()


def _stdout_writer_loop() -> None:
    while True:
        item = _STDOUT_QUEUE.get()
        if item is _STDOUT_SENTINEL:
            return
        text, end, flush = item
        try:
            original_print(text, end=end, flush=flush, file=_REAL_STDOUT)
        except Exception:
            # stdout 被关闭/管道断裂：丢弃该条，钩子层已有异常隔离，
            # 不能让写线程死亡（后续条目还得有人消费）。
            pass


def _ensure_stdout_writer() -> None:
    global _writer_thread
    with _writer_lock:
        if _writer_thread is None or not _writer_thread.is_alive():
            _writer_thread = threading.Thread(
                target=_stdout_writer_loop, name="stdout-writer", daemon=True)
            _writer_thread.start()


def _enqueue_stdout(text: str, end: str, flush: bool) -> None:
    # A bounded item count alone does not bound memory: one exception can
    # contain an entire image/request body. Bound each retained log as well.
    if len(text) > _MAX_STDOUT_CHARS:
        text = text[:_MAX_STDOUT_CHARS] + f"…（已截断，原长度 {len(text)} 字符）"
    _ensure_stdout_writer()
    try:
        _STDOUT_QUEUE.put_nowait((text, end, flush))
    except queue.Full:
        # 丢最旧腾位（与控制台 SSE 流同策略）：背压场景下保住最新日志。
        try:
            _STDOUT_QUEUE.get_nowait()
            _STDOUT_QUEUE.put_nowait((text, end, flush))
        except (queue.Empty, queue.Full):
            pass


def shutdown_stdout_writer(timeout: float = 2.0) -> None:
    """排空并停止后台写线程（lifespan 关闭与 atexit 双保险）。"""
    global _writer_thread
    with _writer_lock:
        thread = _writer_thread
        _writer_thread = None
    if thread is None or not thread.is_alive():
        return
    try:
        _STDOUT_QUEUE.put_nowait(_STDOUT_SENTINEL)
    except queue.Full:
        return
    thread.join(timeout)


@atexit.register
def _drain_stdout_at_exit() -> None:
    # 进程退出前排空队列，保证 subprocess 捕获（check_project/smoke）
    # 能看到全部日志；2 秒超时防性退出路径卡死。
    shutdown_stdout_writer(2.0)


def custom_print(*args, **kwargs):
    """把业务 print 同时推到控制台日志流。

    说明（P1-5 第一步）：仍保留对 builtins.print 的接管以兼容现有 6000 行代码里的
    print 调用，但已经**移除了用正则从日志文本反解 token 数**的逻辑——
    那与中文文案强耦合，改一个字就静默失效。现在由 api_helpers._record_usage()
    直接调用 stats.add_tokens()。后续可分批把 print 迁移到 logging。
    """
    if getattr(_in_hook, "active", False):
        original_print(*args, **kwargs)
        return

    _in_hook.active = True
    try:
        raw_msg = ""
        if kwargs.get("file") is None:
            # 仅当调用方未显式指定 file 时才捕获进控制台日志流。旧实现
            # 无条件覆盖 file= ——开发者只想写 stderr 的内容被广播进 web
            # 控制台，而实际输出仍走原 file，行为分裂且有泄漏面。
            buf = io.StringIO()
            kwargs_for_buffer = dict(kwargs)
            kwargs_for_buffer["file"] = buf

            try:
                original_print(*args, **kwargs_for_buffer)
                raw_msg = buf.getvalue().strip()
            except Exception:
                pass

        try:
            trace = current_trace.get()
            # redact 幂等（四个替换互不再生匹配点），整条钩子只算一次，
            # 旧实现对同一条消息在 stdout 拷贝与推流分支各算一遍（每条
            # 4 个正则 × 全文扫描，2026-10 去重）。
            redacted = redact(raw_msg) if raw_msg else raw_msg
            end, flush = kwargs.get("end", "\n"), kwargs.get("flush", False)
            # stdout 写入改经有界队列 + 后台写线程：管道背压不再阻塞事件
            # 循环（见 _stdout_writer_loop 注释）。重定向（测试）或显式
            # file 之外的同步语义全部保留——sys.stdout 非导入时真实 stdout
            # 时说明 redirect_stdout/capsys 活跃，走原同步路径。
            via_queue = (sys.stdout is _REAL_STDOUT)
            if trace and redacted and trace.id not in redacted:
                line = f"[{trace.id}] {redacted}"
            elif redacted:
                # 无 trace 上下文（启动期/线程池 worker）的 stdout 拷贝同样脱敏：
                # 旧实现此分支原样输出，Docker 日志/采集端恰在这里拿到明文凭据
                # （与 SSE 流的 redact 口径不一致，2026-10 修复）。
                line = redacted
            else:
                line = None
            if line is not None:
                if via_queue:
                    _enqueue_stdout(line, end, flush)
                else:
                    original_print(line, end=end, flush=flush)
            else:
                original_print(*args, **kwargs)
        except Exception:
            pass

        if not redacted:
            return

        try:
            # 推流分支：ANSI 剥离作用于已脱敏文本（redact 的四个模式不会触及
            # 控制序列，等价于先剥后脱）；trace.log 内部的 redact 因幂等成为
            # 纯冗余但无害，保留其防御性。
            message = ANSI_ESCAPE.sub('', redacted)
            if trace := current_trace.get():
                if trace.id not in message:
                    message = f"[{trace.id}] {message}"
                trace.log(message)
            rt_logger.push(message)
        except Exception:
            pass
    finally:
        _in_hook.active = False


builtins.print = custom_print

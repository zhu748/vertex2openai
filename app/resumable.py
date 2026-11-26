"""Process-local bounded reply storage and reconnect delivery.

Each generation has an immutable random ID. A canonical request fingerprint
selects the latest generation for legacy retries; Idempotency-Key explicitly
identifies one logical operation. SSE event IDs contain generation + sequence,
and X-Resume-Id directly addresses the retained generation, bound to its body.

Frontend disconnects do not cancel admitted generation immediately. Repeating
the request after its disconnect grace cancels and replaces that generation;
a generation still running past that grace with no reconnected or polling
client is cancelled outright instead of collecting to the total deadline.
TTL, byte budgets and a total execution deadline bound retention. Upstream loss cannot reconstruct an
unreceived tail; it is reported as an error. ASGI send completion acknowledges
server delivery only, not application-level receipt. Buffers are not shared
between workers/replicas and do not survive a process restart.
"""

import asyncio
from contextlib import asynccontextmanager
import anyio
import hashlib
import inspect
import json
import math
import secrets
import time
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

from fastapi.responses import JSONResponse, StreamingResponse
from request_connection import ClientConnection
from stream_cleanup import await_owned_close

import config as app_config
from generation_policy import generation_setting
from runtime_state import app_state
from streaming import with_heartbeats, HEARTBEAT, HEARTBEAT_FRAME, SSE_NO_BUFFER_HEADERS
from streaming import DONE_FRAME, MAX_ERROR_MESSAGE_CHARS, SETUP_CLOSE_TIMEOUT_SECONDS, StreamDeadlineExceeded
from request_tracking import current_trace, redact
from replay_event import ReplayEvent
from retry_policy import bounded_sse_advisory
from json_encoding import json_size_offloop
from upstream_errors import google_status_detail

# 终止帧：与 OpenAI 流一致，客户端见到它即可判定「本轮完整结束」
# DONE_FRAME 由 streaming 统一导出（上方 import），此处不再重复定义。


class BufferCapacityError(RuntimeError):
    code = 503


class FreshReplayCapacityError(BufferCapacityError):
    """A new request has no retained generation and cannot allocate a receipt."""


class ResumeRequestError(ValueError):
    def __init__(self, code: int, message: str):
        self.code = code
        super().__init__(message)


def resume_cursor(request) -> tuple[Optional[str], int, bool]:
    generation = (request.headers.get("x-resume-id") or "").strip() or None
    raw = (request.headers.get("last-event-id") or "").strip()
    query = getattr(request, "query_params", {}).get("resume_from")
    if query is not None and not str(query).strip():
        # `?resume_from=`（存在但值为空）视同缺省：从 seq 0 开始。
        # 旧实现会落到 int("") 报 400「游标非法」，把无害写法当成错误。
        query = None
    if not raw and query is None:
        return generation, 0, generation is not None
    if raw and ":" in raw:
        event_generation, _, raw = raw.rpartition(":")
        if generation and generation != event_generation:
            raise ResumeRequestError(409, "续传 ID 与事件游标属于不同生成。")
        generation = event_generation
    try:
        seq = int(raw) if raw else int(str(query))
        if seq < 0:
            raise ValueError
        if raw:
            seq += 1
    except (ValueError, TypeError):
        raise ResumeRequestError(400, "续传游标必须是非负整数或服务端返回的事件 ID。") from None
    return generation, seq, True


def resume_intent(request, *, default_on: bool) -> tuple[bool, bool]:
    """Return (buffer requested, explicit reconnect); an opt-in alone is not a cursor."""
    explicit = resume_cursor(request)[2]
    preference = request.headers.get("x-resumable", "").strip().lower()
    if preference in ("0", "false", "no"):
        if explicit:
            raise ResumeRequestError(400, "X-Resumable: false 不能与续传 ID 或游标同时使用。")
        return False, False
    return default_on or explicit or preference in ("1", "true", "yes"), explicit


async def resolve_buffer(store, request, api_key: str, fingerprint: str, *, model: str, kind: str):
    """Resolve an immutable generation, or atomically admit a new request."""
    request_key, source = compute_stream_id(request.headers, api_key, fingerprint)
    fingerprint_key, _ = compute_stream_id({}, api_key, fingerprint)
    owner_key_digest = hashlib.sha256((api_key or "").encode("utf-8")).hexdigest() if api_key else ""
    generation, start, reconnect = resume_cursor(request)
    if reconnect:
        buf = await (store.get_exact(generation) if generation else store.get(request_key))
        if buf is None or buf.kind != kind:
            raise ResumeRequestError(404, "续传缓冲不存在或已过期；重新生成请移除续传 ID 与游标。")
        # 归属校验必须先于指纹校验（2026-11 C-7）：持有他人 rid 的探测者
        # 改一下请求体就能在指纹 409 与归属 404 之间区分出「rid 存在」；
        # 归属 404 不泄露存在性，放在最前。
        if buf.owner_key_digest and owner_key_digest and buf.owner_key_digest != owner_key_digest:
            # 同一 rid 不可能属于另一把 Key（rid 混入 Key+指纹），不一致说明
            # 调用者拿着别人的生成 ID 在试。
            raise ResumeRequestError(404, "续传缓冲不存在或已过期；重新生成请移除续传 ID 与游标。")
        if buf.fingerprint_key and buf.fingerprint_key != fingerprint_key:
            raise ResumeRequestError(409, "续传请求体与原请求不一致。")
        await store.require_resume_window(buf)
        if start > buf.next_seq:
            raise ResumeRequestError(409, f"续传起点 {start} 超过缓冲末尾 {buf.next_seq}。")
        _track_buffer(buf, False, request, start)
        return buf, False, start, source
    buf, new = await store.get_or_create_request(
        request_key, model=model, kind=kind, fingerprint_key=fingerprint_key,
        idempotent=_explicit_request_key(request_key))
    if owner_key_digest:
        # 新建时记录归属；命中已有缓冲时为幂等回写（同一 Key 必然得到同一摘要，
        # 老版本缓冲无此字段时补齐，空 Key 部署不动）。
        buf.owner_key_digest = owner_key_digest
    _track_buffer(buf, new, request)
    return buf, new, 0, source


def _track_buffer(buf, new, request, start=0):
    buf.last_access = time.time()
    connection = getattr(request, "scope", {}).get("state", {}).get("client_connection")
    if connection:
        buf.attach_client(connection, start)
    if trace := current_trace.get():
        if new:
            buf.trace_id = trace.id
        trace.configure(generation_id=buf.id, detached=new, parent_id=None if new else buf.trace_id)
        trace.event("buffer" if new else "resume", "后台缓冲已建立，前端断开后继续生成。" if new else
                    f"续接已有生成 {buf.id}，不会重复请求上游。", phase="preparing" if new else "resuming")


@asynccontextmanager
async def _publish_at_deadline(buf, remaining, timer):
    """Publish the terminal before asking the *actual* worker to unwind.

    A timeout around operation() alone cannot wake readers until its async
    finally has returned. This alarm never replaces the registered pump or
    pretends its transport is closed. Fast operations allocate no extra task.
    """
    loop = asyncio.get_running_loop()
    owner = asyncio.current_task()
    publishing = None
    exiting = False

    async def expire():
        if not buf.done:
            await buf.fail_deadline()  # atomic, wakes all readers immediately
        # A timely terminal wins; shutdown/grace/sweep may already be doing
        # the closing. Do not issue a second cancellation into an async finally.
        if buf._retired_by == "deadline" and not exiting and not owner.cancelling():
            timer.reschedule(loop.time())

    def wake():
        nonlocal publishing
        publishing = loop.create_task(expire())

    alarm = loop.call_later(remaining, wake)
    try:
        yield
    finally:
        exiting = True
        alarm.cancel()
        if publishing is not None:
            await await_owned_close(publishing)


async def within_request_deadline(buf, operation):
    """One deadline covers preparation, retries and all subsequent reads."""
    remaining = buf.deadline_monotonic - time.monotonic()
    try:
        if remaining <= 0:
            # Production callers pass factories. A previously constructed,
            # unstarted coroutine must not run or leak an un-awaited coroutine.
            if inspect.iscoroutine(operation) and inspect.getcoroutinestate(operation) == inspect.CORO_CREATED:
                operation.close()
            raise StreamDeadlineExceeded()
        timer = asyncio.timeout(None)
        try:
            async with timer, _publish_at_deadline(buf, remaining, timer):
                result = await (operation() if callable(operation) else operation)
        except TimeoutError as exc:
            if timer.expired():
                raise StreamDeadlineExceeded() from exc
            raise  # the upstream's own timeout, not exhaustion of this budget
        # CPU-only work can cross the wall-clock deadline without yielding to
        # asyncio's timer. A terminal accepted on time still wins over slow close.
        if (timer.expired() or time.monotonic() >= buf.deadline_monotonic) and (
                not buf.done or buf._retired_by == "deadline"):
            raise StreamDeadlineExceeded()
        return result
    except StreamDeadlineExceeded:
        # REQ-P2-3：fail 是收尸动作，必须屏蔽二次取消——否则 await buf.fail
        # 被再次制停时缓冲永远停在 running，只能等默认 30 分钟的兑底 sweep。
        with anyio.CancelScope(shield=True):
            await buf.fail_deadline()
        raise
    except asyncio.CancelledError:
        with anyio.CancelScope(shield=True):
            await buf.fail("上游任务已取消。", 503)
        raise
    except Exception as exc:
        from upstream_errors import extract_upstream_error
        code, message = extract_upstream_error(exc)
        with anyio.CancelScope(shield=True):
            await buf.fail(message, code, within_deadline=True)
        raise


async def fast_setup_completed(buf, task, window):
    # Preserve prompt HTTP setup errors without making a shorter total
    # deadline wait for the full optimization window (or for slow cleanup).
    remaining = max(0.0, buf.deadline_monotonic - time.monotonic())
    done, _ = await asyncio.wait({task}, timeout=min(window, remaining))
    return bool(done)


def buffered_setup_result(buf, task):
    """A fast setup cancelled by its watchdog is an HTTP result, not ASGI cancellation."""
    try:
        return task.result()
    except (asyncio.CancelledError, StreamDeadlineExceeded):
        if buf._retired_by != "deadline":
            raise
        return JSONResponse(status_code=buf.status_code, content=buf.payload,
                            headers={"X-Resume-Id": buf.id, **SSE_NO_BUFFER_HEADERS})


def request_timeout_seconds() -> float:
    try:
        value = float(_setting("request_timeout_seconds"))
        return max(0.01, value) if math.isfinite(value) else 1800.0
    except (ValueError, TypeError):
        return 1800.0


def disconnect_grace_seconds() -> float:
    try:
        value = float(_setting("resumable_disconnect_grace_seconds"))
        return max(0.0, min(app_config.MAX_TIMEOUT_SECONDS, value)) if math.isfinite(value) else 10.0
    except (ValueError, TypeError, OverflowError):
        return 10.0


def _byte_limit(key: str, default: int) -> int:
    # Per-buffer values are read once at creation. The shared ceiling is read
    # live through the cheap committed-scalar path, also after a budget cut.
    try:
        return max(1, int(float(_setting(key)) * 1024 * 1024))
    except (ValueError, TypeError, OverflowError):
        return default * 1024 * 1024


def _setting(key: str) -> Any:
    try:
        return generation_setting(app_state, key, app_config.DEFAULT_SETTINGS.get(key))
    except Exception:
        return app_config.DEFAULT_SETTINGS.get(key)


def is_enabled() -> bool:
    return bool(_setting("resumable_enabled"))


def image_bypass_enabled(model: str) -> bool:
    """生图模型的回复是否默认绕过续传缓冲（512MB 容器内存治理）。

    一张 4K 生图的 base64 SSE 帧合计 25MB+，落进 ResumableStore 后要
    驻留到 TTL（默认 30 分钟）才回收；单流预算 64MB / 全局 256MB 的
    默认值在 Render 免费档（512MB）上叠加上游解析链与转换链峰值
    （+150~200MB）足以 OOMKill——表现为"生图后服务崩溃重启，之后
    一段时间内持续闪崩"。生图默认直通（放弃断线重连的奢侈），内存
    充裕的部署可开 resumable_image_streams 恢复缓冲。无法识别的
    模型按文本处理（维持既有行为）。
    """
    if _setting("resumable_image_streams"):
        return False
    try:
        from model_capabilities import get_profile
        return bool(get_profile(model).get("is_image"))
    except Exception:
        return False


def ttl_seconds() -> float:
    try:
        return max(30.0, float(_setting("resumable_ttl_seconds")))
    except (TypeError, ValueError):
        return float(app_config.DEFAULT_SETTINGS["resumable_ttl_seconds"])


def delivered_ttl_seconds() -> float:
    """PRF-P2-6：已完整交付且无显式幂等键的缓冲的短保留窗口。

    mark_delivered 语义下，同指纹重发本就应重新生成（不再重放），把这类
    缓冲钉满完整 TTL（默认 30 分钟）只拉高稳态 RSS 基线、挤压 512MB
    容器的图片链峰值。短窗口（默认 120 秒）只覆盖客户端快速重试。
    """
    try:
        return max(15.0, float(_setting("resumable_delivered_ttl_seconds")))
    except (TypeError, ValueError):
        return float(app_config.DEFAULT_SETTINGS["resumable_delivered_ttl_seconds"])


def max_streams() -> int:
    try:
        return max(1, int(_setting("resumable_max_streams")))
    except (TypeError, ValueError):
        return int(app_config.DEFAULT_SETTINGS["resumable_max_streams"])


def _explicit_request_key(request_key: str) -> bool:
    """Only compute_stream_id's normalized client-key namespace is idempotent."""
    return request_key.startswith("cid-")


def compute_stream_id(headers: Any, api_key: str, fingerprint: str) -> Tuple[str, str]:
    """决定这次请求的缓冲区 ID。返回 (id, 来源说明)。

    优先用客户端显式给的幂等键——聪明的客户端可以自己控制"哪两次请求算同一次"；
    没有就用「API Key + 请求体」哈希，让**原样重发**天然命中同一缓冲区。
    """
    # X-Resume-Id identifies an existing generation, not a new idempotency key.
    # REQ-P2-4：不再把 x-request-id 当幂等键——GCP LB / Nginx / 部分 SDK 每次
    # 重试都会生成**新值**，客户端断线重发时每次都命中不同缓冲并新打上游，
    # 击穿续传语义；只有客户端显式声明的 Idempotency-Key 才可信任。
    for h in ("idempotency-key", "x-idempotency-key"):
        try:
            v = (headers.get(h) or "").strip()
        except Exception:
            v = ""
        if v:
            # 幂等键只做“同一用户 + 同一请求体”之内的显式身份：混入 API Key 与请求指纹，
            # 避免不同用户复用同一键值时串缓冲（A 的回复被 B 读到），也避免同一键配不同
            # 请求体时误重放。客户端原样重发（同键 + 同体）仍稳定命中同一缓冲。
            digest = hashlib.sha256(f"{api_key}\x00{v}\x00{fingerprint}".encode()).hexdigest()[:32]
            return f"cid-{digest}", f"客户端请求头 {h}"
    digest = hashlib.sha256(f"{api_key}\x00{fingerprint}".encode()).hexdigest()[:32]
    return f"req-{digest}", "请求体指纹"


def _replay_error_summary(error: str | dict | None) -> str | None:
    """Bound auxiliary diagnostics; stored wire frames and HTTP payloads are separate."""
    if error is None:
        return None
    if isinstance(error, dict):
        error = error.get("message")
    if not isinstance(error, str):
        return "上游返回错误。"
    if len(error) <= MAX_ERROR_MESSAGE_CHARS:
        return redact(error)[:MAX_ERROR_MESSAGE_CHARS]
    # Never feed arbitrary provider detail to the diagnostic formatter, nor
    # expose a half URL/credential whose redaction marker lies past the cut.
    end = MAX_ERROR_MESSAGE_CHARS - 1
    if not error[end].isspace():
        while end and not error[end - 1].isspace():
            end -= 1
    return redact(error[:end]).rstrip()[:MAX_ERROR_MESSAGE_CHARS - 1] + "…"


_FRAME_SIZE_CHUNK_CHARS = 64 * 1024


def _frame_size(frame: str) -> int:
    """Exact UTF-8 charge without materializing a second full-sized frame."""
    size = len(frame)
    if frame.isascii():  # O(1) for the common ASCII and base64 cases
        return size
    if size <= _FRAME_SIZE_CHUNK_CHARS:
        return len(frame.encode("utf-8"))
    total = 0
    for start in range(0, size, _FRAME_SIZE_CHUNK_CHARS):
        part = frame[start:start + _FRAME_SIZE_CHUNK_CHARS]
        total += len(part) if part.isascii() else len(part.encode("utf-8"))
    return total


class ResumableBuffer:
    """一次回复的服务端权威副本（有序、带序号、可多次读取）。"""

    def __init__(self, stream_id: str, model: str = "", kind: str = "stream"):
        self.id = stream_id
        self.model = model
        self.kind = kind                      # stream | json
        self.created_at = time.time()
        self.started_monotonic = time.monotonic()
        self.deadline_monotonic = self.started_monotonic + request_timeout_seconds()
        self.finished_at: Optional[float] = None
        self.finished_monotonic: Optional[float] = None
        self.last_access = self.created_at
        self.status = "running"               # running | done | error
        self.error: Optional[str] = None
        self.events: List[str] = []           # SSE 帧，下标即 seq
        self.payload: Optional[dict] = None   # 非流式：完整 JSON 响应体
        self.status_code: int = 200
        self.json_retry_after: Optional[str] = None  # only a validated HTTP error advisory, never arbitrary headers
        self.openai_setup_failed = False  # a pre-stream HTTP error is replayable as HTTP or numbered SSE
        # None unless native HTTP setup failed before it could return an SSE body.
        self.native_setup_headers: Optional[dict[str, str]] = None
        self.fully_delivered = False          # 是否已有读者完整读到终止帧
        self.readers = 0
        self.clients: dict[object, bool] = {}  # value: this reader can deliver terminal cargo
        self._polls = 0
        self.last_disconnect_monotonic = None
        self.last_disconnected_at = None
        self.last_disconnect_grace_seconds = None
        self._retired_by = None  # "grace" is stale; a "deadline" result is replayable
        self._response_cleanup = None
        self.owner_trace = current_trace.get()
        self.request_key = ""
        self.trace_id = None
        self.fingerprint_key = ""
        # 发起者 API Key 的 sha256 摘要：轮询端点无法携带原请求体，
        # 用它校验 /v1/resumable/{id} 的归属，防止拿到他人 rid 后跨租户读取
        # 或抢先标记交付。不存明文，摘要不可逆推原 Key。
        self.owner_key_digest = ""
        self.bytes_stored = 0
        self._account_bytes = None
        # per-buffer 限额在创建时快照：append 每个 SSE 帧都会检查，
        # 旧写法每帧都进 app_state 锁读设置（变更频率极低，纯浪费）。
        # 全局总预算（resumable_max_total_mb）仍实时读，保证放宽后立即生效。
        self.max_buffer_bytes = _byte_limit("resumable_max_buffer_mb", 64)
        self._cond = asyncio.Condition()
        # 断连续传窗口计时（见 _arm_grace_timer）：最后一个读者断开后启动，
        # 到点仍无人续接（重连 / 轮询）就取消在飞上游。store 经 _attach 注入
        # on_grace_expired 钩子；无 store 的裸缓冲（测试）保持 None 即不动作。
        self._grace_task: Optional[asyncio.Task] = None
        self.on_grace_expired = None

    # ---------- 写入端（后台泵任务持有） ----------

    async def append(self, frame: str) -> Optional[int]:
        async with self._cond:
            # 淘汰/完成后的副本必须保持不变；部分上游会在取消后仍交出最后一块。
            if self.done:
                return None
            self._check_write_deadline_locked()
            self._append_locked(frame)
            self._cond.notify_all()
            return len(self.events) - 1

    def _append_locked(self, frame: str) -> None:
        size = _frame_size(frame)
        if self.bytes_stored + size > self.max_buffer_bytes:
            raise BufferCapacityError("单次响应超过续传缓冲字节上限。")
        if self._account_bytes and not self._account_bytes(size, False):
            raise BufferCapacityError("响应缓冲总字节预算已满，请稍后重试。")
        self.bytes_stored += size
        self.events.append(frame)

    async def finish(self, status: str = "done", error: str | dict | None = None,
                     payload: Optional[dict] = None, status_code: Optional[int] = None,
                     *, terminal_frame: str | ReplayEvent | None = None, retry_after: Optional[str] = None) -> None:
        if self.done:
            return
        # Account potentially large JSON outside the condition. A watchdog can
        # publish its timeout and wake waiters while counting is still in flight.
        size = 0
        if payload is not None and time.monotonic() < self.deadline_monotonic:
            size = await json_size_offloop(payload)
            if retry_after is not None:
                size += len(retry_after)  # validated Retry-After is ASCII
        async with self._cond:
            if self.done:
                return
            self._check_write_deadline_locked()
            if terminal_frame is not None:
                # Readers and the deadline must observe the frame and its outcome together.
                self._append_locked(self.event_frame(terminal_frame))
            self.status = status
            self.error = _replay_error_summary(error)
            if payload is not None:
                fits = self.bytes_stored + size <= self.max_buffer_bytes
                if fits and self._account_bytes:
                    fits = self._account_bytes(size, False)
                if fits:
                    self.json_retry_after = retry_after
                else:
                    self.status = "error"
                    self.error = "响应超过续传缓冲字节预算。"
                    status_code = 503
                    payload = {"error": {"message": self.error, "type": "buffer_capacity", "code": 503}}
                    size = len(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
                    if self._account_bytes:
                        self._account_bytes(size, True)
                self.bytes_stored += size
                self.payload = payload
            if status_code is not None:
                self.status_code = status_code
            self._settled_locked()

    def _settled_locked(self) -> None:
        # Wall time is for displaying the result; only elapsed time determines
        # how long the receipt (and its idempotency mapping) must be retained.
        self.finished_at = time.time()
        self.finished_monotonic = time.monotonic()
        self._cancel_grace_timer()
        self._cond.notify_all()

    def event_frame(self, frame: str | ReplayEvent) -> str:
        return frame_with_id(self.next_seq, frame, self.id if self.id.startswith("rid-") else None)

    async def fail(self, message: str, code: int = 503, error_type: str = "upstream_error",
                   *, within_deadline: bool = False) -> bool:
        """Settle once, optionally rejecting an upstream error arriving after the budget."""
        async with self._cond:
            if within_deadline:
                self._check_write_deadline_locked()
            return self._fail_locked(message, code, error_type)

    async def fail_native_setup(self, payload: dict, retry_after: Optional[str]) -> None:
        """Publish one bounded HTTP failure to both not-yet-started and live SSE readers."""
        async with self._cond:
            self._check_write_deadline_locked()
            if self.done:
                return
            error = payload["error"]
            prefix = f": upstream-status {error['code']}\n"
            # Only the raw SSE note is bounded; the validated HTTP hint still
            # describes the original failure for ordinary and replay callers.
            if (comment_hint := bounded_sse_advisory(retry_after)) is not None:
                prefix += f": retry-after {comment_hint}\n"
            self._fail_locked(error["message"], error["code"], "upstream_error",
                              payload=payload, prefix=prefix)
            self.native_setup_headers = {"retry-after": retry_after} if retry_after is not None else {}

    async def fail_openai_setup(self, payload: dict, code: int, retry_after: Optional[str]) -> None:
        """Record one bounded HTTP error and its SSE equivalent before waking any reader."""
        async with self._cond:
            self._check_write_deadline_locked()
            if self.done:
                return
            error = payload["error"]
            frame_payload = ({**payload, "error": {**error, "retry_after": retry_after}}
                             if retry_after is not None else payload)
            self._fail_locked(error["message"], code, "upstream_error", payload=payload, frame_payload=frame_payload)
            self.openai_setup_failed = True
            self.json_retry_after = retry_after

    def _fail_locked(self, message, code, error_type, *, payload=None, prefix="", frame_payload=None):
        if self.done:
            return False
        if self.owner_trace:
            token = current_trace.set(self.owner_trace)
            try:
                if error_type == "superseded":
                    self.owner_trace.cancel(message)
                else:
                    self.owner_trace.fail(message, code)
            finally:
                current_trace.reset(token)
        if payload is None:
            detail = ({"status": google_status_detail(code)} if self.kind == "native" else {"type": error_type})
            payload = {"error": {"message": message[:MAX_ERROR_MESSAGE_CHARS], **detail, "code": code}}
        frames = ([self.event_frame(f"{prefix}data: {json.dumps(frame_payload or payload, ensure_ascii=False)}\n\n")]
                  if self.kind != "json" else [])
        if self.kind != "json":
            # Allocate the next sequence after the error, not the same ID.
            self.events.extend(frames)
            if self.kind != "native":
                frames.append(self.event_frame(DONE_FRAME))
                self.events.append(frames[-1])
        self.payload = payload
        size = sum(_frame_size(f) for f in frames) + _frame_size(json.dumps(payload))
        if self._account_bytes:
            self._account_bytes(size, True)
        self.bytes_stored += size
        self.status, self.error, self.status_code = "error", _replay_error_summary(message), code
        self._settled_locked()
        return True

    def _fail_deadline_locked(self) -> bool:
        if not self._fail_locked(str(StreamDeadlineExceeded()), 504, "timeout"):
            return False
        self._retired_by = "deadline"
        return True

    def _check_write_deadline_locked(self):
        if self._retired_by == "deadline" or (
                not self.done and time.monotonic() >= self.deadline_monotonic):
            self._fail_deadline_locked()
            raise StreamDeadlineExceeded()

    async def fail_deadline(self) -> bool:
        async with self._cond:
            return self._fail_deadline_locked()

    # ---------- 读取端（可以有多个、可以中途离开再回来） ----------

    def attach_client(self, connection, start=0):
        client = object()
        # An at-end OpenAI cursor pins the response through its HTTP send,
        # but it can't deliver a [DONE] already behind it. Don't erase or
        # renew another reader's grace period for this observation. Native
        # EOF, JSON bodies, and a live stream's future terminal still count.
        can_deliver = not (self.kind == "stream" and self.done and start == self.next_seq)
        self.clients[client] = can_deliver
        if can_deliver:
            self._cancel_grace_timer()
            self.last_disconnect_monotonic = None
            self.last_disconnected_at = None
            self.last_disconnect_grace_seconds = None

        def update_meta():
            if self.owner_trace:
                with self.owner_trace._lock:
                    self.owner_trace.data.update(
                        resume_clients=len(self.clients), resume_grace_seconds=self.last_disconnect_grace_seconds,
                        resume_expires_at=(self.last_disconnected_at + self.last_disconnect_grace_seconds
                                           if self.last_disconnected_at is not None else None))

        def closed(_):
            del self.clients[client]
            if (can_deliver and not self.fully_delivered and not connection.delivery_possible
                    and not any(self.clients.values())):
                self.last_disconnect_monotonic = connection.disconnected_monotonic or time.monotonic()
                self.last_disconnected_at = connection.disconnected_at or time.time()
                # Snapshot settings at the last reader's disconnect, just as
                # each upstream attempt snapshots its first-reply deadline.
                self.last_disconnect_grace_seconds = disconnect_grace_seconds()
                # 最后一个读者走了：启动续传窗口计时，到点无人续接即取消上游。
                self._arm_grace_timer()
            update_meta()
        update_meta()
        connection.listen(closed)

    def attach_poll(self, connection, wait):
        """Pin this authorized GET until it exits, without taking ownership of the pump."""
        self.last_access = time.time()
        if connection is None:
            self.note_client_activity(wait)
            return
        self._polls += 1
        self._cancel_grace_timer()
        horizon = time.monotonic() + wait

        def closed(_):
            self._polls -= 1
            # A successful short poll preserves the declared sliding window;
            # a cancelled wait must not prepay its unused 25 seconds of work.
            self.note_client_activity(horizon - time.monotonic() if connection.delivery_possible else 0)

        connection.listen(closed)

    @property
    def has_live_delivery(self):
        return bool(self.readers or self.clients or self._polls)

    def resume_expired(self):
        return self._retired_by == "grace" or (
            self.last_disconnect_monotonic is not None and not self.fully_delivered and not self._polls
            and not any(self.clients.values())
            and time.monotonic() - self.last_disconnect_monotonic >= self.last_disconnect_grace_seconds
        )

    def _cancel_grace_timer(self):
        task, self._grace_task = self._grace_task, None
        # current_task 守卫：缓冲终态化可能正由计时器自身的取消路径驱动
        # （retire_expired → buf.fail），不能把「正在执行的自己」标成
        # 待取消——否则收尾阶段首个 await 会意外再抛 CancelledError。
        if task is not None and task is not asyncio.current_task() and not task.done():
            task.cancel()

    def _arm_grace_timer(self):
        """按当前断开标记启动续传窗口计时：到点仍无人续接就取消在飞上游。

        remaining 由断开时刻回算（晚挂的客户端连接按观测到的断开时间算），
        已超窗的旧断开立即触发而不是再等满窗口。重连（attach_client）、
        完整交付（mark_delivered）与生成完成（finish / fail）都会撤销计时；
        轮询活动经 note_client_activity 滑动窗口后重挂。"""
        if self._polls or self.done or self.fully_delivered or self.last_disconnect_monotonic is None:
            return
        self._cancel_grace_timer()
        grace = self.last_disconnect_grace_seconds or 0.0
        remaining = self.last_disconnect_monotonic + grace - time.monotonic()

        async def expire():
            try:
                await asyncio.sleep(max(0.001, remaining))
            except asyncio.CancelledError:
                raise
            # 到点复核：窗口内重连 / 已交付 / 已完成后直接退出，取消只针对
            # 「仍在运行且确实无人续接」的生成。
            if self.done or not self.resume_expired():
                return
            hook = self.on_grace_expired
            if hook is None:
                return
            try:
                await hook(self)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"⚠️ [续传缓冲] {self.id} 超窗取消上游失败（已忽略）：{type(exc).__name__} - {exc}")

        try:
            self._grace_task = asyncio.create_task(expire())
        except RuntimeError:
            # 无运行事件循环（同步测试上下文）：懒检查路径仍兑底。
            self._grace_task = None

    def note_client_activity(self, horizon: float = 0.0):
        """短连接客户端活动（/v1/resumable/{id} 轮询）：滑动续传窗口。

        轮询是断线后的合法续接方式（短连接天生抗抖动）：客户端还在轮询就
        视为「还连得上」，不因 SSE 断开超窗而取消仍在运行的上游。horizon
        为本次声明的在途等待（长轮询 wait，封顶 25 秒）：窗口滑到
        max(现有截止, 现在 + horizon)；客户端消失后，最后一次活动 + 窗口
        即为取消时点。"""
        if self.fully_delivered or self.last_disconnect_monotonic is None:
            return
        now = time.monotonic()
        target = max(self.last_disconnect_monotonic, now + max(0.0, min(horizon, 25.0)))
        if target > self.last_disconnect_monotonic:
            self.last_disconnect_monotonic = target
            self.last_disconnected_at = time.time() + (target - now)
            if self.owner_trace:
                with self.owner_trace._lock:
                    self.owner_trace.data["resume_expires_at"] = (
                        self.last_disconnected_at + self.last_disconnect_grace_seconds)
        # Joining a live poll paused the timer, even if an older successful
        # poll already reserved a longer horizon than this reader.
        self._arm_grace_timer()

    @property
    def next_seq(self) -> int:
        return len(self.events)

    @property
    def done(self) -> bool:
        return self.status != "running"

    def snapshot(self, start: int, limit: int) -> List[str]:
        start = max(0, start)
        return self.events[start:start + max(1, limit)]

    async def wait_for(self, seq: int, timeout: float) -> None:
        """等到出现 seq 号事件或本轮结束（或超时）。"""
        if timeout <= 0:
            return
        deadline = time.monotonic() + timeout
        async with self._cond:
            while len(self.events) <= seq and self.status == "running":
                remain = deadline - time.monotonic()
                if remain <= 0:
                    return
                try:
                    await asyncio.wait_for(self._cond.wait(), timeout=remain)
                except TimeoutError:
                    return

    async def iter_from(self, seq: int = 0) -> AsyncIterator[Tuple[int, str]]:
        """从 seq 开始跟随读取，直到本轮结束且读完所有事件。"""
        i = max(0, seq)
        self.readers += 1
        try:
            while True:
                async with self._cond:
                    while len(self.events) <= i and self.status == "running":
                        await self._cond.wait()
                    if len(self.events) <= i:
                        break                     # 已结束且无新事件
                    frame = self.events[i]
                self.last_access = time.time()
                yield i, frame
                i += 1
        finally:
            self.readers = max(0, self.readers - 1)

    async def acknowledge_http_send(self, connection) -> None:
        """Response background hook: only a completed, non-ambiguous final ASGI send counts."""
        if self.done and (connection is None or connection.delivery_possible):
            self.mark_delivered()

    def includes_delivery_terminal(self, start: int, end: int) -> bool:
        """An at-end cursor alone is not an OpenAI [DONE] send receipt."""
        if not self.done or end != self.next_seq:
            return False
        if self.kind == "json":
            return self.payload is not None  # poll includes the completed JSON body
        return self.kind == "native" or (start < end and ReplayEvent(self.events[-1]).is_done())

    def mark_delivered(self) -> None:
        """读者已把终止帧交付出去：下一次同指纹请求应视为新生成，而不是重放。"""
        self.fully_delivered = True
        self.last_access = time.time()
        self._cancel_grace_timer()

    def meta(self) -> dict:
        return {
            "id": self.id,
            "model": self.model,
            "kind": self.kind,
            "status": self.status,
            "error": self.error,
            "events": len(self.events),
            "next": len(self.events),
            "done": self.done,
            "fully_delivered": self.fully_delivered,
            "readers": self.readers,
            "clients": len(self.clients),
            "polls": self._polls,
            "disconnected_at": self.last_disconnected_at,
            "resume_expired": self.resume_expired(),
            "bytes": self.bytes_stored,
            "created_at": self.created_at,
            "finished_at": self.finished_at,
            "age_seconds": round(time.monotonic() - self.started_monotonic, 1),
        }


class ResumableStore:
    """缓冲区登记处：并发安全、带 TTL 与容量上限。"""

    def __init__(self):
        self._buffers: Dict[str, ResumableBuffer] = {}
        self._lock = asyncio.Lock()
        self._pumps: Dict[str, asyncio.Task] = {}
        self._all_pumps: set[asyncio.Task] = set()
        self._requests: Dict[str, str] = {}
        self._bytes_used = 0
        self._cleanup_tasks: set[asyncio.Task] = set()

    def _own_response_close(self, buf, pump, close):
        # Bind before the pump can finish (or be cancelled before its first
        # read). Its response may own setup/transport cleanup beyond the iterator.
        async def cleanup():
            await asyncio.gather(pump, return_exceptions=True)
            try:
                await close()
            except Exception:
                pass
        task = asyncio.create_task(cleanup())
        if buf is not None:
            buf._response_cleanup = task
        self._cleanup_tasks.add(task)

        def finished(completed):
            self._cleanup_tasks.discard(completed)
            if buf is not None and buf._response_cleanup is completed:
                buf._response_cleanup = None
        task.add_done_callback(finished)

    def _attach(self, buf: ResumableBuffer) -> None:
        def account(size, force):
            if not force and self._bytes_used + size > _byte_limit("resumable_max_total_mb", 256):
                return False
            self._bytes_used += size
            return True
        buf._account_bytes = account
        buf.on_grace_expired = self._grace_expired

    async def get_exact(self, stream_id: str) -> Optional[ResumableBuffer]:
        # Merely looking up a receipt cannot record authorized client activity.
        async with self._lock:
            return self._buffers.get(stream_id)

    async def get_or_create_request(self, request_key: str, *, model: str, kind: str,
                                    fingerprint_key: str, idempotent: bool = False):
        """A request key selects a generation; completed generations keep unique IDs."""
        fresh = True
        while True:
            async with self._lock:
                old = self._buffers.get(self._requests.get(request_key, request_key))
                if old is not None:
                    fresh = False
                closing = None
                if old and old.kind == kind:
                    if old.resume_expired():
                        closing = await self._retire_locked(old)
                    elif idempotent or not old.fully_delivered:
                        old.last_access = time.time()
                        return old, False
                    else:
                        # A delivered ordinary request is eligible for regeneration,
                        # but its terminal can precede both iterator and response close.
                        closing = self._draining(old)
                if closing is None:
                    return await self._admit_locked(request_key, model, kind, fingerprint_key, fresh=fresh), True
            # The old receipt and its request mapping remain authoritative
            # while cleanup owns resources, including if this waiter times out
            # or is cancelled. Only after cleanup do contenders elect one owner.
            done, _ = await asyncio.wait({closing}, timeout=SETUP_CLOSE_TIMEOUT_SECONDS)
            if not done:
                raise BufferCapacityError("旧请求仍在关闭连接，请稍后重试。")

    async def _admit_locked(self, request_key, model, kind, fingerprint_key, *, fresh):
        if len(self._buffers) >= max_streams() or self._bytes_used >= _byte_limit("resumable_max_total_mb", 256):
            for candidate in sorted(self._buffers.values(), key=lambda b: b.started_monotonic):
                if self._evictable(candidate):
                    await self._discard_locked(candidate.id, "响应缓冲容量已满。")
                    if len(self._buffers) < max_streams() and self._bytes_used < _byte_limit("resumable_max_total_mb", 256):
                        break
            else:
                error = FreshReplayCapacityError if fresh else BufferCapacityError
                raise error("响应缓冲容量已满，请稍后重试。")
        buf = ResumableBuffer("rid-" + secrets.token_hex(16), model=model, kind=kind)
        buf.request_key, buf.fingerprint_key = request_key, fingerprint_key
        self._attach(buf)
        self._buffers[buf.id] = buf
        self._requests[request_key] = buf.id
        return buf

    async def get(self, stream_id: str) -> Optional[ResumableBuffer]:
        async with self._lock:
            buf = self._buffers.get(stream_id) or self._buffers.get(self._requests.get(stream_id, ""))
            if buf:
                buf.last_access = time.time()
            return buf

    async def create_if_absent(self, stream_id: str, model: str = "",
                               kind: str = "stream", *,
                               replace_delivered: bool = False) -> Tuple[ResumableBuffer, bool]:
        """原子地「占位建缓冲」。返回 (缓冲区, 是否是本次新建的)。

        `get` → 调上游 → `create` 三步之间有竞态窗口：并发的同一请求会各打一次上游，
        后建的缓冲还会把先建的覆盖掉（孤儿泵任务停机时也取消不到）。这里把「查 + 建」
        放进同一把锁：只有拿到 is_new=True 的那个请求去调上游，其余请求直接跟读同一份缓冲。

        注：生产路径走 get_or_create_request（request_key → rid-* 映射 + 指纹/容量
        治理）；本方法是测试可见的最小建缓冲入口，供单元测试直接驱动 eviction /
        pump 生命周期等存储机制（2026-11 审查收敛：原并存的 ResumableStore.create
        仅被测试使用，已并入本方法——新建场景两者行为等价）。
        """
        async with self._lock:
            buf = self._buffers.get(stream_id)
            if buf is not None and replace_delivered and (buf.fully_delivered or buf.kind != kind):
                await self._discard_locked(stream_id, "已开始新一轮生成。")
                buf = None
            if buf is not None:
                buf.last_access = time.time()
                return buf, False
            buf = ResumableBuffer(stream_id, model=model, kind=kind)
            self._attach(buf)
            self._buffers[stream_id] = buf
            await self._evict_locked(allow_running=True)
            return buf, True

    async def _grace_expired(self, buf: ResumableBuffer) -> None:
        """断开超过续传窗口且无人续接：直接取消仍在运行的后台上游请求。"""
        grace = buf.last_disconnect_grace_seconds or 0.0
        print(f"🚫 [续传缓冲] {buf.id}：客户端断开已超过续传窗口（{grace:g} 秒）且无人续接，"
              "已取消后台上游请求，不再等待整轮期限。")
        await self.retire_expired(buf)

    async def require_resume_window(self, buf):
        """Check an owner-validated POST cursor or GET poll before any client activity."""
        if buf.resume_expired():
            await self.retire_expired(buf)
            raise ResumeRequestError(410, "客户端断开已超过续传窗口；重新生成请移除旧续传 ID 与游标。")

    async def retire_expired(self, buf):
        async with self._lock:
            if self._buffers.get(buf.id) is buf and buf.resume_expired():
                await self._retire_locked(buf)

    def _live_pump(self, buf):
        task = self._pumps.get(buf.id)
        return task if task is not None and not task.done() else None

    def _draining(self, buf):
        close = buf._response_cleanup
        return close if close is not None and not close.done() else self._live_pump(buf)

    def _evictable(self, buf):
        # Even natural completion can precede upstream teardown. Neither TTL
        # nor capacity pressure may unlink a live delivery, including the gap
        # before its first iterator pull and a send still awaiting transport.
        return buf.done and not buf.has_live_delivery and self._draining(buf) is None

    async def _retire_locked(self, buf, *, deadline=False):
        """Settle once; keep the receipt, admission guard and bytes through teardown."""
        if deadline:
            settled = await buf.fail_deadline()
        else:
            settled = await buf.fail("客户端断开已超过续传窗口，本轮上游请求已取消。", 410, "superseded")
            buf._retired_by = "grace"  # even an already-settled receipt can become stale
        if settled:
            task = self._live_pump(buf)
            if task and task is not asyncio.current_task() and not task.cancelling():
                task.cancel()  # never interrupt an already-running cancellation's finally
        return self._draining(buf)

    async def drop(self, stream_id: str, *, expected: Optional[ResumableBuffer] = None,
                   reason="响应缓冲已删除。") -> bool:
        async with self._lock:
            # A late setup failure must not delete a retired owner's receipt
            # or unlink its cleanup while an identical retry is waiting.
            if expected is not None and (self._buffers.get(stream_id) is not expected or expected._retired_by):
                return False
            return await self._discard_locked(stream_id, reason)

    async def _discard_locked(self, stream_id: str, reason: str) -> bool:
        """移除缓冲时一并停止后台请求、唤醒仍在等结果的读者。"""
        task = self._pumps.pop(stream_id, None)
        if task and task is not asyncio.current_task() and not task.done() and not task.cancelling():
            task.cancel()
        buf = self._buffers.pop(stream_id, None)
        if buf is not None:
            buf._cancel_grace_timer()
            if self._requests.get(buf.request_key) == stream_id:
                self._requests.pop(buf.request_key, None)
            if not buf.done:
                await buf.fail(reason, error_type="buffer_evicted")
            # 预算回收放到 fail 之后、按**最终**字节数结算（2026-11 C-6）：
            # 旧序「先按旧字节数回收 → 置 _account_bytes=None → 再 fail」会让
            # fail 追加的终态帧完全绕过全局记账，每次淘汰预算永久少计一个
            # 终态帧（虚高、超卖内存）。现在终态帧先经 force 记账短暂入账
            # （正确反映其在读者侧仍占内存），随后连同全部字节一次性回收。
            self._bytes_used = max(0, self._bytes_used - buf.bytes_stored)
            buf._account_bytes = None
        return buf is not None

    async def list_meta(self) -> List[dict]:
        async with self._lock:
            return [b.meta() for b in sorted(self._buffers.values(),
                                             key=lambda x: x.started_monotonic, reverse=True)]

    def register_pump(self, stream_id: str, task: asyncio.Task,
                      *, buffer: Optional[ResumableBuffer] = None, close=None) -> None:
        if close is not None:
            self._own_response_close(buffer, task, close)
        self._all_pumps.add(task)
        task.add_done_callback(self._all_pumps.discard)
        if buffer is not None and (self._buffers.get(stream_id) is not buffer or buffer._retired_by):
            task.cancel()
            return
        self._pumps[stream_id] = task

        def finished(completed):
            # 旧泵的取消回调不能把同 ID 后来注册的新泵移除。
            if self._pumps.get(stream_id) is completed:
                self._pumps.pop(stream_id, None)
            # Background setup can outlive its initiating frontend connection.
            if not completed.cancelled():
                completed.exception()
        task.add_done_callback(finished)

    def cancel_all_pumps(self) -> None:
        """取消所有后台泵任务（服务停机时调用）。"""
        for task in list(self._all_pumps):
            if not task.done() and not task.cancelling():
                task.cancel()
        self._pumps.clear()

    async def aclose(self) -> None:
        tasks = list(self._all_pumps)
        async with self._lock:
            for sid in list(self._buffers):
                await self._discard_locked(sid, "服务正在关闭。")
        self.cancel_all_pumps()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self._cleanup_tasks:
            await asyncio.gather(*list(self._cleanup_tasks), return_exceptions=True)

    async def _evict_locked(self, *, allow_running=False) -> None:
        """容量上限：优先淘汰最旧的「已完成」缓冲；实在没有就淘汰最旧的。"""
        limit = max_streams()
        if len(self._buffers) <= limit:
            return
        finished = sorted((b for b in self._buffers.values() if self._evictable(b)),
                          key=lambda x: x.finished_monotonic)
        for buf in finished:
            if len(self._buffers) <= limit:
                return
            await self._discard_locked(buf.id, "响应缓冲容量已满。")
        while allow_running and len(self._buffers) > limit:
            oldest = min((b for b in self._buffers.values() if not b.has_live_delivery
                          and (not b.done or self._draining(b) is None)),
                         key=lambda b: b.started_monotonic, default=None)
            if oldest is None:
                return
            await self._discard_locked(oldest.id, "响应缓冲容量已满。")

    async def sweep(self) -> int:
        """清掉超过 TTL 的已完成缓冲（有读者在读的不动）。"""
        ttl = ttl_seconds()
        now = time.monotonic()
        removed = 0
        delivered_ttl = delivered_ttl_seconds()
        async with self._lock:
            for sid, buf in list(self._buffers.items()):
                if self._evictable(buf):
                    # 保留策略由请求键决定；传输收据 sid 始终是独立的 rid-*。
                    effective_ttl = ttl
                    if buf.fully_delivered and not _explicit_request_key(buf.request_key):
                        effective_ttl = min(ttl, delivered_ttl)
                    if (now - buf.finished_monotonic) > effective_ttl:
                        await self._discard_locked(sid, "响应缓冲已过期。")
                        removed += 1
                elif not buf.done and time.monotonic() >= buf.deadline_monotonic:
                    # Expiry is a 504 outcome, not capacity eviction. The original
                    # pump may already be in an async finally from its own deadline.
                    await self._retire_locked(buf, deadline=True)
            await self._evict_locked()
        if removed:
            print(f"🧹 [续传缓冲] 已清理 {removed} 个过期响应缓冲。")
        return removed

    async def sweeper_loop(self, interval: float = 60.0) -> None:
        while True:
            try:
                await asyncio.sleep(interval)
                await self.sweep()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"⚠️ [续传缓冲] 清理任务异常（已忽略）：{type(e).__name__} - {e}")


resumable_store = ResumableStore()


class DetachedRequest:
    """把上游调用与前端连接解绑的请求壳子。

    上游代码用 `fastapi_request.is_disconnected()` 决定要不要放弃调用/重试。
    弱网下前端连接随时会断，但**回复必须继续收完**放进缓冲区，所以这里恒返回 False；
    其余属性（app.state、headers 等）原样透传给被包装的真实 Request。
    """

    def __init__(self, request: Any):
        self._request = request

    async def is_disconnected(self) -> bool:
        return False

    def __getattr__(self, item):
        return getattr(self._request, item)


def _as_text(chunk: Any) -> str:
    if isinstance(chunk, bytes):
        return chunk.decode("utf-8", errors="replace")
    return str(chunk)


def frame_with_id(seq: int, frame: str | ReplayEvent, stream_id: Optional[str] = None) -> str:
    """Replace upstream IDs with the server cursor on dispatchable data events."""
    event = ReplayEvent(frame) if isinstance(frame, str) else frame
    return event.with_id(f"{stream_id}:{seq}" if stream_id else str(seq))


async def pump_stream(buf: ResumableBuffer, body_iterator: AsyncIterator[Any]) -> None:
    """后台把上游 SSE 帧抽进缓冲区。**不依赖前端连接**。

    每个 data 帧按落缓冲的顺序分配单调序列号并写入 `id:` 行，
    前端凭 Last-Event-ID / resume_from 续传时与这里的序号对齐。
    """
    stream_error = None
    terminal = DONE_FRAME
    accepted_outcome = None
    stream_error_code = 200
    last_data_at = time.monotonic()
    next_wait_log = last_data_at + 60
    try:
        if buf.done:
            return
        async for chunk in body_iterator:
            if buf.done:
                return
            text = _as_text(chunk)
            if not text:
                continue
            event = ReplayEvent(text)
            if not event.has_data:
                # 注释帧（旧心跳等）不是事件：不进缓冲、不占序号，保持 id == 下标
                now = time.monotonic()
                if now >= next_wait_log:
                    print(f"⏳ [上游等待] {buf.id}：已运行 {now - buf.started_monotonic:.0f} 秒，"
                          f"距上次数据 {now - last_data_at:.0f} 秒，累计 {buf.next_seq} 个事件；"
                          "心跳仅表示连接保活，仍在等待上游数据或错误响应。")
                    next_wait_log = now + 60
                continue
            last_data_at = time.monotonic()
            next_wait_log = last_data_at + 60
            # Generators use normal JSON error frames followed by DONE. Data fields of
            # a multiline event form one payload; comments and field order do not matter.
            if '"error"' in text:
                error = event.error()
                if error:
                    stream_error = _replay_error_summary(error)
                    code = error.get("code") if isinstance(error, dict) else None
                    stream_error_code = code if isinstance(code, int) and 400 <= code <= 599 else 502
            if event.is_done():
                terminal = event
                break
            if await buf.append(buf.event_frame(event)) is None:
                return
        if buf.done:
            return
        status = "error" if stream_error is not None else "done"
        await buf.finish(status, error=stream_error, status_code=stream_error_code,
                         terminal_frame=terminal)
        accepted_outcome = "error" if status == "error" else "succeeded"
        print(f"📦 [续传缓冲] {buf.id} 上游读取结束，状态={buf.status}，共 {len(buf.events)} 个事件已落缓冲。")
    except (asyncio.CancelledError, StreamDeadlineExceeded):
        # The deadline/store owns cancellation status. Finishing here would
        # hide a 504 from the outer deadline and omit the terminal error frame.
        raise
    except Exception as e:
        msg = f"{type(e).__name__} - {e}"
        print(f"❌ [续传缓冲] {buf.id} 上游读取失败：{msg}")
        from upstream_errors import extract_upstream_error
        code, message = extract_upstream_error(e)
        await buf.fail(message, code, within_deadline=True)
    finally:
        aclose = getattr(body_iterator, "aclose", None)
        if aclose:
            try:
                if accepted_outcome is None:
                    await aclose()
                else:
                    # Do not read past DONE merely to trigger an EOF on a lease.
                    # A late close cannot change the already published status.
                    terminal_close = getattr(body_iterator, "aclose_terminal", None)
                    async with asyncio.timeout(SETUP_CLOSE_TIMEOUT_SECONDS):
                        if terminal_close is not None:
                            await terminal_close(accepted_outcome)
                        else:
                            await aclose()
            except Exception:
                pass


async def sse_from_buffer(buf: ResumableBuffer, start_seq: int = 0) -> AsyncIterator[str]:
    """把缓冲区内容按 SSE 吐给当前这条前端连接（可从任意 seq 续取）。

    事件落缓冲时已带 `id: <seq>`（见 pump_stream），这里原样吐出，不再二次加工。
    """
    if start_seq > 0:
        print(f"↩️ [续传] {buf.id} 从事件 #{start_seq} 续传（缓冲区已有 {buf.next_seq} 个事件）。")
    yield HEARTBEAT_FRAME
    events = with_heartbeats(buf.iter_from(start_seq))
    try:
        async for item in events:
            if item is HEARTBEAT:
                yield HEARTBEAT_FRAME
                continue
            _seq, frame = item
            yield frame
    finally:
        await events.aclose()


class ReplayStreamResponse(StreamingResponse):
    """Only a successful, connected ASGI send through EOF can consume a replay."""
    def __init__(self, request, buf: ResumableBuffer, start: int, *, headers):
        self.connection = ClientConnection.for_request(request)
        self.buffer, self.start = buf, start
        super().__init__(sse_from_buffer(buf, start), media_type="text/event-stream", headers=headers)

    async def stream_response(self, send):
        connection, buf = self.connection, self.buffer
        # ASGI >=2.4 relies on send raising to stop Starlette's stream. Some
        # transports instead drop sends, or have no outgoing data to send yet.
        # Cancel only this HTTP reader; the detached upstream still owns its pump.
        with anyio.CancelScope() as scope:
            def peer_closed(gone):
                if gone:
                    scope.cancel()
            if connection is not None:
                connection.listen(peer_closed)
            try:
                await super().stream_response(send)
                if (connection is None or connection.delivery_possible) and buf.includes_delivery_terminal(
                        self.start, buf.next_seq):
                    buf.mark_delivered()
            finally:
                if connection is not None and peer_closed in connection.listeners:
                    connection.listeners.remove(peer_closed)
                # If send fails after yield, async-for does not close its iterator.
                with anyio.CancelScope(shield=True):
                    await self.body_iterator.aclose()

"""Process-local bounded reply storage and reconnect delivery.

Each generation has an immutable random ID. A canonical request fingerprint
selects the latest generation for legacy retries; Idempotency-Key explicitly
identifies one logical operation. SSE event IDs contain generation + sequence,
and X-Resume-Id directly addresses the retained generation, bound to its body.

Frontend disconnects do not cancel admitted generation immediately. Repeating
the request after its disconnect grace cancels and replaces that generation.
TTL, byte budgets and a total execution deadline bound retention. Upstream loss cannot reconstruct an
unreceived tail; it is reported as an error. ASGI send completion acknowledges
server delivery only, not application-level receipt. Buffers are not shared
between workers/replicas and do not survive a process restart.
"""

import asyncio
import anyio
import hashlib
import json
import math
import secrets
import threading
import time
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

import config as app_config
from runtime_state import app_state
from streaming import with_heartbeats, HEARTBEAT, HEARTBEAT_FRAME
from request_tracking import current_trace

# 终止帧：与 OpenAI 流一致，客户端见到它即可判定「本轮完整结束」
DONE_FRAME = "data: [DONE]\n\n"


class BufferCapacityError(RuntimeError):
    code = 503


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
        if buf.fingerprint_key and buf.fingerprint_key != fingerprint_key:
            raise ResumeRequestError(409, "续传请求体与原请求不一致。")
        if buf.owner_key_digest and owner_key_digest and buf.owner_key_digest != owner_key_digest:
            # 同一 rid 不可能属于另一把 Key（rid 混入 Key+指纹），不一致说明
            # 调用者拿着别人的生成 ID 在试。404 不泄露存在性。
            raise ResumeRequestError(404, "续传缓冲不存在或已过期；重新生成请移除续传 ID 与游标。")
        if buf.resume_expired():
            await store.drop(buf.id, expected=buf, reason="客户端断开已超过续传窗口。", superseded=True)
            raise ResumeRequestError(410, "客户端断开已超过续传窗口；重新生成请移除旧续传 ID 与游标。")
        if start > buf.next_seq:
            raise ResumeRequestError(409, f"续传起点 {start} 超过缓冲末尾 {buf.next_seq}。")
        _track_buffer(buf, False, request)
        return buf, False, start, source
    idempotent = bool(request.headers.get("idempotency-key") or request.headers.get("x-idempotency-key"))
    buf, new = await store.get_or_create_request(
        request_key, model=model, kind=kind, fingerprint_key=fingerprint_key, idempotent=idempotent)
    if owner_key_digest:
        # 新建时记录归属；命中已有缓冲时为幂等回写（同一 Key 必然得到同一摘要，
        # 老版本缓冲无此字段时补齐，空 Key 部署不动）。
        buf.owner_key_digest = owner_key_digest
    _track_buffer(buf, new, request)
    return buf, new, 0, source


def _track_buffer(buf, new, request):
    connection = getattr(request, "scope", {}).get("state", {}).get("client_connection")
    if connection:
        buf.attach_client(connection)
    if trace := current_trace.get():
        if new:
            buf.trace_id = trace.id
        trace.configure(generation_id=buf.id, detached=new, parent_id=None if new else buf.trace_id)
        trace.event("buffer" if new else "resume", "后台缓冲已建立，前端断开后继续生成。" if new else
                    f"续接已有生成 {buf.id}，不会重复请求上游。", phase="preparing" if new else "resuming")


async def within_request_deadline(buf, operation):
    """One deadline covers preparation, retries and all subsequent reads."""
    remaining = buf.deadline_monotonic - time.monotonic()
    try:
        async with asyncio.timeout(max(0.001, remaining)):
            return await (operation() if callable(operation) else operation)
    except TimeoutError:
        await buf.fail("本轮请求超过总执行期限。", 504, "timeout")
        raise
    except asyncio.CancelledError:
        await buf.fail("上游任务已取消。", 503)
        raise
    except Exception as exc:
        from upstream_errors import extract_upstream_error
        code, message = extract_upstream_error(exc)
        await buf.fail(message, code)
        raise


def request_timeout_seconds() -> float:
    try:
        value = float(_setting("request_timeout_seconds"))
        return max(0.01, value) if math.isfinite(value) else 1800.0
    except (ValueError, TypeError):
        return 1800.0


def disconnect_grace_seconds() -> float:
    try:
        value = float(_setting("resumable_disconnect_grace_seconds"))
        return max(0.0, min(7200.0, value)) if math.isfinite(value) else 10.0
    except (ValueError, TypeError, OverflowError):
        return 10.0


# 限额 TTL 缓存：append 每个 SSE 帧都会走 _byte_limit 读全局预算（状态锁 +
# 解析），控制台调整频率极低——1 秒内最多重读一次，放宽后至多 1 秒生效
# （原先的"实时读"代价摊在每个帧上，2026-10 优化）。
_limit_cache: dict = {}
_limit_cache_lock = threading.Lock()
_LIMIT_CACHE_TTL = 1.0


def _byte_limit(key: str, default: int) -> int:
    now = time.monotonic()
    with _limit_cache_lock:
        hit = _limit_cache.get(key)
        if hit is not None and now - hit[0] < _LIMIT_CACHE_TTL:
            return hit[1]
    try:
        value = max(1, int(float(_setting(key)) * 1024 * 1024))
    except (ValueError, TypeError, OverflowError):
        value = default * 1024 * 1024
    with _limit_cache_lock:
        _limit_cache[key] = (now, value)
    return value


def _setting(key: str) -> Any:
    try:
        return app_state.get_setting(key, app_config.DEFAULT_SETTINGS.get(key))
    except Exception:
        return app_config.DEFAULT_SETTINGS.get(key)


def is_enabled() -> bool:
    return bool(_setting("resumable_enabled"))


def ttl_seconds() -> float:
    try:
        return max(30.0, float(_setting("resumable_ttl_seconds")))
    except (TypeError, ValueError):
        return float(app_config.DEFAULT_SETTINGS["resumable_ttl_seconds"])


def max_streams() -> int:
    try:
        return max(1, int(_setting("resumable_max_streams")))
    except (TypeError, ValueError):
        return int(app_config.DEFAULT_SETTINGS["resumable_max_streams"])


def compute_stream_id(headers: Any, api_key: str, fingerprint: str) -> Tuple[str, str]:
    """决定这次请求的缓冲区 ID。返回 (id, 来源说明)。

    优先用客户端显式给的幂等键——聪明的客户端可以自己控制"哪两次请求算同一次"；
    没有就用「API Key + 请求体」哈希，让**原样重发**天然命中同一缓冲区。
    """
    # X-Resume-Id identifies an existing generation, not a new idempotency key.
    for h in ("idempotency-key", "x-idempotency-key", "x-request-id"):
        try:
            v = (headers.get(h) or "").strip()
        except Exception:
            v = ""
        if v:
            # 幂等键只做“同一用户 + 同一请求体”之内的显式身份：混入 API Key 与请求指纹，
            # 避免不同用户复用同一键值时串缓冲（A 的回复被 B 读到），也避免同一键配不同
            # 请求体时误重放。客户端原样重发（同键 + 同体）仍稳定命中同一缓冲。
            digest = hashlib.sha256(f"{api_key}\x00{v}\x00{fingerprint}".encode("utf-8")).hexdigest()[:32]
            return f"cid-{digest}", f"客户端请求头 {h}"
    digest = hashlib.sha256(f"{api_key}\x00{fingerprint}".encode("utf-8")).hexdigest()[:32]
    return f"req-{digest}", "请求体指纹"


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
        self.last_access = self.created_at
        self.status = "running"               # running | done | error
        self.error: Optional[str] = None
        self.events: List[str] = []           # SSE 帧，下标即 seq
        self.payload: Optional[dict] = None   # 非流式：完整 JSON 响应体
        self.status_code: int = 200
        self.fully_delivered = False          # 是否已有读者完整读到终止帧
        self.readers = 0
        self.clients = set()
        self.last_disconnect_monotonic = None
        self.last_disconnected_at = None
        self.last_disconnect_grace_seconds = None
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

    # ---------- 写入端（后台泵任务持有） ----------

    async def append(self, frame: str) -> Optional[int]:
        async with self._cond:
            # 淘汰/完成后的副本必须保持不变；部分上游会在取消后仍交出最后一块。
            if self.done:
                return None
            size = len(frame.encode("utf-8"))
            if self.bytes_stored + size > self.max_buffer_bytes:
                raise BufferCapacityError("单次响应超过续传缓冲字节上限。")
            if self._account_bytes and not self._account_bytes(size, False):
                raise BufferCapacityError("响应缓冲总字节预算已满，请稍后重试。")
            self.bytes_stored += size
            self.events.append(frame)
            self._cond.notify_all()
            return len(self.events) - 1

    async def finish(self, status: str = "done", error: Optional[str] = None,
                     payload: Optional[dict] = None, status_code: Optional[int] = None) -> None:
        async with self._cond:
            if self.done:
                return
            self.status = status
            self.error = error
            if payload is not None:
                size = len(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
                fits = self.bytes_stored + size <= self.max_buffer_bytes
                if fits and self._account_bytes:
                    fits = self._account_bytes(size, False)
                if not fits:
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
            self.finished_at = time.time()
            self._cond.notify_all()

    def event_frame(self, frame: str) -> str:
        return frame_with_id(self.next_seq, frame, self.id if self.id.startswith("rid-") else None)

    async def fail(self, message: str, code: int = 503, error_type: str = "upstream_error") -> None:
        """Reserve a small terminal error even when the data budget is exhausted."""
        async with self._cond:
            if self.done:
                return
            if self.owner_trace:
                token = current_trace.set(self.owner_trace)
                try:
                    if error_type == "superseded":
                        self.owner_trace.cancel(message)
                    else:
                        self.owner_trace.fail(message, code)
                finally:
                    current_trace.reset(token)
            payload = {"error": {"message": message[:1024], "type": error_type, "code": code}}
            if self.kind == "native":
                detail = {400: "INVALID_ARGUMENT", 401: "UNAUTHENTICATED", 403: "PERMISSION_DENIED",
                          404: "NOT_FOUND", 429: "RESOURCE_EXHAUSTED", 504: "DEADLINE_EXCEEDED"}.get(code, "UNAVAILABLE")
                payload = {"error": {"message": message[:1024], "status": detail, "code": code}}
            frames = ([self.event_frame(f"data: {json.dumps(payload, ensure_ascii=False)}\n\n")]
                      if self.kind != "json" else [])
            if self.kind != "json":
                # Allocate the next sequence after the error, not the same ID.
                self.events.extend(frames)
                if self.kind != "native":
                    frames.append(self.event_frame(DONE_FRAME))
                    self.events.append(frames[-1])
            self.payload = payload
            size = sum(len(f.encode("utf-8")) for f in frames) + len(json.dumps(payload).encode())
            if self._account_bytes:
                self._account_bytes(size, True)
            self.bytes_stored += size
            self.status, self.error, self.status_code = "error", message[:1024], code
            self.finished_at = time.time()
            self._cond.notify_all()

    # ---------- 读取端（可以有多个、可以中途离开再回来） ----------

    def attach_client(self, connection):
        client = object()
        self.clients.add(client)
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

        def closed(disconnected):
            self.clients.discard(client)
            if disconnected and not self.clients and not self.fully_delivered:
                self.last_disconnect_monotonic = connection.disconnected_monotonic
                self.last_disconnected_at = connection.disconnected_at
                # Snapshot settings at the last reader's disconnect, just as
                # each upstream attempt snapshots its first-reply deadline.
                self.last_disconnect_grace_seconds = disconnect_grace_seconds()
            update_meta()
        update_meta()
        connection.listen(closed)

    def resume_expired(self):
        return (not self.clients and not self.fully_delivered and self.last_disconnect_monotonic is not None
                and time.monotonic() - self.last_disconnect_monotonic >= self.last_disconnect_grace_seconds)

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
                except asyncio.TimeoutError:
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

    def mark_delivered(self) -> None:
        """读者已把终止帧交付出去：下一次同指纹请求应视为新生成，而不是重放。"""
        self.fully_delivered = True
        self.last_access = time.time()

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
            "disconnected_at": self.last_disconnected_at,
            "resume_expired": self.resume_expired(),
            "bytes": self.bytes_stored,
            "created_at": self.created_at,
            "finished_at": self.finished_at,
            "age_seconds": round(time.time() - self.created_at, 1),
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

    def schedule_cleanup(self, close):
        async def cleanup():
            try:
                await close()
            except Exception:
                pass
        task = asyncio.create_task(cleanup())
        self._cleanup_tasks.add(task)
        task.add_done_callback(self._cleanup_tasks.discard)

    def _attach(self, buf: ResumableBuffer) -> None:
        def account(size, force):
            if not force and self._bytes_used + size > _byte_limit("resumable_max_total_mb", 256):
                return False
            self._bytes_used += size
            return True
        buf._account_bytes = account

    async def get_exact(self, stream_id: str) -> Optional[ResumableBuffer]:
        async with self._lock:
            buf = self._buffers.get(stream_id)
            if buf:
                buf.last_access = time.time()
            return buf

    async def get_or_create_request(self, request_key: str, *, model: str, kind: str,
                                    fingerprint_key: str, idempotent: bool = False):
        """A request key selects a generation; completed generations keep unique IDs."""
        cancelled = None
        async with self._lock:
            sid = self._requests.get(request_key, request_key)
            old = self._buffers.get(sid)
            if old and old.kind == kind and old.resume_expired():
                cancelled = self._pumps.get(old.id)
                await self._discard_locked(old.id, "客户端断开已超过续传窗口，相同请求开始新一轮生成。", superseded=True)
                old = None
            if old and old.kind == kind and (idempotent or not old.fully_delivered):
                old.last_access = time.time()
                return old, False
            # Admission must not cancel an unrelated in-flight request.
            if len(self._buffers) >= max_streams() or self._bytes_used >= _byte_limit("resumable_max_total_mb", 256):
                for candidate in sorted(self._buffers.values(), key=lambda b: b.created_at):
                    if candidate.done and candidate.readers == 0:
                        await self._discard_locked(candidate.id, "响应缓冲容量已满。")
                        if len(self._buffers) < max_streams() and self._bytes_used < _byte_limit("resumable_max_total_mb", 256):
                            break
                else:
                    raise BufferCapacityError("响应缓冲容量已满，请稍后重试。")
            buf = ResumableBuffer("rid-" + secrets.token_hex(16), model=model, kind=kind)
            buf.request_key, buf.fingerprint_key = request_key, fingerprint_key
            self._attach(buf)
            self._buffers[buf.id] = buf
            self._requests[request_key] = buf.id
        # Do not hold the store lock while cancelled setup/stream cleanup runs.
        # Followers can already follow the new placeholder; only its owner starts upstream.
        if cancelled and not cancelled.done():
            try:
                done, _ = await asyncio.wait({cancelled}, timeout=5)
                if not done:
                    raise BufferCapacityError("旧请求仍在关闭连接，请稍后重试。")
            except BaseException:
                with anyio.CancelScope(shield=True):
                    await self.drop(buf.id, expected=buf, reason="新请求在替换旧任务时中止。")
                raise
        return buf, True

    async def get(self, stream_id: str) -> Optional[ResumableBuffer]:
        async with self._lock:
            buf = self._buffers.get(stream_id) or self._buffers.get(self._requests.get(stream_id, ""))
            if buf:
                buf.last_access = time.time()
            return buf

    async def create(self, stream_id: str, model: str = "", kind: str = "stream") -> ResumableBuffer:
        async with self._lock:
            await self._discard_locked(stream_id, "已开始新一轮生成。")
            buf = ResumableBuffer(stream_id, model=model, kind=kind)
            self._attach(buf)
            self._buffers[stream_id] = buf
            await self._evict_locked(allow_running=True)
            return buf

    async def create_if_absent(self, stream_id: str, model: str = "",
                               kind: str = "stream", *,
                               replace_delivered: bool = False) -> Tuple[ResumableBuffer, bool]:
        """原子地「占位建缓冲」。返回 (缓冲区, 是否是本次新建的)。

        `get` → 调上游 → `create` 三步之间有竞态窗口：并发的同一请求会各打一次上游，
        后建的缓冲还会把先建的覆盖掉（孤儿泵任务停机时也取消不到）。这里把「查 + 建」
        放进同一把锁：只有拿到 is_new=True 的那个请求去调上游，其余请求直接跟读同一份缓冲。
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

    async def drop(self, stream_id: str, *, expected: Optional[ResumableBuffer] = None,
                   reason="响应缓冲已删除。", superseded=False) -> bool:
        async with self._lock:
            if expected is not None and self._buffers.get(stream_id) is not expected:
                return False
            return await self._discard_locked(stream_id, reason, superseded=superseded)

    async def _discard_locked(self, stream_id: str, reason: str, *, superseded=False) -> bool:
        """移除缓冲时一并停止后台请求、唤醒仍在等结果的读者。"""
        task = self._pumps.pop(stream_id, None)
        if task and task is not asyncio.current_task() and not task.done() and not task.cancelling():
            task.cancel()
        buf = self._buffers.pop(stream_id, None)
        if buf is not None:
            self._bytes_used = max(0, self._bytes_used - buf.bytes_stored)
            buf._account_bytes = None
            if self._requests.get(buf.request_key) == stream_id:
                self._requests.pop(buf.request_key, None)
            if not buf.done:
                await buf.fail(reason, error_type="superseded" if superseded else "buffer_evicted")
            elif superseded and buf.owner_trace:
                token = current_trace.set(buf.owner_trace)
                try:
                    buf.owner_trace.cancel(reason)
                finally:
                    current_trace.reset(token)
        return buf is not None

    async def list_meta(self) -> List[dict]:
        async with self._lock:
            return [b.meta() for b in sorted(self._buffers.values(),
                                             key=lambda x: x.created_at, reverse=True)]

    def register_pump(self, stream_id: str, task: asyncio.Task,
                      *, buffer: Optional[ResumableBuffer] = None) -> None:
        self._all_pumps.add(task)
        task.add_done_callback(self._all_pumps.discard)
        if buffer is not None and self._buffers.get(stream_id) is not buffer:
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
        finished = sorted((b for b in self._buffers.values() if b.done and b.readers == 0),
                          key=lambda x: x.finished_at or x.created_at)
        for buf in finished:
            if len(self._buffers) <= limit:
                return
            await self._discard_locked(buf.id, "响应缓冲容量已满。")
        while allow_running and len(self._buffers) > limit:
            oldest = min(self._buffers.values(), key=lambda x: x.created_at)
            await self._discard_locked(oldest.id, "响应缓冲容量已满。")

    async def sweep(self) -> int:
        """清掉超过 TTL 的已完成缓冲（有读者在读的不动）。"""
        ttl = ttl_seconds()
        now = time.time()
        removed = 0
        async with self._lock:
            for sid, buf in list(self._buffers.items()):
                ref = buf.finished_at or buf.last_access
                if buf.done and buf.readers == 0 and (now - ref) > ttl:
                    await self._discard_locked(sid, "响应缓冲已过期。")
                    removed += 1
                elif not buf.done and time.monotonic() >= buf.deadline_monotonic:
                    # 极端兜底：上游永不结束的僵尸缓冲
                    await self._discard_locked(sid, "上游响应等待超时。")
                    removed += 1
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


def frame_with_id(seq: int, frame: str, stream_id: Optional[str] = None) -> str:
    """给 SSE 数据帧加上事件 ID，供 Last-Event-ID 续传。

    注释帧（以 `:` 开头）不带 id —— 它不是事件，带 id 会污染客户端游标。
    """
    if frame.startswith("data:"):
        event_id = f"{stream_id}:{seq}" if stream_id else str(seq)
        return f"id: {event_id}\n{frame}"
    return frame


async def pump_stream(buf: ResumableBuffer, body_iterator: AsyncIterator[Any]) -> None:
    """后台把上游 SSE 帧抽进缓冲区。**不依赖前端连接**。

    每个 data 帧按落缓冲的顺序分配单调序列号并写入 `id:` 行，
    前端凭 Last-Event-ID / resume_from 续传时与这里的序号对齐。
    """
    stream_error = None
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
            if text.lstrip().startswith(":"):
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
            # The upstream generators report failures as ordinary SSE error
            # frames followed by DONE. Completion of the iterator does not turn
            # such a reply into a successful poll/replay result.
            if '"error"' in text:
                for line in text.splitlines():
                    if not line.startswith("data:"):
                        continue
                    try:
                        payload = json.loads(line[5:].strip())
                    except (ValueError, TypeError):
                        continue
                    error = payload.get("error") if isinstance(payload, dict) else None
                    if error:
                        stream_error = str(error.get("message", "上游返回错误。")
                                           if isinstance(error, dict) else error)[:1024]
                        code = error.get("code") if isinstance(error, dict) else None
                        stream_error_code = code if isinstance(code, int) and 400 <= code <= 599 else 502
            if await buf.append(buf.event_frame(text)) is None:
                return
        if buf.done:
            return
        if not buf.events or not buf.events[-1].strip().endswith("[DONE]"):
            await buf.append(buf.event_frame(DONE_FRAME))
        await buf.finish("error" if stream_error else "done", error=stream_error,
                         status_code=stream_error_code)
        print(f"📦 [续传缓冲] {buf.id} 上游读取结束，状态={buf.status}，共 {len(buf.events)} 个事件已落缓冲。")
    except asyncio.CancelledError:
        # The deadline/store owns cancellation status. Finishing here would
        # hide a 504 from the outer deadline and omit the terminal error frame.
        raise
    except Exception as e:
        msg = f"{type(e).__name__} - {e}"
        print(f"❌ [续传缓冲] {buf.id} 上游读取失败：{msg}")
        await buf.fail(str(e), getattr(e, "code", 500))
    finally:
        aclose = getattr(body_iterator, "aclose", None)
        if aclose:
            try:
                await aclose()
            except Exception:
                pass


async def sse_from_buffer(buf: ResumableBuffer, start_seq: int = 0) -> AsyncIterator[str]:
    """把缓冲区内容按 SSE 吐给当前这条前端连接（可从任意 seq 续取）。

    事件落缓冲时已带 `id: <seq>`（见 pump_stream），这里原样吐出，不再二次加工。
    """
    if start_seq > 0:
        print(f"↩️ [续传] {buf.id} 从事件 #{start_seq} 续传（缓冲区已有 {buf.next_seq} 个事件）。")
    delivered_done = False
    yield HEARTBEAT_FRAME
    events = with_heartbeats(buf.iter_from(start_seq))
    try:
        async for item in events:
            if item is HEARTBEAT:
                yield HEARTBEAT_FRAME
                continue
            _seq, frame = item
            yield frame
            if frame.strip().endswith("[DONE]"):
                delivered_done = True
    finally:
        await events.aclose()
    if delivered_done or (buf.kind == "native" and buf.done and start_seq <= buf.next_seq):
        buf.mark_delivered()

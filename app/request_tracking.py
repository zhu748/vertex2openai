"""Bounded, process-local request diagnostics, separate from aggregate statistics.

Context follows async tasks and to_thread preparation. A frontend connection and
its upstream generation have separate lifetimes: disconnecting a buffered reader
must not make a still-running generation disappear from the console.
"""
from collections import OrderedDict, deque
from contextvars import ContextVar
import json
import math
import re
import secrets
import threading
import time


current_trace = ContextVar("request_trace", default=None)
_attempt_started = ContextVar("attempt_started", default=None)
MAX_INSPECTION_BYTES = 2 * 1024 * 1024

# SSE 帧分隔符：PayloadObserver 每个网络块都会扫描，必须预编译。
_FRAME_SEP_RE = re.compile(rb"\r\n\r\n|\n\n|\r\r")

# redact 在每条日志/每个 SSE 块上都会执行，必须用预编译 pattern。
_RE_URL_CREDS = re.compile(r"(?i)(?<![a-z0-9+.-])([a-z][a-z0-9+.-]{0,31})://[^/\s@]+@")
_RE_QUERY_KEY = re.compile(r"(?i)([?&](?:key|api_key|access_token)=)[^&\s'\"]+")
_RE_BEARER = re.compile(r"(?i)(bearer[ \t]+)[^\s'\",;]+")
_RE_GOOGLE_KEY = re.compile(r"AIza[\w\-]{20,}")


def numeric_usage(usage):
    return {k: v for k, v in usage.items() if isinstance(v, int) or isinstance(v, float) and math.isfinite(v)}


def redact(text):
    text = str(text)
    text = _RE_URL_CREDS.sub(r"\1://***@", text)
    text = _RE_QUERY_KEY.sub(r"\1***", text)
    text = _RE_BEARER.sub(r"\1***", text)
    text = _RE_GOOGLE_KEY.sub("***API_KEY***", text)
    return text


class RequestTrace:
    def __init__(self, path):
        self.id = "trace-" + secrets.token_hex(8)
        self._lock = threading.RLock()
        self.started = time.monotonic()
        self.ended = None
        self.data = {
            "id": self.id, "path": path, "model": "", "protocol": "", "channel": "",
            "stream": False, "mode": "", "phase": "received", "outcome": None,
            "created_at": time.time(), "finished_at": None,
            "http_active": True, "generation_active": False, "connection": "open",
            "http_status": None, "generation_id": None, "parent_id": None, "detached": False,
            "disconnected_at": None,
            "attempts": 0, "retries": 0, "retry_at": None, "last_error": None,
            "generated_frames": 0, "sent_frames": 0, "generated_bytes": 0, "sent_bytes": 0,
            "content_chars": 0, "reasoning_chars": 0, "tool_deltas": 0,
            "sent_content_chars": 0, "sent_reasoning_chars": 0,
            "usage": {}, "finish_reasons": [], "inspection_skipped_bytes": 0,
        }
        self.timings = {}
        self.events = deque(maxlen=160)
        self.logs = deque(maxlen=200)
        self.retained_bytes = 0
        self.event("received", "请求已接收，等待鉴权与参数校验。")

    def event(self, kind, message, *, phase=None):
        with self._lock:
            if phase:
                self.data["phase"] = phase
            item = {"kind": kind, "at": time.time(), "elapsed_ms": round((time.monotonic() - self.started) * 1000),
                    "message": redact(message)[:1200]}
            self._append(self.events, item)
        print(f"🧭 [请求跟踪] [{self.id}] {item['message']}")

    def configure(self, **values):
        with self._lock:
            for key in ("model", "protocol", "channel", "stream", "mode", "generation_id", "parent_id", "detached",
                        "thinking_level", "include_thoughts", "max_output_tokens", "upstream_model", "project_id"):
                if key in values:
                    value = values[key]
                    self.data[key] = value[:500] if isinstance(value, str) else value

    def _append(self, target, item):
        def size(entry):
            # ASCII 快速路径（CPython 位标志 O(1)）：日志绝大多数为 ASCII +
            # emoji 之外的中文也只需精确路径一次；免去每条消息两遍全量
            # UTF-8 编码只为测长的临时 bytes 分配。
            message = entry["message"]
            return (len(message) + 128) if message.isascii() else len(message.encode("utf-8")) + 128
        if len(target) == target.maxlen:
            self.retained_bytes -= size(target[0])
        target.append(item)
        self.retained_bytes += size(item)

    def log(self, message):
        with self._lock:
            self._append(self.logs, {"at": time.time(), "message": redact(message)[:2000]})

    def milestone(self, key, message, *, phase=None):
        with self._lock:
            if key in self.timings:
                return
            self.timings[key] = {"at": time.time(), "elapsed_ms": round((time.monotonic() - self.started) * 1000)}
        self.event(key, message, phase=phase)

    def start_generation(self):
        with self._lock:
            self.ended = None
            self.data["finished_at"] = None
            self.data["generation_active"] = True
        self.milestone("generation_start", "已取得上游并发名额，正在准备消息与连接。", phase="preparing")

    def attempt(self):
        with self._lock:
            self.data["attempts"] += 1
            attempt = self.data["attempts"]
            self.data["retry_at"] = None
        self.event("attempt", f"第 {attempt} 次上游调用已发起，等待响应。", phase="waiting_upstream")

    def retry(self, delay, error):
        with self._lock:
            self.data["retry_at"] = time.time() + delay
            self.data["last_error"] = redact(error)[:1200]
            self.data["first_reply_deadline"] = None
        self.event("retry_wait", f"上游失败，{delay:.1f} 秒后重试：{redact(error)[:400]}", phase="retry_wait")

    def add_retry(self):
        with self._lock:
            self.data["retries"] += 1

    def upstream_chunk(self):
        with self._lock:
            self.timings["last_upstream"] = {"at": time.time(), "elapsed_ms": round((time.monotonic() - self.started) * 1000)}
        self.milestone("first_upstream", "收到首个上游分片；它可能仅包含思考、签名或用量。", phase="receiving")

    def fail(self, message, code=None):
        with self._lock:
            self.data["outcome"] = "error"
            self.data["last_error"] = redact(message)[:1200]
            if isinstance(code, int) and 400 <= code <= 599:
                self.data["error_code"] = code
        self.event("error", f"请求失败：{redact(message)[:600]}", phase="error")

    def cancel(self, message):
        with self._lock:
            self.data["outcome"] = "cancelled"
        self.event("cancelled", message, phase="cancelling" if self.data["generation_active"] else "cancelled")

    def _settle(self):
        if not self.data["http_active"] and not self.data["generation_active"] and self.ended is None:
            self.ended = time.monotonic()
            self.data["finished_at"] = time.time()
            self.data["outcome"] = self.data["outcome"] or "succeeded"
            self.data["phase"] = self.data["outcome"]

    def end_generation(self, outcome="succeeded", message=None):
        with self._lock:
            if not self.data["generation_active"]:
                return
            self.data["generation_active"] = False
            self.data["retry_at"] = None
            self.data["first_reply_deadline"] = None
            if self.data["outcome"] != "error":
                self.data["outcome"] = outcome
            if message:
                self.data["last_error"] = redact(message)[:1200]
            self._settle()
        self.event("generation_end", f"上游生成结束：{self.data['outcome']}。",
                   phase=self.data["outcome"] if not self.data["http_active"] else "delivery")

    def end_connection(self, disconnected=False):
        with self._lock:
            if not self.data["http_active"]:
                return
            self.data["http_active"] = False
            self.data["connection"] = "disconnected" if disconnected else "completed"
            if disconnected:
                self.data["disconnected_at"] = time.time()
            if disconnected and not self.data["generation_active"] and not self.data["outcome"]:
                self.data["outcome"] = "cancelled"
            self._settle()
            background = self.data["generation_active"]
        self.event("connection_end", ("前端连接已断开。" if disconnected else "前端响应发送结束。")
                   + ("后台上游仍在运行，可通过续传恢复。" if background else ""))

    def inspect_payload(self, payload, *, sent=False):
        if not isinstance(payload, dict):
            return
        error = payload.get("error")
        if error:
            self.fail(error.get("message", str(error)) if isinstance(error, dict) else error,
                      error.get("code") if isinstance(error, dict) else None)
            return
        content, reasoning, tools, reasons = 0, 0, 0, []
        media = False
        for choice in payload.get("choices") or []:
            delta = choice.get("delta") or choice.get("message") or {}
            content += len(delta.get("content") or "")
            reasoning += len(delta.get("reasoning_content") or "")
            tools += len(delta.get("tool_calls") or [])
            if choice.get("finish_reason"):
                reasons.append(str(choice["finish_reason"]))
        for candidate in payload.get("candidates") or []:
            for part in (candidate.get("content") or {}).get("parts") or []:
                if part.get("thought"):
                    reasoning += len(part.get("text") or "")
                else:
                    content += len(part.get("text") or "")
                tools += int(bool(part.get("functionCall")))
                media |= bool(part.get("inlineData") or part.get("fileData"))
            if candidate.get("finishReason"):
                reasons.append(str(candidate["finishReason"]))
        with self._lock:
            if sent:
                self.data["sent_content_chars"] += content
                self.data["sent_reasoning_chars"] += reasoning
            else:
                self.data["content_chars"] += content
                self.data["reasoning_chars"] += reasoning
                self.data["tool_deltas"] += tools
                self.data["finish_reasons"] = sorted(set(self.data["finish_reasons"] + reasons))
                usage = payload.get("usage") or payload.get("usageMetadata")
                if isinstance(usage, dict):
                    self.data["usage"] = numeric_usage(usage)
        if sent:
            if content or reasoning or tools or media:
                self.milestone("first_sent_reply", "首个有效模型回复已通过服务端 ASGI 发送。")
            if content:
                self.milestone("first_sent_content", "首段正文已通过服务端 ASGI 发送；不代表客户端已渲染。")
        else:
            if content or reasoning or tools or media:
                self.milestone("first_reply", "收到模型首个有效回复，首回复计时结束。")
            if reasoning:
                self.milestone("first_reasoning", "开始返回思考内容。", phase="thinking")
            if content:
                self.milestone("first_content", "开始返回正文（服务端首字）。", phase="streaming")
            if tools:
                self.milestone("first_tool", "开始返回工具调用。", phase="streaming")

    def snapshot(self, detail=False):
        with self._lock:
            result = dict(self.data)
            result["active"] = result["http_active"] or result["generation_active"]
            result["status"] = "running" if result["active"] else result["outcome"]
            result["duration_ms"] = round(((self.ended or time.monotonic()) - self.started) * 1000)
            result["timings"] = {k: dict(v) for k, v in self.timings.items()}
            result["idle_ms"] = max(0, result["duration_ms"] - self.timings.get("last_upstream", {}).get("elapsed_ms", 0))
            if detail:
                result["events"], result["logs"] = list(self.events), list(self.logs)
            return result


class PayloadObserver:
    """Inspect bounded SSE/JSON copies, never keep prompt or generated text."""
    def __init__(self, trace, *, sent=False, sse=True):
        self.trace, self.sent, self.sse = trace, sent, sse
        self.buffer = bytearray()
        self.skipping = False
        # 已确认无分隔符的前缀长度：下个块从末尾 3 字节重叠处继续扫，
        # 避免每块都从 0 重扫整个缓冲（最坏 O(n²)）。
        self._scan = 0

    def feed(self, chunk, final=False):
        raw = chunk.encode("utf-8") if isinstance(chunk, str) else bytes(chunk)
        with self.trace._lock:
            self.trace.data["sent_bytes" if self.sent else "generated_bytes"] += len(raw)
        if not self.sse:
            if self.skipping or len(self.buffer) + len(raw) > MAX_INSPECTION_BYTES:
                self._skipped(len(raw) + len(self.buffer))
                self.buffer.clear()
                self.skipping = True
                return
            self.buffer.extend(raw)
            if final:
                self._payload(bytes(self.buffer))
                self.buffer.clear()
            return
        # Bound working copies independently of network chunk size. A large
        # chunk containing many small frames is valid and must be inspected.
        step = min(64 * 1024, max(1, MAX_INSPECTION_BYTES))
        for offset in range(0, len(raw), step):
            self.buffer.extend(raw[offset:offset + step])
            self._ingest_sse()

    def _skipped(self, count):
        with self.trace._lock:
            self.trace.data["inspection_skipped_bytes"] += count

    def _ingest_sse(self):
        start = 0
        scan = self._scan
        while match := _FRAME_SEP_RE.search(self.buffer, scan):
            end = match.end()
            if self.skipping or end - start > MAX_INSPECTION_BYTES:
                self._skipped(end - start)
            else:
                frame = bytes(self.buffer[start:match.start()])
                data = b"\n".join(line[5:].lstrip() for line in frame.splitlines() if line.startswith(b"data:"))
                if data and data != b"[DONE]":
                    with self.trace._lock:
                        self.trace.data["sent_frames" if self.sent else "generated_frames"] += 1
                    self._payload(data)
            self.skipping = False
            start = scan = end
        remaining = len(self.buffer) - start
        if self.skipping or remaining > MAX_INSPECTION_BYTES:
            # Preserve the delimiter overlap even when first crossing the
            # budget. Otherwise a split \r\n\r\n can swallow the next frame.
            keep = min(3, remaining)
            self._skipped(remaining - keep)
            tail = self.buffer[-keep:] if keep else b""
            self.buffer.clear()
            self.buffer.extend(tail)
            self.skipping = True
            self._scan = 0
        else:
            # Compact once per bounded input slice, not once per frame.
            if start:
                del self.buffer[:start]
            self._scan = max(0, len(self.buffer) - 3)

    def _payload(self, raw):
        try:
            payload = json.loads(raw)
            for item in payload if isinstance(payload, list) else [payload]:
                self.trace.inspect_payload(item, sent=self.sent)
        except (ValueError, TypeError, AttributeError, RecursionError, OverflowError):
            # Diagnostics must never change protocol delivery.
            return


class RequestRegistry:
    def __init__(self, max_completed=500, ttl_seconds=3600, max_bytes=16 * 1024 * 1024):
        self.max_completed, self.ttl_seconds = max_completed, ttl_seconds
        self.max_bytes = max_bytes
        self._records = OrderedDict()
        self._lock = threading.Lock()
        # 摊销计数：create() 热路径每 _PRUNE_EVERY 次才做一次 O(N) 全表扫
        # 描；list()/get()（控制台驱动，低频）始终全扫保证上限语义精确。
        # 旧行为是每个请求 create()/get() 都扫全表 + 求和，高 RPS 下是纯
        # 浪费的锁内 O(N) 开销（2026-10 优化）。
        self._since_prune = 0

    _PRUNE_EVERY = 8

    def _prune(self):
        finished = [(key, trace) for key, trace in self._records.items() if trace.ended is not None]
        excess = len(finished) - self.max_completed
        for index, (key, trace) in enumerate(finished):
            if index < excess or time.monotonic() - trace.ended > self.ttl_seconds:
                self._records.pop(key, None)
        # 字节预算只统计**已结束**的 trace：活动请求的跟踪数据无法回收，
        # 计入预算的话活动集合一旦超限，会把全部历史记录一次性清空（控制台
        # 历史被破坏）而注册表仍在增长——两头落空。活动集合由并发上限与
        # per-trace 事件/日志上限自然约束。
        used = sum(trace.retained_bytes for trace in self._records.values()
                   if trace.ended is not None)
        for key, trace in finished:
            if used <= self.max_bytes:
                break
            if key in self._records:
                used -= trace.retained_bytes
                self._records.pop(key)
        # 活跃 trace 的保底时限：任何上游代码路径泄漏 lease（release 未被
        # 调用、挂死的后台生成）时，_settle 永不落 ended，TTL/数量/字节三重
        # 上限全部失效（慢泄漏且不可自愈）。最长的合法生成性下游可达
        # 7200s + 重试窗口，取 max(4×TTL, 4h) 作硬上限，超限强制退场并
        # 留下诊断事件，正常请求永不受影响（2026-10 兑底）。
        stuck_limit = max(4 * self.ttl_seconds, 4 * 3600)
        now = time.monotonic()
        for key, trace in list(self._records.items()):
            if trace.ended is None and now - trace.started > stuck_limit:
                trace.event("evicted", "请求跟踪记录超过活跃保底时限，已从监控历史退场"
                            "（上游任务可能未正常释放，请结合运行日志排查）。")
                self._records.pop(key, None)

    def create(self, path):
        trace = RequestTrace(path)
        with self._lock:
            self._since_prune += 1
            if self._since_prune >= self._PRUNE_EVERY:
                self._since_prune = 0
                self._prune()
            self._records[trace.id] = trace
        return trace

    def get(self, request_id):
        with self._lock:
            self._prune()
            return self._records.get(request_id)

    def list(self, *, status="all", query="", offset=0, limit=50):
        with self._lock:
            self._prune()
            rows = [trace.snapshot() for trace in reversed(self._records.values())]
        summary = {"connections": sum(r["http_active"] for r in rows),
                   "generations": sum(r["generation_active"] for r in rows),
                   "background": sum(r["generation_active"] and not r["http_active"] for r in rows),
                   "retry_wait": sum(r["generation_active"] and r["phase"] == "retry_wait" for r in rows),
                   "active": sum(r["active"] for r in rows)}
        query = query.casefold()
        rows = [r for r in rows if (status == "all" or (status == "active" and r["active"])
                                   or (status == "completed" and not r["active"])
                                   or r["status"] == status)
                and (not query or query in f"{r['id']} {r['model']} {r['generation_id'] or ''}".casefold())]
        rows.sort(key=lambda r: not r["active"])
        return {"summary": summary, "total": len(rows), "items": rows[offset:offset + limit],
                "offset": offset, "limit": limit, "retention": {"completed": self.max_completed, "seconds": self.ttl_seconds,
                                                                  "bytes": self.max_bytes}}

    def summary(self):
        """概览面板专用的轻量汇总：不构造任何 snapshot。

        /api/stats 每 3 秒轮询一次，旧行为为了 5 个计数把全部 trace 各拷一
        份 ~30 键 dict + timings 深拷贝再丢弃（列表接口另有专用路径），
        并与每个请求的 create() 共用同一把全局锁。直接读活跃标志位即可，
        快照语义与 list()["summary"] 完全一致（2026-10 优化）。
        """
        with self._lock:
            self._prune()
            connections = generations = background = retry_wait = active = 0
            for trace in self._records.values():
                data = trace.data
                http_active = data["http_active"]
                gen_active = data["generation_active"]
                connections += http_active
                generations += gen_active
                background += gen_active and not http_active
                active += http_active or gen_active
                retry_wait += gen_active and data["phase"] == "retry_wait"
        return {"connections": connections, "generations": generations,
                "background": background, "retry_wait": retry_wait, "active": active}

    def recent_metrics(self, window_seconds=900):
        """A bounded sample of retained primary requests, never a durable history."""
        now = time.time()
        success = errors = cancelled = running = 0
        replies = []
        channels = {}
        with self._lock:
            self._prune()
            records = list(self._records.values())
        for trace in records:
            with trace._lock:
                data = trace.data
                if data["created_at"] < now - window_seconds or data["parent_id"]:
                    continue
                channel = data["channel"] or "unknown"
                counts = channels.setdefault(channel, {"success": 0, "error": 0, "active": 0})
                if data["http_active"] or data["generation_active"]:
                    running += 1
                    counts["active"] += 1
                elif data["outcome"] == "error":
                    errors += 1
                    counts["error"] += 1
                elif data["outcome"] == "succeeded":
                    success += 1
                    counts["success"] += 1
                else:
                    cancelled += 1
                reply = trace.timings.get("first_reply", {}).get("elapsed_ms")
                if reply is not None:
                    replies.append(reply)
        replies.sort()
        def percentile(fraction):
            return replies[max(0, math.ceil(len(replies) * fraction) - 1)] if replies else None
        return {"window_seconds": window_seconds, "success": success, "error": errors,
                "cancelled": cancelled, "active": running,
                "sample_count": success + errors + cancelled + running,
                "reply_samples": len(replies), "first_reply_p50_ms": percentile(.5),
                "first_reply_p95_ms": percentile(.95), "channels": channels,
                "retained_sample": True}


request_registry = RequestRegistry()


def configure(**values):
    if trace := current_trace.get():
        trace.configure(**values)


def upstream_attempt():
    # 每次真实发起上游调用（含同 key 换出口）都刷新起点：日志里的
    # “第 N 次尝试等待 X 秒”应归因到当前出口，而不是把已失败出口的
    # 等待时间累加进来。
    _attempt_started.set(time.monotonic())
    if trace := current_trace.get():
        trace.attempt()


def attempt_started_at():
    """最近一次上游尝试（当前出口）的单调钟起点；尚未发起过则为 None。"""
    return _attempt_started.get()


def upstream_chunk():
    if trace := current_trace.get():
        trace.upstream_chunk()


def retry_wait(delay, error):
    if trace := current_trace.get():
        trace.retry(delay, error)


def record_usage(usage):
    if trace := current_trace.get():
        with trace._lock:
            trace.data["usage"] = numeric_usage(usage)

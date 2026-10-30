"""Observe Cookie framing and tool fragments without accepting a model reply."""
import time

from request_tracking import current_trace


class CookieStreamDiagnostics:
    def __init__(self, body):
        variables = body.get("variables") or {}
        tools = variables.get("tools") or []
        self.active = isinstance(tools, list) and any(isinstance(tool, dict) and tool.get("functionDeclarations") for tool in tools)
        self.trace = current_trace.get()
        self.started = time.monotonic()
        config = (variables.get("toolConfig") or {}).get("functionCallingConfig") or {}
        self.data = {"argument_stream_requested": bool(config.get("streamFunctionCallArguments")),
                     "text_chunks": 0, "text_bytes": 0, "json_objects": 0,
                     "fragment_parts": 0, "argument_fragments": 0, "full_tool_parts": 0}
        if self.active and self.trace:
            seconds = self.trace.data.get("first_reply_timeout_seconds")
            timeout = "关闭" if seconds == 0 else f"{seconds:g} 秒" if isinstance(seconds, (int, float)) else "沿用配置"
            requested = "已请求" if self.data["argument_stream_requested"] else "未请求"
            self.trace.event("cookie_tool_stream", f"Cookie 工具参数分片：{requested}；首回复超时：{timeout}。")
            self._publish()

    def _publish(self):
        if self.active and self.trace:
            with self.trace._lock:
                self.trace.data["cookie_tool_stream"] = dict(self.data)

    def _first(self, key, message):
        if key not in self.data:
            self.data[key] = round((time.monotonic() - self.started) * 1000)
            if self.trace:
                self.trace.event(key, message)

    def headers(self, status):
        if self.active:
            self.data["http_status"] = status
            self._first("cookie_response_headers", f"Cookie 上游响应头已到达：HTTP {status}，仍需等待有效模型数据。")
            self._publish()

    def text(self, chunk):
        if not self.active or not chunk:
            return
        self.data["text_chunks"] += 1
        self.data["text_bytes"] += len(chunk) if chunk.isascii() else len(chunk.encode("utf-8"))
        self._first("cookie_first_data", "收到 Cookie 上游首段正文数据；尚未代表完整 JSON 或模型回复。")
        self._publish()

    def object(self, obj):
        if not self.active:
            return
        self.data["json_objects"] += 1
        self._first("cookie_first_json", "已解析首个 Cookie JSON 对象；继续检查工具分片与有效回复。")
        for result in obj.get("results", []) if isinstance(obj.get("results"), list) else []:
            data = result.get("data") if isinstance(result, dict) else None
            candidates = data.get("candidates") if isinstance(data, dict) else None
            for candidate in candidates if isinstance(candidates, list) else []:
                content = candidate.get("content") if isinstance(candidate, dict) else None
                parts = content.get("parts") if isinstance(content, dict) else None
                for part in parts if isinstance(parts, list) else []:
                    call = part.get("functionCall") if isinstance(part, dict) else None
                    if not isinstance(call, dict):
                        continue
                    fragments = call.get("partialArgs", call.get("partial_args"))
                    if fragments or call.get("willContinue", call.get("will_continue")) is True:
                        self.data["fragment_parts"] += 1
                        self.data["argument_fragments"] += len(fragments) if isinstance(fragments, list) else 0
                        self._first("cookie_first_tool_fragment", "发现上游工具分片字段；分片通过校验后才作为有效生成进度。")
                    elif call.get("name"):
                        self.data["full_tool_parts"] += 1
        self._publish()

    def finish(self):
        if not self.active:
            return
        data = self.data
        message = (f"Cookie 工具流读取结束：文本块 {data['text_chunks']}，正文 {data['text_bytes']} 字节，"
                   f"JSON 对象 {data['json_objects']}，工具分片 Part {data['fragment_parts']}，"
                   f"参数片段 {data['argument_fragments']}，完整工具 Part {data['full_tool_parts']}。")
        if data["full_tool_parts"] and not data["fragment_parts"]:
            message += "本次上游仅返回完整工具对象，未观察到参数分片。"
        if self.trace:
            self.trace.event("cookie_tool_stream_summary", message)
        else:
            print(f"📊 [Studio 工具流] {message}")
        self._publish()

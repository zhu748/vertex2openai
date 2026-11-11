"""Request boundary errors shared by ASGI guards and application handlers."""

from fastapi.responses import JSONResponse

# QUA-P3-1（2026-11 审查收敛）：生成端点请求体体积上限的单一来源。
# 此前 middleware（OpenAI/Claude 端点）与 routes/gemini_native（原生透传）
# 各写一份 32MiB 常量，注释口径也不一致；原始 bytes 在解析、指纹、重试
# 全程被持有，不设限等于允许认证后的内存放大。32MiB 远大于任何正常
# 请求（含数张 4K 图的 base64）。消费方保留自己的模块级别名（测试按
# 模块 patch），值改动只改这里。
MAX_GENERATION_BODY_BYTES = 32 * 1024 * 1024

# QUA-P2-1（2026-11 审查收敛）：请求边界错误的三协议分派单一来源。
# 此前 ingress/body_too_large/validation 三个函数各自重复同一套
# OpenAI/Anthropic/Gemini 路径判断（新增协议要改三处）；现在分派只
# 在 _error_builder_for 发生一次，各函数只保留自己的 HTTP 语义
# （状态码、消息与少量协议差异参数）。三个协议的默认形状对齐既有
# 行为：Anthropic 走 claude_error + anthropic_error_type(code)；Gemini
# 的 error.status 默认 google_status_detail(code)；OpenAI 默认
# detail=消息 + error{type: invalid_request_error, code}。
_PARAM_UNSET = object()


def _error_builder_for(path: str):
    """按请求路径返回 (协议名, 错误体构造器)。

    构造器签名 build(code, message, **shape)，shape 仅承载协议间差异：
    anthropic_type（Anthropic 错误类型串，默认按状态码映射）、
    gemini_status（Gemini error.status，默认按状态码映射）、
    openai_type / openai_code（OpenAI error.type / error.code）、
    detail（OpenAI detail 字段，默认为消息本身）、with_error（是否附
    error 对象）、param（OpenAI error.param，传入 _PARAM_UNSET 表示不
    写该键，显式传 None 会序列化为 null）。
    """
    if path == "/v1/messages" or path.startswith("/v1/messages/"):
        def build(code, message, *, anthropic_type=None, gemini_status=None, openai_type=None,
                  openai_code=None, detail=None, with_error=True, param=_PARAM_UNSET):
            from routes.claude_api import claude_error
            from claude_protocol import anthropic_error_type
            return claude_error(code, anthropic_type or anthropic_error_type(code), message)
        return "anthropic", build
    if path.startswith(("/v1/models/", "/v1beta/models/")):
        def build(code, message, *, anthropic_type=None, gemini_status=None, openai_type=None,
                  openai_code=None, detail=None, with_error=True, param=_PARAM_UNSET):
            from upstreams.native_passthrough import _native_error
            return _native_error(code, message, status_detail=gemini_status)
        return "gemini", build

    def build(code, message, *, anthropic_type=None, gemini_status=None, openai_type=None,
              openai_code=None, detail=None, with_error=True, param=_PARAM_UNSET):
        content = {"detail": message if detail is None else detail}
        if with_error:
            content["error"] = {"message": message, "type": openai_type or "invalid_request_error",
                                "code": code if openai_code is None else openai_code}
            if param is not _PARAM_UNSET:
                content["error"]["param"] = param
        return JSONResponse(status_code=code, content=content)
    return "openai", build


def _no_store(response: JSONResponse, *, retry_after: bool = False) -> JSONResponse:
    response.headers["Cache-Control"] = "no-store"
    if retry_after:
        response.headers["Retry-After"] = "1"
    return response


def ingress_error_response(path: str, code: int, message: str) -> JSONResponse:
    _, build = _error_builder_for(path)
    # OpenAI 侧 401/503 用各自语义化的 error.type；Gemini 408 无默认映射需显式指定。
    return _no_store(
        build(code, message,
              openai_type="authentication_error" if code == 401 else
              "overloaded_error" if code == 503 else None,
              gemini_status="DEADLINE_EXCEEDED" if code == 408 else None),
        retry_after=code == 503)


def declared_body_exceeds(value: str, limit: int) -> bool:
    """Compare an ASCII Content-Length without converting an untrusted integer.

    Actual bytes are always counted as well. Invalid values belong to the HTTP
    server; Unicode digits and very long numbers must never cause a 500 here.
    """
    if not value or not value.isascii() or not value.isdecimal():
        return False
    digits = value.lstrip("0") or "0"
    maximum = str(limit)
    return len(digits) > len(maximum) or (len(digits) == len(maximum) and digits > maximum)


def body_too_large_response(path: str, limit: int) -> JSONResponse:
    size = f"{limit // (1024 * 1024)}MiB" if limit >= 1024 * 1024 else f"{max(1, limit // 1024)}KiB"
    message = f"请求体超过 {size} 上限。"
    return _no_store(_error_builder_for(path)[1](413, message))


def validation_error_response(path: str, errors: list) -> JSONResponse:
    # Pydantic's raw `input` may contain passwords, whole request bodies or
    # non-finite floats that JSONResponse cannot serialize. Keep actionable
    # field locations/messages and preserve management clients' detail shape.
    details = [{"type": str(error.get("type", "value_error"))[:100],
                "loc": [item if isinstance(item, int) else str(item)[:200]
                        for item in error.get("loc", ())[:16]],
                "msg": str(error.get("msg", "Invalid request"))[:500]}
               for error in errors[:50]]
    first = details[0] if details else {"loc": [], "msg": "Invalid request"}
    field = ".".join(str(item) for item in first["loc"])
    message = f"{field}: {first['msg']}" if field else first["msg"]
    kind, build = _error_builder_for(path)
    if kind == "openai":
        # OpenAI 客户端按 422 识别校验失败；管理端点保留 detail 列表形状。
        return _no_store(build(422, message, detail=details,
                               with_error=path == "/v1/chat/completions",
                               openai_code=422, param=field or None))
    return _no_store(build(400, message))

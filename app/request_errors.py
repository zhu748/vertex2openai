"""Request boundary errors shared by ASGI guards and application handlers."""

from fastapi.responses import JSONResponse


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
    if path == "/v1/messages" or path.startswith("/v1/messages/"):
        from routes.claude_api import claude_error
        response = claude_error(413, "invalid_request_error", message)
    elif path.startswith(("/v1/models/", "/v1beta/models/")):
        response = JSONResponse(status_code=413, content={
            "error": {"code": 413, "message": message, "status": "RESOURCE_EXHAUSTED"},
        })
    else:
        response = JSONResponse(status_code=413, content={
            "error": {"message": message, "type": "invalid_request_error", "code": 413},
        })
    response.headers["Cache-Control"] = "no-store"
    return response


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
    if path == "/v1/messages" or path.startswith("/v1/messages/"):
        from routes.claude_api import claude_error
        response = claude_error(400, "invalid_request_error", message)
    elif path.startswith(("/v1/models/", "/v1beta/models/")):
        response = JSONResponse(status_code=400, content={
            "error": {"code": 400, "message": message, "status": "INVALID_ARGUMENT"},
        })
    else:
        content = {"detail": details}
        if path == "/v1/chat/completions":
            content["error"] = {"message": message, "type": "invalid_request_error",
                                "code": 422, "param": field or None}
        response = JSONResponse(status_code=422, content=content)
    response.headers["Cache-Control"] = "no-store"
    return response

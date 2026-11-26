"""Shared routing policy for features the Cookie bridge cannot preserve."""

from model_resource import parse_google_model_resource
from schema_validation import SchemaValidationError


COOKIE_STRUCTURED_OUTPUT_DISABLED_MESSAGE = (
    "Cookie 通道默认不启用结构化输出：batchGraphql 私有协议的接受度未经真机验证。"
    "请先运行 scripts/probe_cookie_structured_output.py 探测，通过后在控制台打开"
    " cookie_structured_output，或改用 Express 通道模型。"
)


def cookie_model_id(resource: str, project_id: str) -> str:
    """Return the ID only when this Cookie account can address the same model."""
    if "/" not in resource and resource not in ("", ".", ".."):
        return resource
    parsed = parse_google_model_resource(resource)
    if parsed is not None and (parsed.project_id is None or
                               (parsed.project_id, parsed.location) == (project_id, "global")):
        return parsed.model_id
    raise SchemaValidationError("Cookie 通道仅支持本账号项目 global 区域下的 Google 模型；"
                                "请使用对应的模型 ID，或改用支持目标资源的通道。")


def cookie_feature_error(request, *, structured_output=False):
    if request.n is not None and request.n != 1:
        return "Cookie 通道暂不支持多个候选结果（n > 1），请使用支持该参数的 Express 模型。"
    if (request.logprobs is True or type(request.logprobs) is int
            or request.response_logprobs or request.top_logprobs is not None):
        return "Cookie 通道暂不支持 logprobs / top_logprobs，请使用 Express 通道。"
    if (request.response_format
            and request.response_format.get("type") not in (None, "text")
            and not structured_output):
        return COOKIE_STRUCTURED_OUTPUT_DISABLED_MESSAGE
    return None

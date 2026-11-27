"""Shared routing policy for features the Cookie bridge cannot preserve."""

from model_capabilities import split_model_alias, strip_image_and_search_suffixes
from model_resource import GoogleModelResource, parse_google_model_resource
from schema_validation import SchemaValidationError


COOKIE_STRUCTURED_OUTPUT_DISABLED_MESSAGE = (
    "Cookie 通道默认不启用结构化输出：batchGraphql 私有协议的接受度未经真机验证。"
    "请先运行 scripts/probe_cookie_structured_output.py 探测，通过后在控制台打开"
    " cookie_structured_output，或改用 Express 通道模型。"
)


class CookieModelScopeError(SchemaValidationError):
    def __init__(self) -> None:
        super().__init__("Cookie 通道仅支持本账号项目 global 区域下的 Google 模型；"
                         "请使用对应的模型 ID，或改用支持目标资源的通道。")


def cookie_model_scope(resource: str) -> GoogleModelResource | None:
    """Apply account eligibility only to an explicit Google publisher resource."""
    name = resource.strip()
    if not name.startswith("projects/"):
        return None
    return parse_google_model_resource(split_model_alias(name)[0])


def cookie_scope_matches(scope: GoogleModelResource, project_id: str) -> bool:
    return scope.location == "global" and scope.project_id == project_id


def cookie_model_id(resource: str, project_id: str) -> str:
    """Return the ID only when this Cookie account can address the same model."""
    if "/" not in resource and resource not in ("", ".", ".."):
        return resource
    parsed = parse_google_model_resource(resource)
    if parsed is not None and (parsed.project_id is None or cookie_scope_matches(parsed, project_id)):
        return parsed.model_id
    raise CookieModelScopeError()


def cookie_settings_model(requested_model: str, project_id: str, wire_model: str) -> str:
    """Use the validated explicit resource for translated policy, never for routing."""
    if "/" not in requested_model:
        return wire_model
    name, _ = split_model_alias(requested_model)
    resource = parse_google_model_resource(name)
    if (resource is None
            or (resource.project_id is not None and not cookie_scope_matches(resource, project_id))
            or strip_image_and_search_suffixes(resource.model_id)[0] != wire_model):
        return wire_model
    # Preserve an expressly configured single search/size alias's exact
    # record. The ordinary settings lookup already falls back for these.
    suffix = resource.model_id[len(wire_model):].lower()
    if suffix in ("", "-search", "-512", "-1k", "-2k", "-4k"):
        return name
    # Hand-combined aliases are not published by the catalog. Keep their
    # previous normalized base policy rather than inventing fallback order.
    return f"{name.rsplit('/', 1)[0]}/{wire_model}"


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

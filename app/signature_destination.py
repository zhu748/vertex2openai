"""Classify an actual Vertex attempt without retaining provider URLs or IDs."""
import hashlib
import re
import secrets


# None denotes consumers without attempt provenance (standalone converters).
# An empty digest means an actual attempt whose resource cannot be qualified.
UNQUALIFIED = b""
_KEY = secrets.token_bytes(32)
_PUBLISHER_MODEL = re.compile(
    r"projects/[a-z0-9][a-z0-9-]{0,127}/locations/[a-z0-9][a-z0-9-]{0,63}"
    r"/publishers/google/models/[a-zA-Z0-9][a-zA-Z0-9._-]{0,255}"
)
_VERTEX_ENDPOINTS = ("https://aiplatform.googleapis.com", "https://aiplatform.googleapis.com/")


def publisher_destination(resource: str) -> bytes:
    if not isinstance(resource, str) or len(resource) > 512 or not _PUBLISHER_MODEL.fullmatch(resource):
        return UNQUALIFIED
    return hashlib.blake2b(resource.encode("ascii"), key=_KEY, digest_size=32, person=b"vertex-route-v1").digest()


def sdk_destination(client, model: str, config: dict) -> bytes:
    """Use the actual SDK client's resolved host and its complete target resource."""
    # An override may alter host or path. Do not infer a project from the Key
    # record even if the original configured model had a pinned resource.
    if config.get("http_options") is not None:
        return UNQUALIFIED
    api = getattr(client, "_api_client", None)
    options = getattr(api, "_http_options", None)
    if (getattr(api, "vertexai", None) is not True
            or getattr(options, "base_url", None) not in _VERTEX_ENDPOINTS
            or getattr(options, "api_version", None) not in ("v1", "v1beta1")):
        return UNQUALIFIED
    return publisher_destination(model)


def studio_model_resource(project: str, model: str) -> str:
    return f"projects/{project}/locations/global/publishers/google/models/{model}"


def note_accepted_destination(request, destination: bytes) -> None:
    if (observer := request._signature_output_destination) is not None:
        observer(destination)

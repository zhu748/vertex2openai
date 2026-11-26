"""Recognize Google model paths without rewriting the caller's destination."""

import re
from typing import NamedTuple


class GoogleModelResource(NamedTuple):
    model_id: str
    project_id: str | None
    location: str | None


_GOOGLE_MODEL = re.compile(
    r"(?:(?:projects/(?P<project>[a-z0-9][a-z0-9-]{0,127})/locations/"
    r"(?P<location>[a-z0-9][a-z0-9-]{0,63})/)?publishers/google/)?"
    r"models/(?P<model>[A-Za-z0-9][A-Za-z0-9._-]{0,255})"
)


def parse_google_model_resource(resource: str) -> GoogleModelResource | None:
    """Typed Google publisher or relative model name, not an opaque endpoint."""
    match = _GOOGLE_MODEL.fullmatch(resource)
    if match:
        return GoogleModelResource(match["model"], match["project"], match["location"])
    return None


def model_id_for_policy(name: str) -> str:
    """Model capability/default lookup only; keep the full resource on the wire."""
    name = (name or "").strip()
    resource = parse_google_model_resource(name) if "/" in name else None
    return resource.model_id if resource else name

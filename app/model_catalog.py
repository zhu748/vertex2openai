"""Model directory variants shared by all API protocols.

Pure rules: callers supply effective settings; this module owns no runtime
state, HTTP client, synthetic transport decoder, or response delivery.
"""

from model_capabilities import (
    SUFFIX_MODES,
    get_profile,
    image_size_variant_ids,
    split_model_alias,
    supports_transport,
)


def listed_suffixes(model: str, settings: dict) -> tuple[str, ...]:
    """Only per-model settings publish aliases; a plain ID never opts in."""
    name, _ = split_model_alias(model)
    if name.lower().endswith("-search") or not supports_transport(name):
        return ()
    selected = settings.get("anti_truncation_mode", "off")
    return tuple(suffix for suffix, mode in SUFFIX_MODES.items()
                 if suffix != "-antitruncation" and selected in (mode, "both"))


def search_variant_available(model_id: str) -> bool:
    """Search aliases are available for Gemini text models across all protocols.

    Capability profiles also recognize image families without 'image' in their
    names (for example nano-banana); checking a substring alone misses them.
    """
    lowered = (model_id or "").lower()
    if "gemini" not in lowered or lowered.endswith("-search"):
        return False
    return not get_profile(lowered)["is_image"]


def catalog_variant_ids(model_id: str, settings: dict) -> list[str]:
    """Expand OpenAI/Anthropic IDs using the model's effective settings.

    Plain entries publish standard variants plus enabled synthetic transports.
    An explicit synthetic alias is published only when its mode is enabled.
    """
    plain_id, mode = split_model_alias(model_id)
    available = listed_suffixes(plain_id, settings)
    if mode is not None:
        return [model_id] if f"-antitruncation-{mode}" in available else []
    return [*standard_variant_ids(model_id),
            *(f"{model_id}{suffix}" for suffix in available)]


def standard_variant_ids(model_id: str) -> list[str]:
    """Base/search/image variants accepted by all three protocols."""
    ids = [model_id]
    if search_variant_available(model_id):
        ids.append(f"{model_id}-search")
    ids.extend(f"{model_id}{suffix}" for suffix in image_size_variant_ids(model_id))
    return ids


def native_catalog_ids(raw_models: list[str]) -> list[str]:
    """Native requests support standard aliases; synthetic transports are separate."""
    return list(dict.fromkeys(
        candidate for model_id in sorted(raw_models)
        for candidate in standard_variant_ids(model_id)
    ))

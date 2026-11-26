"""Read-only catalog routing eligibility, not a provider or account health check."""
from dataclasses import dataclass
from enum import IntFlag

from channel_capabilities import cookie_scope_matches
from model_capabilities import split_model_alias
from model_resource import parse_google_model_resource


class NativeMethods(IntFlag):
    GENERATE = 1  # both generateContent and streamGenerateContent
    COUNT_TOKENS = 2


def _cookie_can_address(model: str, projects: frozenset[str]) -> bool:
    if not projects:
        return False
    if "/" not in model:
        return model not in ("", ".", "..")
    resource = parse_google_model_resource(model)
    if resource is None:
        return False  # Cookie cannot translate opaque endpoints or non-Google publishers.
    project = resource.project_id
    return project is None or (project in projects and cookie_scope_matches(resource, project))


@dataclass(frozen=True, slots=True)
class CatalogAvailability:
    # Use committed, enabled rows and exact projects, not temporary cooldowns or
    # upstream authorization. Discovery must never select/reserve a credential.
    translated_express: bool
    translated_cookie_projects: frozenset[str]
    native_express: bool
    native_cookie_projects: frozenset[str]
    native_count_tokens: bool

    @classmethod
    def read(cls, state, key_manager):
        get_total = getattr(key_manager, "get_total_keys", None)
        has_key = callable(get_total) and get_total() > 0
        mode = state.get_channel_mode()
        fallback = state.get_setting("cross_channel_fallback", False)
        enabled = frozenset(r["project_id"] for r in state.get_cookie_accounts(committed=True) if r["enabled"])
        complete = enabled - {""}
        native_projects = complete
        if enabled and not complete:
            legacy = state.get_project_id()
            if isinstance(legacy, str) and (legacy := legacy.strip()):
                native_projects = frozenset({legacy})

        return cls(
            # Cookie-only translated routes can reach Express after the
            # missing-account 401, but not after a scope/project 400.
            translated_express=has_key and (mode != "web_proxy" or fallback and not enabled),
            translated_cookie_projects=complete if mode != "api_key" or fallback else frozenset(),
            # Native generation chooses Cookie before validating the model,
            # while countTokens can still use a key in the very same mode.
            native_express=has_key and (mode != "web_proxy" or not enabled),
            native_cookie_projects=native_projects,
            native_count_tokens=has_key,
        )

    def supports_translated(self, model: str) -> bool:
        return self.translated_express or _cookie_can_address(
            split_model_alias(model)[0], self.translated_cookie_projects)

    def native_methods(self, model: str) -> NativeMethods:
        methods = NativeMethods.COUNT_TOKENS if self.native_count_tokens else NativeMethods(0)
        # The POST receiver strips one optional relative "models/" prefix.
        if self.native_express or _cookie_can_address(model.removeprefix("models/"), self.native_cookie_projects):
            methods |= NativeMethods.GENERATE
        return methods

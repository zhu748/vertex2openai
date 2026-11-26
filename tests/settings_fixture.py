"""Supply one already-saved scalar configuration to offline adapter tests."""
from unittest.mock import patch

from config import DEFAULT_SETTINGS


def saved_settings(state, values):
    # Some adapter controls are read individually; request policies are read
    # atomically. Persistence tests use real AppState and do not use this seam.
    def scalar(key, default=None):
        return values.get(key, default)
    def snapshot(*keys):
        return {key: values.get(key, DEFAULT_SETTINGS.get(key)) for key in keys}
    return patch.multiple(state, get_setting=scalar, get_committed_setting=scalar, get_committed_settings=snapshot)

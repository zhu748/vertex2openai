"""Keep console HTML and its scripts on the same deployment version."""

from functools import lru_cache
import hashlib
from pathlib import Path
import re

from fastapi.staticfiles import StaticFiles
from starlette.datastructures import QueryParams

STATIC_DIR = Path(__file__).parent / "static"


@lru_cache(maxsize=1)
def _asset_manifest(directory: Path) -> tuple[str, frozenset[str]]:
    """Packaged assets, like the HTML templates, stay fixed for the process lifetime."""
    digest = hashlib.sha256()
    names = set()
    for path in sorted(directory.iterdir()):
        if path.suffix in (".js", ".css") and path.is_file():
            digest.update(path.name.encode())
            digest.update(path.read_bytes())
            names.add(path.name)
    return digest.hexdigest()[:16], frozenset(names)


def versioned_assets(template: str) -> str:
    version, _ = _asset_manifest(STATIC_DIR)
    return re.sub(r'((?:src|href)="/static/[^"?]+\.(?:js|css))"',
                  lambda match: f'{match[1]}?v={version}"', template)


_IMMUTABLE_CACHE = "public, max-age=31536000, immutable"
_REVALIDATE = "no-cache, max-age=0, must-revalidate"


class ConsoleStaticFiles(StaticFiles):
    async def get_response(self, path, scope):
        response = await super().get_response(path, scope)
        version, names = _asset_manifest(STATIC_DIR)
        versions = QueryParams(scope.get("query_string", b"")).getlist("v")
        # Stale/ambiguous version URLs address the current bytes on disk, not
        # the historical bytes the client requested. Never pin those responses.
        response.headers["Cache-Control"] = _IMMUTABLE_CACHE if path in names and versions == [version] else _REVALIDATE
        return response

"""Keep console HTML and its scripts on the same deployment version."""

import hashlib
from pathlib import Path
import re

from fastapi.staticfiles import StaticFiles

STATIC_DIR = Path(__file__).parent / "static"


def versioned_assets(template: str) -> str:
    digest = hashlib.sha256()
    for path in sorted(STATIC_DIR.iterdir()):
        if path.suffix in (".js", ".css"):
            digest.update(path.name.encode())
            digest.update(path.read_bytes())
    version = digest.hexdigest()[:16]
    return re.sub(r'((?:src|href)="/static/[^"?]+\.(?:js|css))"',
                  lambda match: f'{match[1]}?v={version}"', template)


class ConsoleStaticFiles(StaticFiles):
    async def get_response(self, path, scope):
        response = await super().get_response(path, scope)
        # Also revalidate old unversioned URLs; CDN/browser heuristics otherwise
        # keep a previous deployment's JavaScript for hours.
        response.headers["Cache-Control"] = "no-cache, max-age=0, must-revalidate"
        return response

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


# ?v=<内容哈希> 的 URL 可以永久缓存：内容一变哈希就变，URL 随之改变。
_IMMUTABLE_CACHE = "public, max-age=31536000, immutable"


class ConsoleStaticFiles(StaticFiles):
    async def get_response(self, path, scope):
        response = await super().get_response(path, scope)
        if b"v=" in scope.get("query_string", b""):
            # 版本化 URL：浏览器一年内不再回源，弱网下控制台首屏省掉
            # 全部静态资源的 revalidate 往返（内容变更时哈希/URL 一起变，
            # 不存在旧缓存命中问题）。
            response.headers["Cache-Control"] = _IMMUTABLE_CACHE
        else:
            # 旧的无版本 URL（书签/外部引用）仍要求 revalidate，
            # 否则浏览器会长期持有上一个部署的 JS。
            response.headers["Cache-Control"] = "no-cache, max-age=0, must-revalidate"
        return response

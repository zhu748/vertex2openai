"""Capability URLs for generated chat images (no client Authorization needed)."""
from contextlib import asynccontextmanager
from typing import Annotated

import anyio
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from image_delivery import OpenImage, open_image_asset

router = APIRouter()


async def _download_asset(name: str):
    asset = await anyio.to_thread.run_sync(open_image_asset, name)
    if asset is None:
        raise HTTPException(404, "图片不存在或已过期。")
    try:
        yield asset
    finally:
        # Local read-only file: close even on cancellation or a failed ASGI send.
        asset.file.close()


class _PinnedImageResponse(FileResponse):
    def __init__(self, asset):
        self.file = asset.file
        super().__init__(asset.path, stat_result=asset.stat, headers={
            "Cache-Control": f"private, max-age={min(3600, asset.remaining)}",
            "X-Content-Type-Options": "nosniff",
            "Referrer-Policy": "no-referrer",
        })

    @asynccontextmanager
    async def _open_file(self):
        # Keep Starlette's HEAD, If-Range, and multipart range implementation.
        # The request-scoped dependency owns this already open descriptor.
        yield anyio.wrap_file(self.file)

    async def __call__(self, scope, receive, send):
        extensions = scope.get("extensions", {})
        if "http.response.pathsend" in extensions:
            # Path-send makes the server reopen a name that may no longer exist.
            scope = {**scope, "extensions": {k: v for k, v in extensions.items()
                                             if k != "http.response.pathsend"}}
        await super().__call__(scope, receive, send)


@router.api_route("/v1/generated-images/{name}", methods=["GET", "HEAD"])
async def generated_image(asset: Annotated[OpenImage, Depends(_download_asset, scope="request")]):
    return _PinnedImageResponse(asset)

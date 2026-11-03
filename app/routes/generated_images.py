"""Capability URLs for generated chat images (no client Authorization needed)."""
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from image_delivery import resolve_image


router = APIRouter()


@router.api_route("/v1/generated-images/{name}", methods=["GET", "HEAD"])
def generated_image(name: str):
    path = resolve_image(name)
    if path is None:
        raise HTTPException(404, "图片不存在或已过期。")
    return FileResponse(path, headers={
        "Cache-Control": "private, max-age=3600",
        "X-Content-Type-Options": "nosniff",
        "Referrer-Policy": "no-referrer",
    })

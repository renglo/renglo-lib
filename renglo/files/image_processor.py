"""Validate, normalize, and resize uploaded raster images."""

from __future__ import annotations

from io import BytesIO
from typing import Tuple

from PIL import Image, ImageOps, UnidentifiedImageError

ALLOWED_IMAGE_MIME_TYPES = frozenset({"image/jpeg", "image/png"})
DEFAULT_IMAGE_SIZE: Tuple[int, int] = (500, 500)
OUTPUT_MIME_TYPE = "image/png"
OUTPUT_EXTENSION = "png"


class ImageUploadError(ValueError):
    """Raised when uploaded bytes are not a valid/processable raster image."""


def is_raster_image_mime(mime_type: str) -> bool:
    return mime_type in ALLOWED_IMAGE_MIME_TYPES


def _looks_like_jpeg_or_png(data: bytes) -> bool:
    if len(data) < 8:
        return False
    if data[:3] == b"\xff\xd8\xff":
        return True
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return True
    return False


def process_uploaded_image(
    raw_bytes: bytes,
    *,
    size: Tuple[int, int] = DEFAULT_IMAGE_SIZE,
) -> bytes:
    """Verify JPEG/PNG content, convert to PNG, and resize to a square cover crop."""
    if not raw_bytes:
        raise ImageUploadError("Empty file")

    if not _looks_like_jpeg_or_png(raw_bytes):
        raise ImageUploadError("File is not a valid JPEG or PNG image")

    try:
        with Image.open(BytesIO(raw_bytes)) as img:
            img.verify()
    except (UnidentifiedImageError, OSError) as exc:
        raise ImageUploadError("File is not a valid image") from exc

    try:
        with Image.open(BytesIO(raw_bytes)) as img:
            img = ImageOps.exif_transpose(img)
            if img.mode not in ("RGB", "RGBA"):
                img = img.convert("RGBA")
            fitted = ImageOps.fit(
                img,
                size,
                method=Image.Resampling.LANCZOS,
                centering=(0.5, 0.5),
            )
            out = BytesIO()
            fitted.save(out, format="PNG", optimize=True)
            return out.getvalue()
    except (UnidentifiedImageError, OSError) as exc:
        raise ImageUploadError("Could not process image") from exc

import hashlib
import os
import logging
from io import BytesIO
from typing import Any

import asyncio
from PIL import Image

from .constants import CACHE_DIR, PREVIEW_JPEG_QUALITY_IMAGE
from .storage_helpers import call_storage

logger = logging.getLogger(__name__)


def _cache_path(filename: str) -> str:
    name_hash = hashlib.md5(filename.encode()).hexdigest()
    return os.path.join(CACHE_DIR, f"{name_hash}.jpg")


def generate_preview(filename: str, is_vid: bool, storage: Any, size: int = 300) -> bytes:
    """Sync preview generation (uses sync storage methods)."""
    cache_path = _cache_path(filename)
    if os.path.isfile(cache_path):
        try:
            with open(cache_path, 'rb') as f:
                return f.read()
        except Exception:
            pass

    if is_vid:
        frame_data = storage.extract_video_frame(filename, timestamp=1.0)
        if not frame_data:
            raise FileNotFoundError(filename)
        img = Image.open(BytesIO(frame_data))
    else:
        data = storage.download_file(filename)
        if data is None:
            raise FileNotFoundError(filename)
        img = Image.open(BytesIO(data))

    img.thumbnail((size, size), Image.Resampling.LANCZOS)
    if img.mode in ('RGBA', 'LA', 'P'):
        background = Image.new('RGB', img.size, (255, 255, 255))
        background.paste(img, mask=img.split()[-1] if img.mode in ('RGBA', 'LA') else None)
        img = background

    with BytesIO() as bio:
        img.save(bio, format='JPEG', quality=PREVIEW_JPEG_QUALITY_IMAGE)
        preview_bytes = bio.getvalue()

    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        with open(cache_path, 'wb') as f:
            f.write(preview_bytes)
    except Exception:
        logger.debug('Failed to write preview cache for %s', filename)

    return preview_bytes


async def async_generate_preview(filename: str, is_vid: bool, storage: Any, size: int = 300) -> bytes:
    """Async preview generation using `call_storage` to dispatch to async/sync storage methods."""
    cache_path = _cache_path(filename)
    if os.path.isfile(cache_path):
        try:
            with open(cache_path, 'rb') as f:
                return f.read()
        except Exception:
            pass

    try:
        if is_vid:
            frame_data = await call_storage(storage, 'extract_video_frame', filename, timestamp=1.0)
            if not frame_data:
                raise FileNotFoundError(filename)
            img = Image.open(BytesIO(frame_data))
        else:
            data = await call_storage(storage, 'download_file', filename)
            if data is None:
                raise FileNotFoundError(filename)
            img = Image.open(BytesIO(data))

        img.thumbnail((size, size), Image.Resampling.LANCZOS)
        if img.mode in ('RGBA', 'LA', 'P'):
            background = Image.new('RGB', img.size, (255, 255, 255))
            background.paste(img, mask=img.split()[-1] if img.mode in ('RGBA', 'LA') else None)
            img = background

        with BytesIO() as bio:
            img.save(bio, format='JPEG', quality=PREVIEW_JPEG_QUALITY_IMAGE)
            preview_bytes = bio.getvalue()

        try:
            os.makedirs(CACHE_DIR, exist_ok=True)
            loop = asyncio.get_running_loop()
            def _write_cache():
                with open(cache_path, 'wb') as f:
                    f.write(preview_bytes)
            await loop.run_in_executor(None, _write_cache)
        except Exception:
            logger.debug('Failed to write preview cache for %s', filename)

        return preview_bytes
    except FileNotFoundError:
        raise
    except Exception as e:
        logger.exception('Failed to generate preview for %s: %s', filename, e)
        raise

import hashlib
import os
import logging
import json
from io import BytesIO
from typing import Any, Set

import asyncio
from PIL import Image

from .constants import CACHE_DIR, PREVIEW_JPEG_QUALITY_IMAGE
from .storage_helpers import call_storage

logger = logging.getLogger(__name__)

PREVIEW_CACHE_METADATA = "/data/preview_cache/cache_manifest.json"


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


def save_preview_cache() -> int:
    """
    Save the current preview cache to disk.
    Scans CACHE_DIR for all jpg files and copies them to /data/preview_cache.
    Also saves a manifest with filenames.
    
    Returns:
        Number of cached previews saved to disk.
    """
    try:
        if not os.path.isdir(CACHE_DIR):
            logger.info(f"Cache directory does not exist: {CACHE_DIR}")
            preview_cache_dir = os.path.dirname(PREVIEW_CACHE_METADATA)
            os.makedirs(preview_cache_dir, exist_ok=True)
            cache_manifest = {'cached_previews': [], 'count': 0}
            with open(PREVIEW_CACHE_METADATA, 'w') as f:
                json.dump(cache_manifest, f, indent=2)
            return 0
        
        cached_files = []
        try:
            all_files = os.listdir(CACHE_DIR)
            logger.debug(f"Files in {CACHE_DIR}: {all_files}")
            for filename in all_files:
                if filename.endswith('.jpg'):
                    cached_files.append(filename)
        except OSError as e:
            logger.warning(f"Failed to list files in {CACHE_DIR}: {e}")
            return 0
        
        logger.info(f"Found {len(cached_files)} jpg files in cache: {cached_files}")
        
        preview_cache_dir = os.path.dirname(PREVIEW_CACHE_METADATA)
        os.makedirs(preview_cache_dir, exist_ok=True)
        
        saved_count = 0
        for filename in cached_files:
            src_path = os.path.join(CACHE_DIR, filename)
            dst_path = os.path.join(preview_cache_dir, filename)
            try:
                if os.path.isfile(src_path):
                    with open(src_path, 'rb') as src:
                        with open(dst_path, 'wb') as dst:
                            dst.write(src.read())
                    saved_count += 1
                    logger.debug(f"Saved cache file: {filename}")
            except Exception as e:
                logger.warning(f"Failed to save cache file {filename}: {e}")
        
        cache_manifest = {
            'cached_previews': cached_files,
            'count': saved_count
        }
        
        with open(PREVIEW_CACHE_METADATA, 'w') as f:
            json.dump(cache_manifest, f, indent=2)
        
        logger.info(f"Saved preview cache with {saved_count} files to {preview_cache_dir}")
        return saved_count
    except Exception as e:
        logger.exception(f"Failed to save preview cache: {e}")
        return 0


def restore_preview_cache() -> int:
    """
    Restore preview cache from disk by copying cached files from manifest back to CACHE_DIR.
    
    Returns:
        Number of cache files restored.
    """
    try:
        if not os.path.isfile(PREVIEW_CACHE_METADATA):
            logger.info(f"No preview cache manifest found at {PREVIEW_CACHE_METADATA}")
            return 0
        
        with open(PREVIEW_CACHE_METADATA, 'r') as f:
            cache_manifest = json.load(f)
        
        cached_files = cache_manifest.get('cached_previews', [])
        preview_cache_dir = os.path.dirname(PREVIEW_CACHE_METADATA)
        restored_count = 0
        
        os.makedirs(CACHE_DIR, exist_ok=True)
        
        for filename in cached_files:
            src_path = os.path.join(preview_cache_dir, filename)
            dst_path = os.path.join(CACHE_DIR, filename)
            try:
                if os.path.isfile(src_path) and not os.path.isfile(dst_path):
                    with open(src_path, 'rb') as src:
                        with open(dst_path, 'wb') as dst:
                            dst.write(src.read())
                    restored_count += 1
            except Exception as e:
                logger.debug(f"Failed to restore cache file {filename}: {e}")
        
        logger.info(f"Restored {restored_count} preview cache files from {preview_cache_dir}")
        return restored_count
    except Exception as e:
        logger.exception(f"Failed to restore preview cache: {e}")
        return 0

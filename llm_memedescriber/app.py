from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
from io import BytesIO
import os
from typing import Dict, Optional, Any, List
import logging

from pydantic import BaseModel

from .config import load_settings, configure_logging, parse_interval
from .constants import *
from .constants import _get_extension
from .db import init_db, get_stats, get_meme_by_filename
from .main import App
from .storage import WebDavStorage
from .storage_workers import StorageWorkerPool
from .genai_client import get_client
from .search import rebuild_index, add_meme_to_index, search_memes as whoosh_search
from .deduplication import (
    find_duplicate_groups,
    mark_false_positive,
    merge_duplicates,
    hamming_distance,
    add_pair_exception,
    remove_pair_exception,
    list_pair_exceptions,
)
from .dup_helpers import get_group_members, get_groups_for_filename
from .storage_helpers import compute_and_persist_phash
from .preview_helpers import generate_preview, async_generate_preview, restore_preview_cache, save_preview_cache, cleanup_orphaned_cache
from sqlmodel import select
from .db_helpers import session_scope
from .models import Meme, DuplicateGroup as DBDuplicateGroup, MemeDuplicateGroup as DBDupeLink
import datetime
from sqlalchemy import text

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    """Manage application lifecycle (startup and shutdown events)."""
    settings = load_settings()
    configure_logging(settings)
    logger.info("Starting llm_memedescriber FastAPI app (preview cache: %s)", CACHE_DIR)
    
    try:
        logger.debug("Restoring preview cache from disk...")
        restored = restore_preview_cache()
        logger.info("Preview cache restored: %d entries", restored)
    except Exception:
        logger.exception("Failed to restore preview cache, continuing with empty cache")
    
    app_instance.state.engine = init_db()
    
    try:
        with session_scope(app_instance.state.engine) as session:
            removed_memes = session.exec(select(Meme).where(Meme.status == 'removed')).all()
            if removed_memes:
                removed_filenames = {meme.filename for meme in removed_memes}
                for meme in removed_memes:
                    session.delete(meme)
                session.commit()
                logger.info(f"Cleaned up {len(removed_memes)} removed memes from database")
                # Clean up cache entries for removed memes
                cleanup_orphaned_cache(set(session.exec(select(Meme.filename)).all()) if session.exec(select(Meme)).first() else set())
    except Exception:
        logger.exception("Failed to clean up removed memes from database")
    storage = None
    if getattr(settings, 'webdav_url', None):
        base_url = settings.webdav_url.rstrip('/') + '/' + settings.webdav_path.lstrip('/')
        base_storage = WebDavStorage(base_url, auth=(settings.webdav_username, settings.webdav_password))
        try:
            storage_workers = int(getattr(settings, 'storage_workers', DEFAULT_STORAGE_WORKERS) or DEFAULT_STORAGE_WORKERS)
        except Exception:
            storage_workers = DEFAULT_STORAGE_WORKERS
        try:
            storage_concurrency = int(getattr(settings, 'storage_concurrency', DEFAULT_STORAGE_CONCURRENCY) or DEFAULT_STORAGE_CONCURRENCY)
        except Exception:
            storage_concurrency = DEFAULT_STORAGE_CONCURRENCY

        storage = StorageWorkerPool(base_storage, max_workers=storage_workers, max_concurrent=storage_concurrency)

    genai_client = None
    if getattr(settings, 'google_genai_api_key', None):
        genai_client = get_client(settings.google_genai_api_key)

    interval = 60
    if getattr(settings, 'run_interval', None):
        try:
            interval = parse_interval(settings.run_interval)
        except Exception:
            logger.exception("Invalid run_interval; defaulting to 60s")

    app_instance.state.app_instance = App(settings=settings, storage=storage, genai_client=genai_client, engine=app_instance.state.engine, interval_seconds=interval)
    app_instance.state._started = True

    try:
        if getattr(settings, 'backfill_from_listing_on_empty_db', True) and storage is not None:
            with session_scope(app_instance.state.engine) as session:
                exists = session.exec(select(Meme)).first() is not None
            if not exists:
                logger.info("DB appears empty; checking for listing.json on WebDAV to optionally backfill")
                try:
                    entries = await getattr(storage, 'async_list_files', storage.list_files)('/', recursive=False)
                    has_listing = any(
                        (not (e.get('is_dir') or False)) and (e.get('name') == 'listing.json' or str(e.get('path', '')).rstrip('/').endswith('/listing.json'))
                        for e in entries
                    )
                except Exception:
                    has_listing = False
                if not has_listing:
                    logger.info("No listing.json found on WebDAV; starting with empty database")
                else:
                    try:
                        result = app_instance.state.app_instance.import_listing_into_db()
                        created = result.get('created', 0)
                        updated = result.get('updated', 0)
                        skipped = result.get('skipped', 0)
                        if created == 0 and updated == 0:
                            logger.info("listing.json found but contained no entries; starting with empty database")
                        else:
                            logger.info("Backfill completed: created=%s, updated=%s, skipped=%s", created, updated, skipped)
                    except Exception:
                        logger.exception("Backfill from listing.json failed")
    except Exception:
        logger.exception("Error checking DB emptiness or performing backfill")

    if getattr(settings, 'auto_start_worker', False):
        logger.info("auto_start_worker enabled")
        try:
            logger.info("Running initial sync to populate database...")
            result = app_instance.state.app_instance.sync_and_process()
            logger.info("Initial sync completed: added=%s, removed=%s, unfilled=%s", 
                       result.get('added', 0), result.get('removed', 0), result.get('unfilled', 0))
        except Exception:
            logger.exception("Initial sync failed, continuing anyway")
        
        try:
            logger.info("Building Whoosh search index...")
            rebuild_index(app_instance.state.engine)
            logger.info("Search index ready")
        except Exception:
            logger.exception("Failed to build search index, search may be unavailable")
        
        try:
            logger.info("Initializing perceptual hashes for deduplication...")
            if not storage:
                logger.warning("Storage not configured, skipping phash initialization")
                return
            else:
                filenames = []
            with session_scope(app_instance.state.engine) as session:
                rows = session.exec(select(Meme).where(Meme.phash == None)).all()
                filenames = [r.filename for r in rows]

            if filenames:
                successful = 0
                failed = 0

                async def process_phash(filename: str) -> bool:
                    try:
                        result = await compute_and_persist_phash(filename, storage, app_instance.state.engine, timestamp=1.0)
                        return result is not None
                    except Exception:
                        logger.exception("Exception while processing phash for %s", filename)
                        return False

                try:
                    tasks = [asyncio.create_task(process_phash(fn)) for fn in filenames]
                    results = await asyncio.gather(*tasks)
                    for r in results:
                        if r:
                            successful += 1
                        else:
                            failed += 1
                except Exception:
                    logger.exception("Error during async phash initialization")

                logger.info(f"Perceptual hashes initialized: {successful}/{len(filenames)} successful, {failed} failed")
                if failed > len(filenames) * 0.5:
                    logger.warning("More than 50% memes failed phash calculation. Check storage and image formats.")
            else:
                logger.info("All memes already have perceptual hashes")
        except Exception:
            logger.exception("Failed to initialize perceptual hashes, deduplication may be unavailable")

        try:
            preview_workers = int(getattr(settings, 'preview_workers', DEFAULT_PREVIEW_WORKERS) or DEFAULT_PREVIEW_WORKERS)
            if preview_workers and storage:
                logger.info("Pre-generating preview thumbnails using %s workers...", preview_workers)

                to_generate = []
                with session_scope(app_instance.state.engine) as session:
                    rows = session.exec(select(Meme)).all()
                    for r in rows:
                        cache_path = _get_cache_path(r.filename)
                        if not os.path.isfile(cache_path):
                            to_generate.append((r.filename, r.filename.lower().rsplit('.', 1)[-1] if '.' in r.filename else ''))

                if to_generate:
                    success = 0
                    failed = 0

                    semaphore = asyncio.Semaphore(preview_workers)

                    async def do_preview(filename: str, is_vid: bool):
                        async with (asyncio.Semaphore(1)):
                            try:
                                await _aget_or_generate_preview(filename, is_vid, storage, PREVIEW_SIZE)
                                return True
                            except Exception:
                                return False

                    tasks = []
                    for filename, ext in to_generate:
                        is_vid = ext in VIDEO_EXTENSIONS
                        tasks.append(asyncio.create_task(do_preview(filename, is_vid)))

                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for r in results:
                        if isinstance(r, Exception):
                            failed += 1
                        elif r:
                            success += 1
                        else:
                            failed += 1

                    logger.info("Preview generation complete: %d succeeded, %d failed", success, failed)
                else:
                    logger.info("No previews needed pre-generation; cache already populated")
        except Exception:
            logger.exception("Failed during pre-generation of previews")
        
        try:
            logger.info("Starting background worker thread...")
            app_instance.state.app_instance.start()
        except Exception:
            logger.exception("Failed to start worker thread")
    
    yield
    
    logger.info("Shutting down llm_memedescriber FastAPI app")
    
    # Save preview cache to disk
    try:
        logger.info("Saving preview cache to disk...")
        saved = save_preview_cache()
        logger.info("Preview cache saved: %d entries", saved)
    except Exception:
        logger.exception("Failed to save preview cache on shutdown")
    
    try:
        if getattr(app_instance.state, 'app_instance', None):
            app_inst = app_instance.state.app_instance
            if app_inst.storage:
                try:
                    logger.info("Exporting listing.json on shutdown")
                    app_inst.export_listing_to_webdav()
                    logger.info("Listing export completed on shutdown")
                except Exception:
                    logger.exception("Failed to export listing on shutdown")
            try:
                logger.info("Stopping app worker")
                app_inst.stop()
                logger.info("App worker stopped")
            except Exception:
                logger.exception("Error stopping app instance")
    finally:
        app_instance.state._started = False
        logger.info("Shutdown event completed")


app = FastAPI(title="llm_memedescriber", description="Meme describing service", version="0.0.1", lifespan=lifespan)

# Setup templates
templates_dir = os.path.join(os.path.dirname(__file__), 'templates')
templates = Jinja2Templates(directory=templates_dir)

static_dir = os.path.join(os.path.dirname(__file__), 'static')
if os.path.isdir(static_dir):
    app.mount('/static', StaticFiles(directory=static_dir), name='static')


class UpdateMemeRequest(BaseModel):
    """Request body for updating meme metadata."""
    category: Optional[str] = None
    keywords: Optional[str] = None
    description: Optional[str] = None


class DuplicateInfo(BaseModel):
    """Information about a single duplicate."""
    filename: str
    similarity: int  # hamming distance (0-64)
    preview_url: Optional[str] = None


class DuplicateGroup(BaseModel):
    """Group of duplicate memes."""
    primary: DuplicateInfo
    duplicates: List[DuplicateInfo]


class MergeDuplicatesRequest(BaseModel):
    """Request to merge duplicate memes."""
    primary_filename: str
    duplicate_filenames: List[str]
    merge_metadata: bool = True
    metadata_sources: Optional[List[str]] = None


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.state._started = False

def _get_cache_path(filename: str) -> str:
    """Get safe cache file path from filename hash."""
    import hashlib
    name_hash = hashlib.md5(filename.encode()).hexdigest()
    return os.path.join(CACHE_DIR, f"{name_hash}.jpg")


def _get_mime_type(ext: str) -> str:
    """Get MIME type based on file extension."""
    ext = ext.lower()
    mime_types = {
        'jpg': 'image/jpeg',
        'jpeg': 'image/jpeg',
        'png': 'image/png',
        'webp': 'image/webp',
        'gif': 'image/gif',
        'bmp': 'image/bmp',
        'tiff': 'image/tiff',
        'mp4': 'video/mp4',
        'webm': 'video/webm',
        'mov': 'video/quicktime',
        'mkv': 'video/x-matroska',
        'avi': 'video/x-msvideo',
        'flv': 'video/x-flv',
    }
    return mime_types.get(ext, 'application/octet-stream')


async def _aget_or_generate_preview(filename: str, is_vid: bool, storage: Any, size: int = 300) -> bytes:
    """Async wrapper for preview generation that uses storage async methods when available."""
    cache_path = _get_cache_path(filename)

    try:
        return await async_generate_preview(filename, is_vid, storage, size=size)
    except FileNotFoundError:
        logger.info('File not found: %s', filename)
        raise HTTPException(status_code=404, detail='File not found in storage')
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception('Storage/FFmpeg error for %s: %s', filename, exc)
        raise HTTPException(status_code=503, detail='Storage/FFmpeg error')


def get_settings() -> Settings:
    return load_settings()


@app.get("/health", tags=["health"])
def health() -> Dict[str, Any]:
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse, tags=["ui"])
def index(request: Request):
    """Serve the main meme gallery page."""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/duplicates", response_class=HTMLResponse, tags=["ui"])
def duplicates_page(request: Request):
    """Serve the duplicates UI page."""
    return templates.TemplateResponse("duplicates.html", {"request": request})


@app.get("/memes/{filename}/download", tags=["memes"])
async def download_meme(filename: str):
    """Download raw meme bytes from WebDAV proxy (no API token required for convenience)."""
    try:
        filename = sanitize_filename(filename)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    storage = getattr(app.state, 'app_instance', None) and getattr(app.state.app_instance, 'storage', None)
    if not storage:
        raise HTTPException(status_code=503, detail='Storage is not configured')
    try:
        data = await getattr(storage, 'async_download_file', storage.download_file)(filename)
        if data is None:
            raise HTTPException(status_code=404, detail='File not found in storage')
        
        ext = _get_extension(filename)
        ctype = _get_mime_type(ext)
        return StreamingResponse(BytesIO(data), media_type=ctype)
    except HTTPException:
        raise
    except FileNotFoundError:
        logger.info('File not found: %s', filename)
        raise HTTPException(status_code=404, detail='File not found in storage')
    except IOError as exc:
        logger.exception('Storage error for %s: %s', filename, exc)
        raise HTTPException(status_code=503, detail='Storage error')
    except Exception as exc:
        logger.exception('Failed to download %s: %s', filename, exc)
        raise HTTPException(status_code=500, detail='Download failed')


@app.get("/health", tags=["health"])
def health_check():
    """Health check endpoint."""
    return {"status": "ok", "message": "App is running"}


@app.get("/memes", tags=["memes"])
def list_memes(limit: int = DEFAULT_LIST_LIMIT, offset: int = DEFAULT_OFFSET, status: Optional[str] = None, sort: str = "-created_at"):
    """List memes with optional filtering and sorting (excludes removed)."""
    logger.debug(f"list_memes called: limit={limit}, offset={offset}, status={status}, sort={sort}")
    
    try:
        with session_scope(app.state.engine) as session:
            q = select(Meme).where(Meme.status != 'removed')
            
            if status:
                q = q.where(Meme.status == status)
            
            if sort.startswith("-"):
                from sqlalchemy import desc
                q = q.order_by(desc(getattr(Meme, sort[1:])))
            else:
                q = q.order_by(getattr(Meme, sort))
            
            q = q.limit(limit).offset(offset)
            rows = session.exec(q).all()
            
            logger.debug(f"Query returned {len(rows)} rows")
            
            result = []
            for r in rows:
                meme_dict = r.model_dump()
                meme_dict['processed'] = r.status == 'filled'
                if len(result) == 0:
                    logger.debug(f"First meme keys: {meme_dict.keys()}")
                    logger.debug(f"First meme phash: {meme_dict.get('phash')}")
                result.append(meme_dict)
            
            logger.debug(f"Returning {len(result)} memes")
            return result
    except Exception as e:
        logger.exception("Error in list_memes")
        raise HTTPException(status_code=500, detail=f"List memes failed: {str(e)}")


@app.get("/memes/phash-status", tags=["deduplication"])
def get_phash_status():
    """Get status of perceptual hash initialization.
    
    Returns count of memes with/without phash and success rate.
    """
    try:
        with session_scope(app.state.engine) as session:
            total = session.exec(select(Meme)).all()
            total_count = len(total)
            
            with_hash = session.exec(select(Meme).where(Meme.phash != None)).all()
            with_hash_count = len(with_hash)
            
            without_hash_count = total_count - with_hash_count
            success_rate = (with_hash_count / total_count * 100) if total_count > 0 else 0
            
            return {
                "total_memes": total_count,
                "with_phash": with_hash_count,
                "without_phash": without_hash_count,
                "success_rate": round(success_rate, 1),
                "status": "ok"
            }
    except Exception:
        logger.exception("Failed to get phash status")
        raise HTTPException(status_code=500, detail="Failed to get phash status")


@app.get("/memes/debug/phashes", tags=["debug"])
def debug_phashes():
    """DEBUG: Return all memes with their phashes for debugging."""
    try:
        with session_scope(app.state.engine) as session:
            memes = session.exec(select(Meme)).all()
            result = []
            for meme in memes:
                result.append({
                    "filename": meme.filename,
                    "phash": meme.phash,
                    "is_false_positive": meme.is_false_positive
                })
            return result
    except Exception:
        logger.exception("Failed to get phashes")
        raise HTTPException(status_code=500, detail="Failed to get phashes")


@app.get("/memes/debug/schema", tags=["debug"])
def debug_db_schema():
    """DEBUG: Return list of tables and PRAGMA table_info for any table names containing 'meme'."""
    try:
        engine = app.state.engine
        with engine.connect() as conn:
            tables = [r[0] for r in conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))]
            meme_tables = [t for t in tables if 'meme' in t.lower()]
            schemas = {}
            for t in meme_tables:
                result = conn.execute(text(f"PRAGMA table_info('{t}')"))
                cols = [dict(r) for r in result.mappings().all()]
                schemas[t] = cols
            return {"tables": tables, "meme_tables": meme_tables, "schemas": schemas}
    except Exception:
        logger.exception("Failed to read DB schema")
        raise HTTPException(status_code=500, detail="Failed to read DB schema")


@app.post("/sync", tags=["sync"])
def trigger_sync():
    """Manually trigger a sync job to check for new/removed memes from WebDAV.
    
    Returns dict with added, removed, saved, failed, unfilled, unsupported counts.
    """
    try:
        if not hasattr(app.state, 'app_instance') or app.state.app_instance is None:
            raise HTTPException(status_code=503, detail="Application not fully initialized")
        
        result = app.state.app_instance.sync_and_process()
        return result
    except Exception as e:
        logger.exception("Error during manual sync: %s", e)
        raise HTTPException(status_code=500, detail=f"Sync failed: {str(e)}")


@app.post("/memes/deduplication/analyze", tags=["deduplication"])
def analyze_duplicates():
    """Analyze all memes and find duplicate groups using perceptual hashing.

    Calculates phash for all memes and groups visually similar ones.
    Persists groups in `DuplicateGroup` and membership via `MemeDuplicateGroup`.
    Returns dict with total_groups, total_duplicates, and list of duplicate groups.
    """
    try:
        with session_scope(app.state.engine) as session:
            try:
                old_links = session.exec(select(DBDupeLink)).all()
                for l in old_links:
                    session.delete(l)
                old_groups = session.exec(select(DBDuplicateGroup)).all()
                for g in old_groups:
                    session.delete(g)
                session.commit()
            except Exception:
                logger.debug("No previous duplicate groups to clear or failed to clear")

            duplicate_groups = find_duplicate_groups(session)

            result = []
            total_duplicates = 0

            
            for group in duplicate_groups:
                if not group:
                    continue

                dg = DBDuplicateGroup()
                session.add(dg)
                session.commit()
                session.refresh(dg)

                
                primary_meme = group[0]
                duplicates = group[1:]
                total_duplicates += len(duplicates)

                
                for meme in group:
                    link = DBDupeLink(group_id=dg.id, filename=meme.filename)
                    session.add(link)

                
                primary_info = DuplicateInfo(
                    filename=primary_meme.filename,
                    similarity=0,
                    preview_url=f"/memes/{primary_meme.filename}/preview"
                )

                duplicates_info = []
                for dup_meme in duplicates:
                    if primary_meme.phash and dup_meme.phash:
                        distance = hamming_distance(primary_meme.phash, dup_meme.phash)
                    else:
                        distance = 64

                    dup_info = DuplicateInfo(
                        filename=dup_meme.filename,
                        similarity=distance,
                        preview_url=f"/memes/{dup_meme.filename}/preview"
                    )
                    duplicates_info.append(dup_info)

                result.append(DuplicateGroup(
                    primary=primary_info,
                    duplicates=duplicates_info
                ))

            session.commit()

            logger.info(f"Found {len(result)} duplicate groups with {total_duplicates} duplicates, saved to database")
            
            return {
                "total_groups": len(result),
                "total_duplicates": total_duplicates,
                "groups": result
            }
            
    except Exception:
        logger.exception("Failed to analyze duplicates")
        raise HTTPException(status_code=500, detail="Duplicate analysis failed")


@app.get("/memes/duplicates-by-group", tags=["deduplication"])
def get_duplicates_by_group():
    """Get all memes grouped by duplicate_group_id.
    
    Returns list of duplicate groups with all memes in each group.
    Primary is automatically selected as the file with largest size.
    Only includes groups with duplicate_group_id != None and at least 2 memes.
    """
    try:
        storage = getattr(app.state, 'app_instance', None) and getattr(app.state.app_instance, 'storage', None)
        with session_scope(app.state.engine) as session:
            groups_out = []
            groups = session.exec(select(DBDuplicateGroup)).all()
            for g in groups:
                links = session.exec(select(DBDupeLink).where(DBDupeLink.group_id == g.id)).all()
                filenames = [l.filename for l in links]
                meme_map = {}
                if filenames:
                    rows = session.exec(select(Meme).where(Meme.filename.in_(filenames))).all()
                    meme_map = {m.filename: m for m in rows}

                memes = []
                for l in links:
                    m = meme_map.get(l.filename)
                    file_size = 0
                    if storage:
                        try:
                            try:
                                file_entries = storage.client.ls(l.filename)
                                if file_entries and isinstance(file_entries[0], dict):
                                    entry = file_entries[0]
                                    logger.debug(f"WebDAV entry for {l.filename}: {entry}")
                                    for size_field in ('getcontentlength', 'size'):
                                        if size_field in entry:
                                            try:
                                                file_size = int(entry[size_field])
                                                logger.debug(f"Found {size_field}={file_size} for {l.filename}")
                                                break
                                            except (ValueError, TypeError):
                                                pass
                            except Exception as e:
                                logger.debug(f"WebDAV ls failed for {l.filename}: {e}")
                                pass
                        except Exception:
                            pass
                        
                        if file_size == 0:
                            try:
                                logger.debug(f"Downloading {l.filename} to measure size")
                                data = storage.download_file(l.filename)
                                file_size = len(data) if data else 0
                                logger.debug(f"Downloaded {l.filename}, size={file_size}")
                            except Exception as e:
                                logger.debug(f"Download fallback failed for {l.filename}: {e}")
                                file_size = 0
                    
                    memes.append({
                        "filename": l.filename,
                        "phash": m.phash if m else None,
                        "preview_url": f"/memes/{l.filename}/preview",
                        "size": file_size
                    })
                
                if len(memes) >= 2:
                    primary_meme = max(memes, key=lambda x: x['size']) if memes else memes[0]
                    
                    groups_out.append({
                        "group_id": g.id,
                        "count": len(memes),
                        "primary_filename": primary_meme['filename'],
                        "memes": memes
                    })

            logger.debug(f"Returning {len(groups_out)} duplicate groups")
            return {"total_groups": len(groups_out), "groups": groups_out}
    except Exception:
        logger.exception("Failed to get duplicates by group")
        raise HTTPException(status_code=500, detail="Failed to get duplicates")


@app.get("/memes/search/by-keywords", tags=["memes"])
def search_memes(q: str = "", limit: int = DEFAULT_SEARCH_LIMIT, offset: int = DEFAULT_OFFSET):
    """Full-text search memes using Whoosh.
    
    Searches across: filename, keywords, description, category, text_in_image (OCR)
    Results ordered by relevance (Whoosh score).
    Pagination with limit and offset.
    """
    if not q or len(q) < MIN_SEARCH_QUERY_LENGTH:
        return []
    
    results = whoosh_search(q, limit=limit + offset, offset=0)
    
    paginated_results = results[offset : offset + limit]
    
    return paginated_results


@app.get("/memes/{filename}", tags=["memes"])
def get_meme_detail(filename: str):
    """Get detailed info about a specific meme."""
    if not filename or len(filename) > MAX_FILENAME_LENGTH:
        raise HTTPException(status_code=400, detail="Invalid filename")
    
    with session_scope(app.state.engine) as session:
        m = get_meme_by_filename(session, filename)
        if not m:
            raise HTTPException(status_code=404, detail="Meme not found")
        meme_dict = m.model_dump()
        meme_dict['processed'] = m.status == 'filled'
        return meme_dict


@app.patch("/memes/{filename}", tags=["memes"])
def update_meme(filename: str, request: UpdateMemeRequest):
    """Update meme metadata (category, keywords, description). Only provided fields are updated."""
    try:
        filename = sanitize_filename(filename)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    with session_scope(app.state.engine) as session:
        m = get_meme_by_filename(session, filename)
        if not m:
            raise HTTPException(status_code=404, detail="Meme not found")
        
        if request.category is not None:
            m.category = request.category
        if request.keywords is not None:
            m.keywords = request.keywords
        if request.description is not None:
            m.description = request.description
        
        m.updated_at = datetime.datetime.now(datetime.timezone.utc)
        
        session.add(m)
        session.commit()
        session.refresh(m)
        logger.info("Updated meme %s", filename)
        
        try:
            add_meme_to_index(m)
        except Exception:
            logger.exception("Failed to update search index for %s", filename)
        
        meme_dict = m.model_dump()
        meme_dict['processed'] = m.status == 'filled'
        return meme_dict


@app.delete("/memes/{filename}", tags=["memes"])
async def remove_meme(filename: str):
    """Delete a meme from database and WebDAV storage."""
    try:
        filename = sanitize_filename(filename)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    storage = getattr(app.state, 'app_instance', None) and getattr(app.state.app_instance, 'storage', None)
    
    with session_scope(app.state.engine) as session:
        m = get_meme_by_filename(session, filename)
        if not m:
            raise HTTPException(status_code=404, detail="Meme not found")
    
    if storage:
        try:
            await getattr(storage, 'async_delete_file', storage.delete_file)(filename)
            logger.info("Deleted %s from WebDAV storage", filename)
        except Exception as exc:
            logger.exception("Failed to delete %s from WebDAV: %s", filename, exc)
            raise HTTPException(status_code=500, detail=f"Failed to delete from storage: {exc}")
    
    try:
        with session_scope(app.state.engine) as session:
            m = session.exec(select(Meme).where(Meme.filename == filename)).first()
            if m:
                session.delete(m)
                session.commit()
                logger.info("Deleted %s from database", filename)
    except Exception as exc:
        logger.exception("Failed to delete %s from database: %s", filename, exc)
        raise HTTPException(status_code=500, detail=f"Failed to delete from database: {exc}")
    
    return {"status": "deleted", "filename": filename}


@app.get("/memes/{filename}/preview", tags=["memes"])
async def preview_meme(filename: str, size: int = PREVIEW_SIZE):
    """Get a thumbnail preview of a meme (resized). Supports images and videos (extracts first frame).
    
    For videos, extracts the first frame at 1 second mark and returns as JPEG.
    For images, resizes and returns as JPEG.
    Previews are cached to /data/previews for fast repeated access.
    """
    try:
        filename = sanitize_filename(filename)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    storage = getattr(app.state, 'app_instance', None) and getattr(app.state.app_instance, 'storage', None)
    if not storage:
        raise HTTPException(status_code=503, detail='Storage not configured')
    
    if not (is_image(filename) or is_video(filename)):
        raise HTTPException(status_code=400, detail='File type is not supported for preview')
    
    is_vid = is_video(filename)
    preview_bytes = await _aget_or_generate_preview(filename, is_vid, storage, size)
    ctype = 'image/jpeg'
    logger.debug('Served preview for %s', filename)
    return StreamingResponse(BytesIO(preview_bytes), media_type=ctype)


@app.get("/api/stats", tags=["api"])
def get_stats_endpoint():
    """Get application statistics (excludes 'removed' status memes). Uses single aggregated query."""
    try:
        with session_scope(app.state.engine) as session:
            stats = get_stats(session)
            return stats
    except Exception:
        logger.exception("Failed to get stats")
        raise HTTPException(status_code=500, detail="Stats failed")

@app.get("/memes/{filename}/duplicates", tags=["deduplication"])
def get_meme_duplicates(filename: str):
    """Get duplicate memes for a specific meme.
    
    Returns list of memes that are visually similar to the given meme.
    Includes hamming distance scores.
    """
    try:
        filename = sanitize_filename(filename)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    try:
        with session_scope(app.state.engine) as session:
            primary_meme = get_meme_by_filename(session, filename)
            if not primary_meme:
                raise HTTPException(status_code=404, detail="Meme not found")
            
            if primary_meme.is_false_positive or not primary_meme.phash:
                return {"primary": None, "duplicates": []}
            
            group_ids = get_groups_for_filename(session, filename)
            if not group_ids:
                return {"primary": None, "duplicates": []}

            duplicates_info = []
            seen = set()
            for gid in group_ids:
                members = get_group_members(session, gid)
                for mem_fn in members:
                    if mem_fn == filename or mem_fn in seen:
                        continue
                    seen.add(mem_fn)
                    mem = session.exec(select(Meme).where(Meme.filename == mem_fn)).first()
                    if mem and mem.phash and primary_meme.phash:
                        distance = hamming_distance(primary_meme.phash, mem.phash)
                    else:
                        distance = 64
                    duplicates_info.append(DuplicateInfo(filename=mem_fn, similarity=distance, preview_url=f"/memes/{mem_fn}/preview"))

            return {
                "primary": DuplicateInfo(filename=primary_meme.filename, similarity=0, preview_url=f"/memes/{primary_meme.filename}/preview"),
                "duplicates": duplicates_info,
            }
    except HTTPException:
        raise
    except Exception:
        logger.exception(f"Failed to get duplicates for {filename}")
        raise HTTPException(status_code=500, detail="Failed to get duplicates")

@app.post("/memes/{filename}/recalculate-phash", tags=["deduplication"])
async def recalculate_meme_phash(filename: str):
    """Manually recalculate perceptual hash for a meme.
    
    Useful for memes that failed during initialization.
    Returns details about the calculation attempt.
    """
    try:
        filename = sanitize_filename(filename)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    storage = getattr(app.state, 'app_instance', None) and getattr(app.state.app_instance, 'storage', None)
    if not storage:
        raise HTTPException(status_code=503, detail='Storage not configured')
    
    try:
        with session_scope(app.state.engine) as session:
            meme = get_meme_by_filename(session, filename)
            if not meme:
                raise HTTPException(status_code=404, detail="Meme not found")
            
            try:
                result = await compute_and_persist_phash(filename, storage, app.state.engine, timestamp=1.0)
                if result:
                    logger.info(f"Successfully recalculated phash for {filename}")
                    return {
                        "status": "ok",
                        "message": "Phash calculated successfully",
                        "filename": filename,
                        "phash": result
                    }
                else:
                    return {
                        "status": "error",
                        "message": "Failed to calculate phash from image data or persist it",
                        "filename": filename
                    }
            except Exception as e:
                logger.exception(f"Failed to recalculate phash for {filename}: {e}")
                return {
                    "status": "error",
                    "message": f"Error: {str(e)}",
                    "filename": filename,
                    "error_type": type(e).__name__
                }
    except HTTPException:
        raise
    except Exception:
        logger.exception(f"Failed to recalculate phash for {filename}")
        raise HTTPException(status_code=500, detail="Failed to recalculate phash")


@app.post("/memes/{filename}/mark-not-duplicate", tags=["deduplication"])
def mark_meme_not_duplicate(filename: str):
    """Mark a meme as not a duplicate (false positive).
    
    Prevents the meme from appearing in duplicate groups in future analyses.
    """
    try:
        filename = sanitize_filename(filename)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    try:
        with session_scope(app.state.engine) as session:
            group_ids = get_groups_for_filename(session, filename)
            if not group_ids:
                success = mark_false_positive(session, filename)
                if not success:
                    raise HTTPException(status_code=404, detail="Meme not found")
                meme = session.exec(select(Meme).where(Meme.filename == filename)).first()
                if meme:
                    session.refresh(meme)
                    return {"status": "ok", "message": "Meme marked as not duplicate", "meme": meme.model_dump()}
                return {"status": "ok", "message": "Meme marked as not duplicate"}

            created = []
            for gid in group_ids:
                members = get_group_members(session, gid)
                for mem_fn in members:
                    if mem_fn == filename:
                        continue
                    dup = add_pair_exception(session, filename, mem_fn)
                    created.append({"id": dup.id, "a": dup.filename_a, "b": dup.filename_b, "is_false_positive": dup.is_false_positive})

                    try:
                        from .models import MemeDuplicateGroup as DBDupeLink
                        links = session.exec(select(DBDupeLink).where(DBDupeLink.group_id == gid, DBDupeLink.filename == filename)).all()
                        for l in links:
                            try:
                                session.delete(l)
                            except Exception:
                                logger.debug("Failed to delete meme-group link for %s in group %s", filename, gid)
                        session.commit()
                    except Exception:
                        logger.debug("Failed to remove group links after creating pair exception for %s and %s", filename, mem_fn)

            try:
                from .models import DuplicateGroup as DBDuplicateGroup, MemeDuplicateGroup as DBDupeLink
                for gid in group_ids:
                    try:
                        remaining = session.exec(select(DBDupeLink).where(DBDupeLink.group_id == gid)).all()
                        if len(remaining) <= 1:
                            for r in remaining:
                                try:
                                    session.delete(r)
                                except Exception:
                                    pass
                            try:
                                grp = session.exec(select(DBDuplicateGroup).where(DBDuplicateGroup.id == gid)).first()
                                if grp:
                                    session.delete(grp)
                            except Exception:
                                pass
                    except Exception:
                        logger.debug("Failed to inspect/delete group %s during cleanup", gid)
                session.commit()
            except Exception:
                logger.debug("Failed to cleanup duplicate groups after marking not-duplicate")

            logger.info(f"Created {len(created)} pairwise exceptions for {filename}")
            return {"status": "ok", "message": "Pairwise exceptions created", "created": created}
    except HTTPException:
        raise
    except Exception:
        logger.exception(f"Failed to mark {filename} as not duplicate")
        raise HTTPException(status_code=500, detail="Failed to mark as not duplicate")


@app.post("/memes/merge-duplicates", tags=["deduplication"])
def merge_duplicate_memes(request: MergeDuplicatesRequest):
    """Merge duplicate memes into the primary meme.
    
    Combines metadata (keywords, description) from duplicates into primary.
    Deletes duplicate files from storage and database.
    Primary meme is preserved with merged metadata.
    """
    if not request.primary_filename or not request.duplicate_filenames:
        raise HTTPException(status_code=400, detail="primary_filename and duplicate_filenames are required")
    
    try:
        request.primary_filename = sanitize_filename(request.primary_filename)
        request.duplicate_filenames = [sanitize_filename(f) for f in request.duplicate_filenames]
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    storage = getattr(app.state, 'app_instance', None) and getattr(app.state.app_instance, 'storage', None)
    if not storage:
        raise HTTPException(status_code=503, detail='Storage not configured')
    
    try:
        with session_scope(app.state.engine) as session:
            success = merge_duplicates(
                session,
                storage,
                request.primary_filename,
                request.duplicate_filenames,
                merge_metadata=request.merge_metadata,
                metadata_sources=request.metadata_sources
            )
            
            if not success:
                raise HTTPException(status_code=404, detail="Primary meme or duplicates not found")
            
            logger.info(f"Merged {len(request.duplicate_filenames)} duplicates into {request.primary_filename}")
            return {
                "status": "ok",
                "message": f"Merged {len(request.duplicate_filenames)} duplicates into {request.primary_filename}",
                "primary_filename": request.primary_filename
            }
    except HTTPException:
        raise
    except Exception:
        logger.exception(f"Failed to merge duplicates into {request.primary_filename}")
        raise HTTPException(status_code=500, detail="Failed to merge duplicates")

class PairDTO(BaseModel):
    filename_a: str
    filename_b: str


@app.post("/duplicates/pairs", tags=["deduplication"])
def create_duplicate_pair(pair: PairDTO):
    try:
        a = sanitize_filename(pair.filename_a)
        b = sanitize_filename(pair.filename_b)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        with session_scope(app.state.engine) as session:
            dup = add_pair_exception(session, a, b)
            return {"status": "ok", "pair": {"id": dup.id, "a": dup.filename_a, "b": dup.filename_b, "is_false_positive": dup.is_false_positive}}
    except Exception:
        logger.exception("Failed to create duplicate pair")
        raise HTTPException(status_code=500, detail="Failed to create duplicate pair")


@app.get("/duplicates/pairs", tags=["deduplication"])
def list_duplicate_pairs():
    try:
        with session_scope(app.state.engine) as session:
            rows = list_pair_exceptions(session)
            out = [{"id": r.id, "a": r.filename_a, "b": r.filename_b, "is_false_positive": r.is_false_positive} for r in rows]
            return {"total": len(out), "pairs": out}
    except Exception:
        logger.exception("Failed to list duplicate pairs")
        raise HTTPException(status_code=500, detail="Failed to list duplicate pairs")


@app.delete("/duplicates/pairs", tags=["deduplication"])
def delete_duplicate_pair(pair: PairDTO):
    try:
        a = sanitize_filename(pair.filename_a)
        b = sanitize_filename(pair.filename_b)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        with session_scope(app.state.engine) as session:
            ok = remove_pair_exception(session, a, b)
            if not ok:
                raise HTTPException(status_code=404, detail="Pair not found")
            return {"status": "ok", "deleted": {"a": a, "b": b}}
    except HTTPException:
        raise
    except Exception:
        logger.exception("Failed to delete duplicate pair")
        raise HTTPException(status_code=500, detail="Failed to delete duplicate pair")
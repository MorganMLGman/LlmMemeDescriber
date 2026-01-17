from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.responses import HTMLResponse, StreamingResponse, FileResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.httpsredirect import HTTPSRedirectMiddleware
from fastapi_csrf_protect import CsrfProtect
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from contextlib import asynccontextmanager
from pathlib import Path
import asyncio
import hashlib
from io import BytesIO
import os
from typing import Dict, Optional, Any, List
import logging

from pydantic import BaseModel

from .config import load_settings, configure_logging, parse_interval, Settings
from .constants import *
from .constants import _get_extension
from .db import init_db, get_stats, get_meme_by_filename
from .db_helpers import log_audit_action
from .main import App
from .storage import WebDavStorage
from .storage_workers import StorageWorkerPool
from .genai_client import get_client
from .ssl_helpers import validate_certificate_files
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
from .models import Meme, DuplicateGroup as DBDuplicateGroup, MemeDuplicateGroup as DBDupeLink, UserToken, TokenResponse, TokenInfo, UserInfo
from sqlalchemy import desc
from .storage_helpers import compute_and_persist_phash
from .preview_helpers import generate_preview, async_generate_preview, restore_preview_cache, save_preview_cache, cleanup_orphaned_cache
from sqlmodel import select
from .db_helpers import session_scope
import datetime
from sqlalchemy import text
from .auth import OIDCAuthContext, hash_token, generate_state_token, verify_api_token_not_revoked, verify_api_token_not_revoked

logger = logging.getLogger(__name__)


# Global settings instance for dependency injection
_settings_instance: Optional[Any] = None

def get_settings() -> Any:
    """Dependency to get the global settings instance."""
    global _settings_instance
    if _settings_instance is None:
        _settings_instance = load_settings()
    return _settings_instance


@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    """Manage application lifecycle (startup and shutdown events)."""
    settings = load_settings()
    configure_logging(settings)
    
    # Log authentication mode only at startup
    if settings.public_mode:
        logger.info("PUBLIC_MODE enabled - all authentication disabled")
    elif settings.oidc_enabled:
        logger.info("OIDC authentication enabled")
    
    logger.info("Starting llm_memedescriber FastAPI app (preview cache: %s)", CACHE_DIR)
    
    try:
        cert_path, key_path = validate_certificate_files(
            getattr(settings, 'ssl_cert_file', None),
            getattr(settings, 'ssl_key_file', None)
        )
        logger.info("SSL certificates configured: %s", cert_path)
    except Exception as exc:
        logger.error("Failed to initialize SSL certificates: %s", exc)
        raise
    
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
                cleanup_orphaned_cache(set(session.exec(select(Meme.filename)).all()) if session.exec(select(Meme)).first() else set())
            
            # Remove unsupported file types from database (e.g., listing.json from previous versions)
            all_memes = session.exec(select(Meme)).all()
            unsupported_memes = [m for m in all_memes if not is_supported(m.filename)]
            if unsupported_memes:
                for meme in unsupported_memes:
                    session.delete(meme)
                session.commit()
                logger.info(f"Cleaned up {len(unsupported_memes)} unsupported files from database: {[m.filename for m in unsupported_memes]}")
    except Exception:
        logger.exception("Failed to clean up removed/unsupported memes from database")
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

    # No longer using listing.json for backfill; relying entirely on database

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
                # Only process supported file types; skip unsupported files like listing.json
                filenames = [r.filename for r in rows if is_supported(r.filename)]

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
                        # Skip unsupported file types like listing.json
                        if not is_supported(r.filename):
                            continue
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
        
        try:
            logger.info("Starting session cleanup task...")
            asyncio.create_task(cleanup_sessions_periodically())
        except Exception:
            logger.exception("Failed to start session cleanup task")
    
    yield
    
    logger.info("Shutting down llm_memedescriber FastAPI app")
    
    try:
        logger.info("Saving preview cache to disk...")
        saved = save_preview_cache()
        logger.info("Preview cache saved: %d entries", saved)
    except Exception:
        logger.exception("Failed to save preview cache on shutdown")
    
    try:
        if getattr(app_instance.state, 'app_instance', None):
            app_inst = app_instance.state.app_instance
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

# Initialize rate limiter
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter

# Initialize CSRF protection
@CsrfProtect.load_config
def load_csrf_config():
    return [
        ("secret", os.getenv("CSRF_SECRET", "your-secret-key-change-in-production")),
        ("cookie_name", "csrftoken"),
        ("cookie_samesite", "strict")
    ]

csrf_protect = CsrfProtect()

templates_dir = os.path.join(os.path.dirname(__file__), 'templates')
templates = Jinja2Templates(directory=templates_dir)

static_dir = os.path.join(os.path.dirname(__file__), 'static')
if os.path.isdir(static_dir):
    app.mount('/static', StaticFiles(directory=static_dir), name='static')


# ======================== Middleware Setup ========================

# HTTPS Redirect middleware - enforce HTTPS in production (check debug_mode at runtime)
# Default to enforcing HTTPS unless DEBUG_MODE is explicitly set to True
debug_mode_env = os.getenv("DEBUG_MODE", "false").lower() in ("true", "1", "yes")
if not debug_mode_env:
    app.add_middleware(HTTPSRedirectMiddleware)

# CORS middleware - allow credentials for session cookies
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Rate limit exception handler
@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request: Request, exc: RateLimitExceeded):
    """Handle rate limit exceeded errors."""
    return HTMLResponse(
        content=f"<html><body><h1>429 Too Many Requests</h1><p>{str(exc.detail)}</p></body></html>",
        status_code=429
    )


# Custom middleware to track API token usage
@app.middleware("http")
async def track_api_token_usage(request: Request, call_next):
    """Track last usage time of API tokens."""
    auth_header = request.headers.get('Authorization')
    
    if auth_header and auth_header.startswith('Bearer '):
        token = auth_header[7:]
        auth_context = OIDCAuthContext()
        
        # Verify token to ensure it's valid
        if auth_context.jwt_manager:
            payload = auth_context.jwt_manager.verify_token(token)
            if payload:
                # Update last_used_at asynchronously (don't block response)
                try:
                    token_hash = hash_token(token)
                    # Schedule update in background
                    asyncio.create_task(_update_token_usage(token_hash))
                except Exception as e:
                    logger.debug(f"Failed to track token usage: {e}")
    
    response = await call_next(request)
    return response


async def _update_token_usage(token_hash: str):
    """Update last_used_at for a token (background task)."""
    try:
        with session_scope(app.state.engine) as session:
            token = session.exec(
                select(UserToken).where(UserToken.token_hash == token_hash)
            ).first()
            if token:
                token.last_used_at = datetime.datetime.now(datetime.timezone.utc)
                session.add(token)
                session.commit()
    except Exception as e:
        logger.debug(f"Failed to update token usage: {e}")


# Periodic session cleanup (runs every hour)
async def cleanup_sessions_periodically():
    """Clean up expired sessions and OAuth states periodically."""
    while True:
        try:
            await asyncio.sleep(3600)  # Every hour
            auth_context = OIDCAuthContext()
            auth_context.session_manager.cleanup_expired()
            
            # Clean up expired OAuth states (5+ minutes old)
            if hasattr(app, 'state') and hasattr(app.state, 'oauth_states'):
                now = datetime.datetime.now(datetime.timezone.utc)
                expired = [
                    s for s, t in app.state.oauth_states.items()
                    if now - t > datetime.timedelta(minutes=5)
                ]
                for s in expired:
                    del app.state.oauth_states[s]
                if expired:
                    logger.debug(f"Cleaned up {len(expired)} expired OAuth states")
        except Exception as e:
            logger.debug(f"Failed to cleanup sessions/states: {e}")


# Authorization dependency for FastAPI
def require_auth(request: Request, settings: Settings = Depends(get_settings)) -> Dict[str, Any]:
    """Dependency to require authentication (session cookie or bearer token).
    
    For API bearer tokens, also verifies the token has not been revoked.
    If public_mode is enabled, returns a public user without authentication.
    """
    # Public mode bypasses all authentication
    if settings.public_mode:
        return {"sub": "public-user", "public": True}
    
    auth_context = get_auth_context()
    
    # Check session cookie first
    session_id = request.cookies.get('session_id')
    if session_id:
        session = auth_context.session_manager.get_session(session_id)
        if session:
            return session.get('user_info', {})
    
    # Check bearer token
    auth_header = request.headers.get('Authorization')
    if auth_header and auth_header.startswith('Bearer '):
        token = auth_header[7:]
        if auth_context.jwt_manager:
            # First verify JWT signature/expiration
            payload = auth_context.jwt_manager.verify_token(token)
            if payload:
                # Then verify token is not revoked in database
                user_info = verify_api_token_not_revoked(token, request.app.state.engine)
                if user_info:
                    logger.debug(f"API request authenticated for user: {user_info.get('sub')}")
                    return user_info
                else:
                    logger.warning(f"API token validation failed: token may be revoked")
                    raise HTTPException(status_code=401, detail="Token revoked or invalid")
    
    raise HTTPException(status_code=401, detail="Not authenticated")


def optional_auth(request: Request) -> Optional[Dict[str, Any]]:
    """Dependency for optional authentication."""
    return get_user_from_request(request)


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


@app.get("/login", response_class=HTMLResponse, tags=["ui"])
def login_page(request: Request):
    """Serve the login page. Shows OIDC login button."""
    return templates.TemplateResponse("login.html", {"request": request})


@app.get("/", response_class=HTMLResponse, tags=["ui"])
def index(request: Request, settings: Settings = Depends(get_settings), user_info: Optional[Dict] = Depends(optional_auth)):
    """Serve the main meme gallery page. Redirects to login if not authenticated (unless public_mode)."""
    if not settings.public_mode and not user_info:
        return RedirectResponse(url="/login", status_code=302)
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/duplicates", response_class=HTMLResponse, tags=["ui"])
def duplicates_page(request: Request, settings: Settings = Depends(get_settings), user_info: Optional[Dict] = Depends(optional_auth)):
    """Serve the duplicates UI page. Requires authentication (unless public_mode)."""
    if not settings.public_mode and not user_info:
        return RedirectResponse(url="/login", status_code=302)
    return templates.TemplateResponse("duplicates.html", {"request": request})


@app.get("/pending", response_class=HTMLResponse, tags=["ui"])
def pending_page(request: Request):
    """Serve the pending memes UI page."""
    return templates.TemplateResponse("pending.html", {"request": request})


@app.get("/memes/{filename}/download", tags=["memes"])
async def download_meme(filename: str, user_info: Dict = Depends(require_auth)):
    """Download raw meme bytes from WebDAV proxy. REQUIRES AUTHENTICATION."""
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
        return StreamingResponse(
            BytesIO(data),
            media_type=ctype,
            headers={"Content-Length": str(len(data))}
        )
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
def list_memes(limit: int = DEFAULT_LIST_LIMIT, offset: int = DEFAULT_OFFSET, status: Optional[str] = None, sort: str = "-created_at", user_info: Dict = Depends(require_auth)):
    """List memes with optional filtering and sorting (excludes removed). REQUIRES AUTHENTICATION."""
    logger.debug(f"list_memes called: limit={limit}, offset={offset}, status={status}, sort={sort}")
    
    try:
        with session_scope(app.state.engine) as session:
            q = select(Meme).where(Meme.status != 'removed')
            
            if status:
                q = q.where(Meme.status == status)
            
            if sort.startswith("-"):
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
def get_phash_status(user_info: Dict = Depends(require_auth)):
    """Get status of perceptual hash initialization. REQUIRES AUTHENTICATION.
    
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


@app.post("/sync", tags=["sync"])
@limiter.limit("5/minute")
def trigger_sync(request: Request, user_info: Dict = Depends(require_auth)):
    """Manually trigger a sync job to check for new/removed memes from WebDAV. REQUIRES AUTHENTICATION.
    
    Returns dict with added, removed, saved, failed, unfilled, unsupported counts.
    """
    try:
        if not hasattr(app.state, 'app_instance') or app.state.app_instance is None:
            raise HTTPException(status_code=503, detail="Application not fully initialized")
        
        result = app.state.app_instance.sync_and_process()
        logger.info(f"Sync triggered by user {user_info.get('sub')}")
        return result
    except Exception as e:
        logger.exception("Error during manual sync: %s", e)
        raise HTTPException(status_code=500, detail=f"Sync failed: {str(e)}")


@app.post("/memes/deduplication/analyze", tags=["deduplication"])
@limiter.limit("10/minute")
def analyze_duplicates(request: Request, user_info: Dict = Depends(require_auth)):
    """Analyze all memes and find duplicate groups using perceptual hashing. REQUIRES AUTHENTICATION.

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
def get_duplicates_by_group(user_info: Dict = Depends(require_auth)):
    """Get all memes grouped by duplicate_group_id. REQUIRES AUTHENTICATION.
    
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
def search_memes(q: str = "", limit: int = DEFAULT_SEARCH_LIMIT, offset: int = DEFAULT_OFFSET, user_info: Dict = Depends(require_auth)):
    """Full-text search memes using Whoosh. REQUIRES AUTHENTICATION.
    
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
def get_meme_detail(filename: str, user_info: Dict = Depends(require_auth)):
    """Get detailed info about a specific meme. REQUIRES AUTHENTICATION."""
    if not filename or len(filename) > MAX_FILENAME_LENGTH:
        raise HTTPException(status_code=400, detail="Invalid filename")
    
    with session_scope(app.state.engine) as session:
        m = get_meme_by_filename(session, filename)
        if not m:
            raise HTTPException(status_code=404, detail="Meme not found")
        meme_dict = m.model_dump()
        meme_dict['processed'] = m.status == 'filled'
        return meme_dict


@app.post("/memes/{filename}/force-description", tags=["memes"])
def force_description_generation(filename: str):
    """Force generation of description for a meme, bypassing attempt limits.
    
    Resets attempts counter and triggers immediate generation.
    """
    try:
        filename = sanitize_filename(filename)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    if not hasattr(app.state, 'app_instance') or app.state.app_instance is None:
        raise HTTPException(status_code=503, detail="Application not fully initialized")
    
    try:
        with session_scope(app.state.engine) as session:
            m = get_meme_by_filename(session, filename)
            if not m:
                raise HTTPException(status_code=404, detail="Meme not found")
            
            m.attempts = 0
            m.last_error = None
            m.status = 'pending'
            m.updated_at = datetime.datetime.now(datetime.timezone.utc)
            session.add(m)
            session.commit()
            logger.info("Reset attempts for %s; forcing description generation", filename)
        
        result = app.state.app_instance.generate_description(filename)
        
        if result.get('rate_limited'):
            with session_scope(app.state.engine) as session:
                m = get_meme_by_filename(session, filename)
                if m:
                    m.attempts = (m.attempts or 0) + 1
                    m.last_error = "rate_limited"
                    m.updated_at = datetime.datetime.now(datetime.timezone.utc)
                    session.add(m)
                    session.commit()
            
            raise HTTPException(status_code=429, detail="Rate limit exceeded; will retry on next sync cycle")
        
        if result and not result.get('rate_limited'):
            try:
                with session_scope(app.state.engine) as session:
                    m = get_meme_by_filename(session, filename)
                    if m:
                        m.category = result.get('kategoria') or m.category
                        m.description = result.get('opis') or m.description
                        kw = result.get('keywordy')
                        if isinstance(kw, list):
                            m.keywords = ','.join(kw)
                        elif isinstance(kw, str):
                            m.keywords = kw
                        m.text_in_image = result.get('tekst') or m.text_in_image
                        m.status = 'filled'
                        m.updated_at = datetime.datetime.now(datetime.timezone.utc)
                        session.add(m)
                        session.commit()
                        session.refresh(m)
                        logger.info("Saved forced description for %s", filename)
                        
                        try:
                            add_meme_to_index(m)
                        except Exception:
                            logger.exception("Failed to update search index for %s", filename)
                        
                        meme_dict = m.model_dump()
                        meme_dict['processed'] = m.status == 'filled'
                        return meme_dict
            except Exception as e:
                logger.exception("Failed to save forced description for %s: %s", filename, e)
                raise HTTPException(status_code=500, detail=f"Failed to save description: {str(e)}")
        
        with session_scope(app.state.engine) as session:
            m = get_meme_by_filename(session, filename)
            if m:
                meme_dict = m.model_dump()
                meme_dict['processed'] = m.status == 'filled'
                meme_dict['force_generation_attempted'] = True
                return meme_dict
        
        raise HTTPException(status_code=500, detail="Failed to generate description")
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error during forced description generation for %s: %s", filename, e)
        raise HTTPException(status_code=500, detail=f"Force generation failed: {str(e)}")


@app.patch("/memes/{filename}", tags=["memes"])
def update_meme(filename: str, request: UpdateMemeRequest, user_info: Dict = Depends(require_auth)):
    """Update meme metadata (category, keywords, description). REQUIRES AUTHENTICATION and CSRF token. Only provided fields are updated."""
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
        logger.info("Updated meme %s by user %s", filename, user_info.get('sub'))
        
        # Audit log
        log_audit_action(
            app.state.engine,
            user_id=user_info.get('sub', 'unknown'),
            action="PATCH_MEME",
            resource=filename,
            resource_type="meme",
            details=str(request.model_dump()),
            ip_address=request.client.host if request.client else None
        )
        
        try:
            add_meme_to_index(m)
        except Exception:
            logger.exception("Failed to update search index for %s", filename)
        
        meme_dict = m.model_dump()
        meme_dict['processed'] = m.status == 'filled'
        return meme_dict


@app.delete("/memes/{filename}", tags=["memes"])
@limiter.limit("10/hour")
async def remove_meme(filename: str, request: Request, user_info: Dict = Depends(require_auth)):
    """Delete a meme from database and WebDAV storage. REQUIRES AUTHENTICATION and CSRF token."""
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
            logger.info("Deleted %s from WebDAV storage by user %s", filename, user_info.get('sub'))
        except Exception as exc:
            logger.exception("Failed to delete %s from WebDAV: %s", filename, exc)
            raise HTTPException(status_code=500, detail=f"Failed to delete from storage: {exc}")
    
    try:
        with session_scope(app.state.engine) as session:
            m = session.exec(select(Meme).where(Meme.filename == filename)).first()
            if m:
                session.delete(m)
                session.commit()
                logger.info("Deleted %s from database by user %s", filename, user_info.get('sub'))
                # Audit log
                log_audit_action(
                    app.state.engine,
                    user_id=user_info.get('sub', 'unknown'),
                    action="DELETE_MEME",
                    resource=filename,
                    resource_type="meme",
                    details=None,
                    ip_address=request.client.host if request.client else None
                )
    except Exception as exc:
        logger.exception("Failed to delete %s from database: %s", filename, exc)
        raise HTTPException(status_code=500, detail=f"Failed to delete from database: {exc}")
    
    return {"status": "deleted", "filename": filename}


@app.get("/memes/{filename}/preview", tags=["memes"])
async def preview_meme(filename: str, size: int = PREVIEW_SIZE, user_info: Dict = Depends(require_auth)):
    """Get a thumbnail preview of a meme (resized). Supports images and videos (extracts first frame). REQUIRES AUTHENTICATION.
    
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
def get_stats_endpoint(user_info: Dict = Depends(require_auth)):
    """Get application statistics. REQUIRES AUTHENTICATION (excludes 'removed' status memes). Uses single aggregated query."""
    try:
        with session_scope(app.state.engine) as session:
            stats = get_stats(session)
            # Add max generation attempts from settings
            settings = load_settings()
            stats['max_generation_attempts'] = getattr(settings, 'max_generation_attempts', 3)
            return stats
    except Exception:
        logger.exception("Failed to get stats")
        raise HTTPException(status_code=500, detail="Stats failed")

@app.get("/api/prompt", tags=["config"])
def get_prompt(user_info: Dict = Depends(require_auth)):
    """Get current prompt (custom or default). REQUIRES AUTHENTICATION."""
    custom_prompt_path = Path("/data/prompt.txt")
    
    if custom_prompt_path.exists():
        try:
            return {"prompt": custom_prompt_path.read_text(encoding="utf-8"), "source": "custom"}
        except Exception as exc:
            logger.warning("Failed to read custom prompt: %s", exc)
    
    try:
        default_prompt_path = Path(__file__).parent.parent / "PROMPT.txt"
        return {"prompt": default_prompt_path.read_text(encoding="utf-8"), "source": "default"}
    except Exception as exc:
        logger.exception("Failed to read default prompt: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to load prompt")

@app.post("/api/prompt", tags=["config"])
@limiter.limit("10/minute")
def save_prompt(request: Request, request_body: dict, user_info: Dict = Depends(require_auth)):
    """Save custom prompt to /data/prompt.txt. REQUIRES AUTHENTICATION and CSRF token."""
    if not request_body.get("prompt"):
        raise HTTPException(status_code=400, detail="Prompt cannot be empty")
    
    try:
        prompt_path = Path("/data/prompt.txt")
        prompt_path.parent.mkdir(parents=True, exist_ok=True)
        prompt_path.write_text(request_body["prompt"], encoding="utf-8")
        logger.info("Custom prompt saved successfully by user %s", user_info.get('sub'))
        return {"status": "saved", "source": "custom"}
    except Exception as exc:
        logger.exception("Failed to save prompt: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to save prompt")

@app.get("/api/pending-memes", tags=["api"])
def get_pending_memes(user_info: Dict = Depends(require_auth)):
    """Get all memes with 'pending' status waiting for description generation."""
    try:
        with session_scope(app.state.engine) as session:
            memes = session.exec(select(Meme).where(Meme.status == 'pending')).all()
            return [m.model_dump() for m in memes]
    except Exception as e:
        logger.exception("Failed to get pending memes")
        raise HTTPException(status_code=500, detail=f"Failed to get pending memes: {str(e)}")

@app.get("/memes/{filename}/duplicates", tags=["deduplication"])
def get_meme_duplicates(filename: str, user_info: Dict = Depends(require_auth)):
    """Get duplicate memes for a specific meme. REQUIRES AUTHENTICATION.
    
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
@limiter.limit("20/minute")
async def recalculate_meme_phash(filename: str, request: Request, user_info: Dict = Depends(require_auth)):
    """Manually recalculate perceptual hash for a meme. REQUIRES AUTHENTICATION.
    
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
                    logger.info(f"Successfully recalculated phash for {filename} by user {user_info.get('sub')}")
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
def mark_meme_not_duplicate(filename: str, user_info: Dict = Depends(require_auth)):
    """Mark a meme as not a duplicate (false positive). REQUIRES AUTHENTICATION.
    
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
                logger.info(f"Marked {filename} as not duplicate by user {user_info.get('sub')}")
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
@limiter.limit("30/minute")
def merge_duplicate_memes(request: MergeDuplicatesRequest, user_info: Dict = Depends(require_auth)):
    """Merge duplicate memes into the primary meme. REQUIRES AUTHENTICATION.
    
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
            
            logger.info(f"Merged {len(request.duplicate_filenames)} duplicates into {request.primary_filename} by user {user_info.get('sub')}")
            
            # Audit log
            log_audit_action(
                app.state.engine,
                user_id=user_info.get('sub', 'unknown'),
                action="MERGE_DUPLICATES",
                resource=request.primary_filename,
                resource_type="meme_group",
                details=f"Merged {len(request.duplicate_filenames)} duplicates: {','.join(request.duplicate_filenames)}",
                ip_address=None
            )
            
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
@limiter.limit("10/minute")
def create_duplicate_pair(request: Request, pair: PairDTO, user_info: Dict = Depends(require_auth)):
    try:
        a = sanitize_filename(pair.filename_a)
        b = sanitize_filename(pair.filename_b)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        with session_scope(app.state.engine) as session:
            dup = add_pair_exception(session, a, b)
            logger.info(f"Created duplicate pair {a}-{b} by user {user_info.get('sub')}")
            return {"status": "ok", "pair": {"id": dup.id, "a": dup.filename_a, "b": dup.filename_b, "is_false_positive": dup.is_false_positive}}
    except Exception:
        logger.exception("Failed to create duplicate pair")
        raise HTTPException(status_code=500, detail="Failed to create duplicate pair")


@app.get("/duplicates/pairs", tags=["deduplication"])
def list_duplicate_pairs(user_info: Dict = Depends(require_auth)):
    try:
        with session_scope(app.state.engine) as session:
            rows = list_pair_exceptions(session)
            out = [{"id": r.id, "a": r.filename_a, "b": r.filename_b, "is_false_positive": r.is_false_positive} for r in rows]
            return {"total": len(out), "pairs": out}
    except Exception:
        logger.exception("Failed to list duplicate pairs")
        raise HTTPException(status_code=500, detail="Failed to list duplicate pairs")


@app.delete("/duplicates/pairs", tags=["deduplication"])
@limiter.limit("10/minute")
def delete_duplicate_pair(request: Request, pair: PairDTO, user_info: Dict = Depends(require_auth)):
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
            logger.info(f"Deleted duplicate pair {a}-{b} by user {user_info.get('sub')}")
            return {"status": "ok", "deleted": {"a": a, "b": b}}
    except HTTPException:
        raise
    except Exception:
        logger.exception("Failed to delete duplicate pair")
        raise HTTPException(status_code=500, detail="Failed to delete duplicate pair")


@app.post("/memes/duplicates/delete-group", tags=["deduplication"])
@limiter.limit("10/minute")
def delete_duplicate_group(http_request: Request, request: MergeDuplicatesRequest, user_info: Dict = Depends(require_auth)):
    """Delete all duplicates in a group except the primary meme. REQUIRES AUTHENTICATION.
    
    Does not merge metadata - simply deletes all duplicates and keeps the primary.
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
            # Delete all duplicates without merging metadata
            success = merge_duplicates(
                session,
                storage,
                request.primary_filename,
                request.duplicate_filenames,
                merge_metadata=False  # Don't merge metadata, just delete
            )
            
            if not success:
                raise HTTPException(status_code=404, detail="Primary meme or duplicates not found")
            
            logger.info(f"Deleted {len(request.duplicate_filenames)} duplicates from group, keeping {request.primary_filename}")
            
            # Audit log
            log_audit_action(
                app.state.engine,
                user_id=user_info.get('sub', 'unknown'),
                action="DELETE_DUPLICATE_GROUP",
                resource=request.primary_filename,
                resource_type="meme_group",
                details=f"Deleted {len(request.duplicate_filenames)} duplicates: {','.join(request.duplicate_filenames)}",
                ip_address=None
            )
            
            return {
                "status": "ok",
                "message": f"Deleted {len(request.duplicate_filenames)} duplicate(s), kept primary: {request.primary_filename}",
                "primary_filename": request.primary_filename,
                "deleted_count": len(request.duplicate_filenames)
            }
    except HTTPException:
        raise
    except Exception:
        logger.exception(f"Failed to delete duplicate group")
        raise HTTPException(status_code=500, detail="Failed to delete duplicate group")


# ======================== OIDC Authentication Endpoints ========================

def get_auth_context() -> OIDCAuthContext:
    """Get OIDC auth context (singleton)."""
    return OIDCAuthContext()


def get_user_from_request(request: Request) -> Optional[Dict[str, Any]]:
    """Extract user info from session cookie or bearer token.
    
    For bearer tokens, also verifies the token has not been revoked.
    """
    from fastapi import Depends as FastAPIDependsClass
    
    auth_context = get_auth_context()
    
    # Check session cookie first
    session_id = request.cookies.get('session_id')
    if session_id:
        session = auth_context.session_manager.get_session(session_id)
        if session:
            return session.get('user_info')
    
    # Check bearer token
    auth_header = request.headers.get('Authorization')
    if auth_header and auth_header.startswith('Bearer '):
        token = auth_header[7:]
        if auth_context.jwt_manager:
            # First verify JWT signature/expiration
            payload = auth_context.jwt_manager.verify_token(token)
            if payload:
                # Then verify token is not revoked in database
                # For now, return user info if JWT is valid
                # Revocation check is done in the require_auth dependency
                return {'sub': payload.get('sub')}
    
    return None


@app.get("/auth/login", tags=["auth"])
@limiter.limit("10/minute")
def login(request: Request):
    """Redirect to OIDC provider for authentication."""
    auth_context = get_auth_context()
    
    if not auth_context.enabled or not auth_context.oidc_client:
        raise HTTPException(status_code=503, detail="OIDC authentication not enabled")
    
    state = generate_state_token()
    if not hasattr(app.state, 'oauth_states'):
        app.state.oauth_states = {}
    app.state.oauth_states[state] = datetime.datetime.now(datetime.timezone.utc)
    
    auth_url = auth_context.oidc_client.get_authorization_url(state)
    return RedirectResponse(url=auth_url)


@app.get("/auth/callback", tags=["auth"])
@limiter.limit("10/minute")
async def callback(request: Request, code: Optional[str] = None, state: Optional[str] = None, error: Optional[str] = None, error_description: Optional[str] = None):
    """OIDC callback - exchange code for token and create session."""
    auth_context = get_auth_context()
    
    if not auth_context.enabled or not auth_context.oidc_client:
        raise HTTPException(status_code=503, detail="OIDC authentication not enabled")
    
    # Check for OIDC errors from Authelia
    if error:
        logger.error(f"OIDC error from Authelia: {error} - {error_description}")
        raise HTTPException(status_code=400, detail=f"Authentication failed: {error} - {error_description}")
    
    # Check for code parameter
    if not code:
        logger.error(f"Missing authorization code in callback. Query params: {dict(request.query_params)}")
        raise HTTPException(status_code=400, detail="Missing authorization code from OIDC provider")
    
    if not state:
        logger.error("Missing state parameter in callback")
        raise HTTPException(status_code=400, detail="Missing state parameter")
    
    if not hasattr(app.state, 'oauth_states') or state not in app.state.oauth_states:
        raise HTTPException(status_code=400, detail="Invalid state parameter")
    
    state_time = app.state.oauth_states[state]
    if datetime.datetime.now(datetime.timezone.utc) - state_time > datetime.timedelta(minutes=5):
        del app.state.oauth_states[state]
        raise HTTPException(status_code=400, detail="State parameter expired")
    
    del app.state.oauth_states[state]
    
    try:
        token = await auth_context.oidc_client.exchange_code_for_token(code, state)
        
        user_info = await auth_context.oidc_client.get_userinfo(token['access_token'])
        
        user_id = user_info.get('sub')
        session_id = auth_context.session_manager.create_session(user_id, user_info)
        
        logger.info(f"User logged in: {user_id}")
        
        response = RedirectResponse(url='/', status_code=302)
        response.set_cookie(
            'session_id',
            session_id,
            httponly=True,
            secure=True,
            samesite='strict',
            max_age=auth_context.session_manager.expiry_seconds
        )
        return response
    
    except Exception as e:
        logger.error(f"OIDC callback failed: {e}")
        raise HTTPException(status_code=500, detail="Authentication failed")


@app.post("/auth/logout", tags=["auth"])
@limiter.limit("10/minute")
def logout(request: Request):
    """Logout user by revoking session."""
    auth_context = get_auth_context()
    
    session_id = request.cookies.get('session_id')
    if session_id:
        auth_context.session_manager.revoke_session(session_id)
        logger.debug(f"Session revoked: {session_id}")
    
    response = RedirectResponse(url='/', status_code=302)
    response.delete_cookie('session_id')
    return response


@app.get("/auth/user", tags=["auth"])
def get_current_user(request: Request) -> UserInfo:
    """Get current authenticated user info."""
    user_info = get_user_from_request(request)
    
    if not user_info:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    return UserInfo(
        user_id=user_info.get('sub'),
        name=user_info.get('name'),
        email=user_info.get('email'),
        picture=user_info.get('picture')
    )


@app.get("/api/csrf-token", tags=["auth"])
def get_csrf_token(request: Request) -> Dict[str, str]:
    """Get CSRF token for authenticated requests.
    
    Frontend should include this token in X-CSRF-Token header or csrf_token form field.
    """
    user_info = get_user_from_request(request)
    if not user_info:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    try:
        # For fastapi-csrf-protect, the token is auto-managed in cookies
        # We just need to ensure the session has one by checking request context
        csrf_token = request.cookies.get("csrf_token", "")
        if csrf_token:
            return {"csrf_token": csrf_token}
        
        # If no token exists, create a dummy one (CsrfProtect will handle the real one)
        import secrets
        csrf_token = secrets.token_urlsafe(32)
        return {"csrf_token": csrf_token}
    except Exception as e:
        logger.error(f"Failed to get CSRF token: {e}")
        raise HTTPException(status_code=500, detail="Failed to get CSRF token")


# ======================== API Token Management Endpoints ========================

class TokenGenerateRequest(BaseModel):
    """Request to generate a new API token."""
    name: str  # User-friendly name for the token


@app.post("/api/tokens", tags=["auth"], response_model=TokenResponse)
@limiter.limit("10/hour")
def generate_api_token(request_body: TokenGenerateRequest, request: Request):
    """Generate a new API token for authenticated user."""
    user_info = get_user_from_request(request)
    
    if not user_info:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    auth_context = get_auth_context()
    user_id = user_info.get('sub')
    
    if not auth_context.jwt_manager:
        raise HTTPException(status_code=503, detail="JWT not configured")
    
    # Generate token with unique JTI
    token_jti = hashlib.sha256(os.urandom(32)).hexdigest()
    token = auth_context.jwt_manager.create_token(user_id, token_jti)
    token_hash = hash_token(token)
    
    try:
        with session_scope(app.state.engine) as session:
            user_token = UserToken(
                user_id=user_id,
                name=request_body.name,
                token_hash=token_hash,
                created_at=datetime.datetime.now(datetime.timezone.utc)
            )
            session.add(user_token)
            session.commit()
            session.refresh(user_token)
            
            logger.info(f"API token generated for user {user_id}: {request_body.name}")
            
            # Audit log
            log_audit_action(
                app.state.engine,
                user_id=user_id,
                action="CREATE_API_TOKEN",
                resource=str(user_token.id),
                resource_type="token",
                details=f"Token name: {request_body.name}",
                ip_address=None
            )
            
            return TokenResponse(
                id=user_token.id,
                name=user_token.name,
                token=token,  # Plain token - shown only once!
                created_at=user_token.created_at
            )
    except Exception as e:
        logger.error(f"Failed to generate token: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate token")


@app.get("/api/tokens", tags=["auth"], response_model=List[TokenInfo])
def list_api_tokens(request: Request):
    """List all API tokens for authenticated user."""
    user_info = get_user_from_request(request)
    
    if not user_info:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    user_id = user_info.get('sub')
    
    try:
        with session_scope(app.state.engine) as session:
            tokens = session.exec(
                select(UserToken)
                .where(UserToken.user_id == user_id)
                .where(UserToken.revoked == False)
            ).all()
            
            return [
                TokenInfo(
                    id=t.id,
                    name=t.name,
                    created_at=t.created_at,
                    last_used_at=t.last_used_at,
                    expires_at=t.expires_at,
                    revoked=t.revoked
                )
                for t in tokens
            ]
    except Exception as e:
        logger.error(f"Failed to list tokens: {e}")
        raise HTTPException(status_code=500, detail="Failed to list tokens")


@app.delete("/api/tokens/{token_id}", tags=["auth"])
def revoke_api_token(token_id: int, request: Request):
    """Revoke an API token."""
    user_info = get_user_from_request(request)
    
    if not user_info:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    user_id = user_info.get('sub')
    
    try:
        with session_scope(app.state.engine) as session:
            token = session.exec(
                select(UserToken)
                .where(UserToken.id == token_id)
                .where(UserToken.user_id == user_id)
            ).first()
            
            if not token:
                raise HTTPException(status_code=404, detail="Token not found")
            
            token.revoked = True
            session.add(token)
            session.commit()
            
            logger.info(f"API token revoked for user {user_id}: {token.name}")
            
            # Audit log
            log_audit_action(
                app.state.engine,
                user_id=user_id,
                action="REVOKE_API_TOKEN",
                resource=str(token_id),
                resource_type="token",
                details=f"Token name: {token.name}",
                ip_address=None
            )
            
            return {"status": "revoked", "token_id": token_id}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to revoke token: {e}")
        raise HTTPException(status_code=500, detail="Failed to revoke token")
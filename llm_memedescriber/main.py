import asyncio
import datetime
import email.utils
import json
import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

from google.genai import types
from sqlmodel import select
from .db_helpers import session_scope

from .config import parse_interval, load_settings, configure_logging
from .constants import *
from .models import Meme, DuplicateGroup as DBDuplicateGroup, MemeDuplicateGroup as DBDupeLink
from .deduplication import find_duplicate_groups, calculate_phash
from .storage import WebDavStorage
from .storage_workers import StorageWorkerPool
from .storage_helpers import compute_and_persist_phash
from .preview_helpers import cleanup_orphaned_cache
from .genai_client import get_client
from .db import init_db

logger = logging.getLogger(__name__)


def _load_prompt() -> str:
    try:
        with open('/app/PROMPT.txt', 'r', encoding='utf-8') as f:
            return f.read().strip()
    except FileNotFoundError:
        raise FileNotFoundError("PROMPT.txt file not found in /app/ directory")

PROMPT = _load_prompt()


def main():
    settings = load_settings()
    configure_logging(settings)
    global logger
    logger = logging.getLogger(__name__)

    logger.info("Settings loaded")

    base_url = settings.webdav_url.rstrip('/')
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
    
    engine = init_db()
    
    app = App(settings, storage, engine=engine)
    app.run()


class App:
    """Encapsulates app state, storage adapter and the worker loop.

    start() will start worker (non-blocking). Use run() to start and block until stopped.
    """
    def __init__(self, settings, storage: Any, genai_client = None, engine=None, interval_seconds: int = 60):
        self.settings = settings
        self.storage = storage
        
        if genai_client is not None:
            self.genai_client = genai_client
        else:
            try:
                self.genai_client = get_client(getattr(settings, 'google_genai_api_key', None))
            except Exception:
                self.genai_client = None
        self.engine = engine
        self.interval_seconds = interval_seconds
        self.stop_event = threading.Event()
        self.worker_thread: Optional[threading.Thread] = None
        self.needs_description: List[str] = []
        self._needs_description_lock = threading.Lock()
        self._shutdown_done: bool = False
        self._sync_lock = threading.Lock()
        self._sync_in_progress: bool = False

        try:
            self.export_on_shutdown: bool = bool(getattr(settings, 'export_listing_on_shutdown', True))
            ei = getattr(settings, 'export_listing_interval', None)
            self.export_interval_seconds: Optional[int] = None
            if ei:
                try:
                    self.export_interval_seconds = parse_interval(str(ei))
                except Exception:
                    self.export_interval_seconds = None
            self._last_export_at: float = 0.0
        except Exception:
            self.export_on_shutdown = True
            self.export_interval_seconds = None
            self._last_export_at = 0.0

    def start(self):
        """Start the worker thread (non-blocking)."""
        if self.worker_thread and self.worker_thread.is_alive():
            logger.debug("Worker already running")
            return
        logger.info("App starting worker thread (interval=%s)", self.interval_seconds)
        self.worker_thread = threading.Thread(target=self._worker, daemon=True, name="SyncWorker")
        self.worker_thread.start()

    def run(self):
        """Start and block until stopped; intended for CLI/foreground use."""
        self.start()
        try:
            self.stop_event.wait()
        finally:
            logger.info("App shutdown requested")
            self.stop()

    def stop(self):
        if self._shutdown_done:
            logger.debug("stop() already called; skipping")
            return
        logger.info("Stopping App worker")
        self._shutdown_done = True
        self.stop_event.set()
        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=10)
            if self.worker_thread.is_alive():
                logger.warning("Worker thread did not exit within timeout; it may still be processing ongoing operations")
        try:
            if self.storage:
                logger.info("Exporting listing.json on graceful shutdown")
                self.export_listing_to_webdav()
        except Exception:
            logger.exception("Failed to export listing during shutdown")

    def _worker(self):
        logger.info("Worker started")
        while not self.stop_event.is_set():
            try:
                summary = self.sync_and_process()
                logger.debug("Sync: added=%d, removed=%d, unfilled=%d", summary['added'], summary['removed'], summary['unfilled'])
                if summary.get('saved') or summary.get('failed'):
                    logger.info("Generated: saved=%d, failed=%d, unsupported=%d", summary.get('saved'), summary.get('failed'), summary.get('unsupported'))
                
                if self.export_interval_seconds:
                    now = time.time()
                    if (now - self._last_export_at) >= self.export_interval_seconds:
                        self.export_listing_to_webdav()
                        self._last_export_at = now
            except Exception:
                logger.exception("Worker error")
            if self.stop_event.wait(self.interval_seconds):
                break

    def export_listing_to_webdav(self) -> bool:
        """Export current DB contents into a listing.json on WebDAV.

        Returns True on success, False otherwise.
        """
        try:
            mapping: Dict[str, Dict] = {}
            with session_scope(self.engine) as session:
                
                rows = session.exec(select(Meme).where(Meme.status != 'removed')).all()
                for m in rows:
                    key = m.filename
                    if m.description or m.category or m.keywords or m.text_in_image:
                        obj: Dict[str, Any] = {}
                        if m.category:
                            obj['kategoria'] = m.category
                        if m.description:
                            obj['opis'] = m.description
                        if m.keywords:
                            obj['keywordy'] = m.keywords
                        if m.text_in_image:
                            obj['tekst'] = m.text_in_image
                        try:
                            if getattr(m, 'phash', None):
                                obj['phash'] = m.phash
                        except Exception:
                            pass
                        try:
                            if getattr(m, 'created_at', None):
                                obj['created_at'] = m.created_at.isoformat()
                        except Exception:
                            pass
                        mapping[key] = obj
                    else:
                        entry: Dict[str, Any] = {}
                        try:
                            if getattr(m, 'phash', None):
                                entry['phash'] = m.phash
                        except Exception:
                            pass
                        mapping[key] = entry
            
            self.storage.write_listing('.', mapping, json_filename='listing.json')
            logger.info("Exported listing.json with %s entries", len(mapping))
            return True
        except Exception:
            logger.exception("Failed to export listing to WebDAV")
            return False

    def _db_operation_with_retry(self, operation, max_retries: int = MAX_DB_RETRY_ATTEMPTS, initial_backoff: float = INITIAL_DB_BACKOFF) -> bool:
        """Execute a DB operation with exponential backoff retry for SQLite locked errors.
        
        operation: callable that performs DB operation, should raise Exception on failure
        max_retries: maximum number of retry attempts (including initial)
        initial_backoff: initial backoff in seconds
        
        Returns True if successful, False otherwise.
        """
        last_exc = None
        for attempt in range(max_retries):
            try:
                operation()
                return True
            except Exception as exc:
                last_exc = exc
                
                exc_str = str(exc).lower()
                if 'locked' in exc_str or 'database is locked' in exc_str:
                    if attempt < max_retries - 1:
                        backoff = initial_backoff * (2 ** attempt)
                        logger.debug("DB locked on attempt %d; retrying after %.2fs", attempt + 1, backoff)
                        time.sleep(backoff)
                        continue
                
                logger.exception("DB operation failed (non-locked error): %s", exc)
                return False
        
        logger.exception("DB operation failed after %d attempts: %s", max_retries, last_exc)
        return False

    def _process_single_meme(self, name: str) -> Dict[str, Any]:
        """Process a single meme: generate description and save to DB only (not WebDAV listing).
        
        WebDAV listing is written once at end of sync_and_process_impl to avoid concurrent writes.
        Returns dict with 'saved', 'unsupported', or 'failed' keys, and 'desc' with description.
        """
        try:
            desc = self.generate_description(name)
            if desc:
                
                def save_to_db():
                    with session_scope(self.engine) as session:
                        m = session.exec(select(Meme).where(Meme.filename == name)).first()
                        if not m:
                            m = Meme(filename=name)
                        m.category = desc.get('kategoria') or m.category
                        m.description = desc.get('opis') or m.description
                        kw = desc.get('keywordy')
                        if isinstance(kw, list):
                            m.keywords = ','.join(kw)
                        elif isinstance(kw, str):
                            m.keywords = kw
                        m.text_in_image = desc.get('tekst') or m.text_in_image
                        m.status = 'filled'
                        m.updated_at = datetime.datetime.now(datetime.timezone.utc)
                        session.add(m)
                        session.commit()
                
                if not self._db_operation_with_retry(save_to_db, max_retries=3):
                    logger.error("Failed to save description to DB for %s after retries", name)
                    return {'failed': True}
                
                with self._needs_description_lock:
                    self.needs_description = [k for k in self.needs_description if k != name]
                logger.debug("Successfully processed %s", name)
                return {'saved': True, 'desc': desc, 'name': name}
            else:
                return {'failed': True}
        except Exception as exc:
            logger.exception("Failed to process meme %s: %s", name, exc)
            return {'failed': True}

    def generate_description(self, filename: str) -> Dict[str, Any]:
        """Generate a description for `filename` using the instance genai client and webdav client.
        
        Returns dict with description if successful, empty dict otherwise.
        Updates DB with error info and increments attempts counter.
        """
        error_info = ""
        try:
            file_bytes = self.storage.download_file(filename)
        except Exception as exc:
            error_info = str(exc)
            logger.error("Error reading file %s from WebDAV: %s", filename, exc)
            
            try:
                with session_scope(self.engine) as session:
                    m = session.exec(select(Meme).where(Meme.filename == filename)).first()
                    if m:
                        m.attempts = (m.attempts or 0) + 1
                        m.last_attempt_at = datetime.datetime.now(datetime.timezone.utc)
                        m.last_error = error_info
                        session.add(m)
                        session.commit()
            except Exception:
                pass
            return {}

        mime_type, media_res = self._detect_media(filename)

        if not self.genai_client:
            logger.warning("GenAI client is not configured; skipping generation for %s", filename)
            return {}
        try:
            part = types.Part.from_bytes(data=file_bytes, mime_type=mime_type, media_resolution=media_res)
            response = self.genai_client.models.generate_content(
                model=self.settings.google_genai_model,
                contents=[part, PROMPT],
                config=types.GenerateContentConfig(
                    safety_settings=[
                        types.SafetySetting(
                            category=types.HarmCategory.HARM_CATEGORY_HARASSMENT,
                            threshold=types.HarmBlockThreshold.BLOCK_NONE,
                        ),
                        types.SafetySetting(
                            category=types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
                            threshold=types.HarmBlockThreshold.BLOCK_NONE,
                        ),
                        types.SafetySetting(
                            category=types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
                            threshold=types.HarmBlockThreshold.BLOCK_NONE,
                        ),
                        types.SafetySetting(
                            category=types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
                            threshold=types.HarmBlockThreshold.BLOCK_NONE,
                        ),
                    ]
                )
            )
            
        except Exception as exc:
            error_info = str(exc)
            logger.error("GenAI request failed for %s: %s", filename, exc)
            
            is_unsupported = 'Unsupported MIME type' in error_info
            
            try:
                with session_scope(self.engine) as session:
                    m = session.exec(select(Meme).where(Meme.filename == filename)).first()
                    if m:
                        m.attempts = (m.attempts or 0) + 1
                        m.last_attempt_at = datetime.datetime.now(datetime.timezone.utc)
                        m.last_error = error_info
                        if is_unsupported:
                            m.status = 'unsupported'
                            logger.info("Marked %s as unsupported MIME type; will not retry", filename)
                        session.add(m)
                        session.commit()
            except Exception:
                pass
            return {}

        
        for txt in self._text_candidates_from_response(response):
            if not txt:
                continue
            parsed = self._extract_json_from_text(txt)
            if parsed is not None:
                logger.debug("Generated JSON for %s", filename)
                
                try:
                    with session_scope(self.engine) as session:
                        m = session.exec(select(Meme).where(Meme.filename == filename)).first()
                        if m:
                            m.attempts = (m.attempts or 0) + 1
                            m.last_attempt_at = datetime.datetime.now(datetime.timezone.utc)
                            m.last_error = None
                            session.add(m)
                            session.commit()
                except Exception:
                    pass
                return parsed

        logger.warning("Failed to extract JSON description for %s", filename)
        
        try:
            with session_scope(self.engine) as session:
                m = session.exec(select(Meme).where(Meme.filename == filename)).first()
                if m:
                    m.attempts = (m.attempts or 0) + 1
                    m.last_attempt_at = datetime.datetime.now(datetime.timezone.utc)
                    m.last_error = "no_json_extracted"
                    session.add(m)
                    session.commit()
        except Exception:
            pass
        return {}
    


    def sync_and_process(self) -> Dict[str, int]:
        """Run a single sync and generate descriptions for unfilled files using instance clients."""
        
        if not self._sync_lock.acquire(blocking=False):
            logger.warning("Previous sync job still in progress; skipping this cycle")
            return {
                'added': 0, 'removed': 0, 'unfilled': 0,
                'saved': 0, 'failed': 0, 'unsupported': 0
            }
        
        try:
            return self._sync_and_process_impl()
        finally:
            self._sync_lock.release()

    def _sync_and_process_impl(self) -> Dict[str, int]:
        """Implementation of sync and process (called with lock held)."""
        
        existing = self.storage.load_listing('/', json_filename='listing.json')
        existing.pop('listing.json', None)

        entries = self.storage.list_files('/', recursive=False)
        server_names = {e['name'] for e in entries if not e['is_dir'] and e['name'] != 'listing.json'}
        server_names_to_process = server_names

        existing_basename_map = {k: str(k).rstrip('/').split('/')[-1] for k in existing.keys()}
        existing_basenames = set(existing_basename_map.values())

        to_add = sorted(list(server_names - existing_basenames))
        to_remove = [k for k, base in existing_basename_map.items() if base not in server_names]

        changed = False
        for k in to_remove:
            existing.pop(k, None)
            changed = True
        for name in to_add:
            existing[name] = {}
            changed = True

        updated_path = None
        if changed:
            updated_path = self.storage.write_listing('/', existing, json_filename='listing.json')

        logger.debug("Sync summary: server_count=%d, listing_count=%d, to_add=%d, to_remove=%d, changed=%s, max_sync_records=%s",
                     len(server_names), len(existing), len(to_add), len(to_remove), changed, getattr(self.settings, 'sync_max_records', None))

        unfilled = []
        try:
            with session_scope(self.engine) as session:
                filenames_to_check = [k for k, v in existing.items() if not v]
                if filenames_to_check:
                    memes = session.exec(select(Meme).where(Meme.filename.in_(filenames_to_check))).all()
                    meme_map = {m.filename: m for m in memes}
                    for k in filenames_to_check:
                        m = meme_map.get(k)
                        if not m or m.status != 'filled':
                            unfilled.append(k)
                else:
                    unfilled = []
        except Exception:
            logger.exception("Failed to check DB status for unfilled detection; using listing.json only")
            unfilled = [k for k, v in existing.items() if not v]

        with self._needs_description_lock:
            self.needs_description = unfilled

        
        max_records = getattr(self.settings, 'sync_max_records', DEFAULT_SYNC_MAX_RECORDS)
        try:
            if max_records is not None:
                max_records = int(max_records)
                if max_records > 0:
                    if len(unfilled) > max_records:
                        logger.debug("Limiting unfilled processing from %d to %d due to sync_max_records", len(unfilled), max_records)
                        unfilled = unfilled[:max_records]
                    
                    if len(server_names) > max_records:
                        server_names_to_process = set(list(server_names)[:max_records])
                        logger.debug("Limiting server_names processed for additions to %d due to sync_max_records", max_records)
        except Exception:
            logger.debug("Invalid sync_max_records setting: %s", max_records)

        try:
            entry_map = {e['name']: e for e in entries if not e.get('is_dir')}
            with session_scope(self.engine) as session:
                names_to_check = list(server_names.union(set(to_remove)))
                existing_map = {}
                if names_to_check:
                    existing_mems = session.exec(select(Meme).where(Meme.filename.in_(names_to_check))).all()
                    existing_map = {m.filename: m for m in existing_mems}

                for name in server_names_to_process:
                    if name not in existing_map:
                        source_url = self.settings.webdav_url.rstrip('/') + '/' + self.settings.webdav_path.lstrip('/') + '/' + name
                        status = 'filled' if existing.get(name) else 'pending'
                        m = Meme(filename=name, source_url=source_url, status=status)
                        try:
                            entry = entry_map.get(name)
                            if entry:
                                date_str = entry.get('getlastmodified') or entry.get('modified') or entry.get('creationdate') or entry.get('getcreationdate')
                                if date_str:
                                    if isinstance(date_str, datetime.datetime):
                                        m.created_at = date_str
                                    else:
                                        try:
                                            dt = email.utils.parsedate_to_datetime(date_str)
                                            m.created_at = dt
                                        except Exception:
                                            try:
                                                m.created_at = datetime.datetime.fromisoformat(date_str)
                                            except Exception:
                                                pass
                        except Exception:
                            pass
                        session.add(m)
                
                for name in to_remove:
                    existing_m = existing_map.get(name)
                    if existing_m:
                        existing_m.status = 'removed'
                        session.add(existing_m)
                session.commit()
        except Exception:
            logger.exception("Failed to persist listing changes to DB")

        saved_count = 0
        failed_count = 0
        unsupported_count = 0
        processed_descriptions = {}

        batch_size = BATCH_PROCESS_WORKERS
        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            futures = {}
            
            
            unsupported_set = set()
            try:
                with session_scope(self.engine) as session:
                    if unfilled:
                        rows = session.exec(select(Meme).where((Meme.filename.in_(unfilled)) & (Meme.status == 'unsupported'))).all()
                        unsupported_set = {r.filename for r in rows}
            except Exception:
                unsupported_set = set()

            for name in unfilled:
                if self.stop_event.is_set():
                    logger.info("Stop requested; aborting generation loop")
                    break
                if name in unsupported_set:
                    logger.debug("Skipping %s: marked as unsupported MIME type", name)
                    unsupported_count += 1
                    continue
                
                if self.stop_event.is_set():
                    logger.info("Stop requested before generating %s; skipping", name)
                    break
                
                future = executor.submit(self._process_single_meme, name)
                futures[future] = name
            
            for future in futures:
                name = futures[future]
                try:
                    result = future.result()
                    if result.get('saved'):
                        saved_count += 1
                        if result.get('desc') and result.get('name'):
                            processed_descriptions[result['name']] = result['desc']
                    elif result.get('unsupported'):
                        unsupported_count += 1
                    else:
                        failed_count += 1
                except Exception as exc:
                    logger.exception("Exception in batch processing for %s: %s", name, exc)
                    failed_count += 1
        
        if processed_descriptions:
            try:
                mapping = self.storage.load_listing('/', json_filename='listing.json')
                mapping.update(processed_descriptions)
                self.storage.write_listing('/', mapping, json_filename='listing.json')
                logger.debug("Wrote %d descriptions to listing.json", len(processed_descriptions))
            except Exception as exc:
                logger.exception("Failed to write descriptions to listing.json: %s", exc)

        if to_add:
            logger.info("Scheduling phash calculation for %d newly added memes", len(to_add))
            # Note: Phashes will be calculated on-demand via /memes/deduplication/analyze API endpoint
            # Cannot use asyncio.run() here as we're already in a running event loop during shutdown

        try:
            with session_scope(self.engine) as session:
                try:
                    old_links = session.exec(select(DBDupeLink)).all()
                    for l in old_links:
                        session.delete(l)
                    old_groups = session.exec(select(DBDuplicateGroup)).all()
                    for g in old_groups:
                        session.delete(g)
                    session.commit()
                except Exception:
                    logger.debug("No previous duplicate groups to clear or failed to clear (during sync)")

                duplicate_groups = find_duplicate_groups(session)
                for group in duplicate_groups:
                    if not group:
                        continue
                    dg = DBDuplicateGroup()
                    session.add(dg)
                    session.commit()
                    session.refresh(dg)
                    for meme in group:
                        link = DBDupeLink(group_id=dg.id, filename=meme.filename)
                        session.add(link)
                session.commit()
            logger.debug("Deduplication analysis completed after sync: %d groups persisted", len(duplicate_groups))
        except Exception:
            logger.exception("Failed to run deduplication analysis after sync_and_process")

        try:
            with session_scope(self.engine) as session:
                valid_filenames = set(session.exec(select(Meme.filename)).all())
                removed_count = cleanup_orphaned_cache(valid_filenames)
                if removed_count > 0:
                    logger.info("Cleaned up %d orphaned cache files after sync", removed_count)
        except Exception:
            logger.exception("Failed to cleanup orphaned cache after sync_and_process")

        result = {
            'added': len(to_add),
            'removed': len(to_remove),
            'saved': saved_count,
            'failed': failed_count,
            'unsupported': unsupported_count,
            'unfilled': len(unfilled),
            'updated': bool(updated_path),
        }
        
        if result['added'] > 0:
            logger.info("Sync job completed: %d memes added", result['added'])
        
        return result

    def import_listing_into_db(self) -> Dict[str, int]:
        """Import listing.json from WebDAV into DB."""
        try:
            mapping = self.storage.load_listing('/', json_filename='listing.json')
            mapping.pop('listing.json', None)
        except Exception as exc:
            logger.warning("Failed to load listing.json: %s", exc)
            return {"created": 0, "updated": 0, "skipped": 0}

        created = 0
        updated = 0
        try:
            try:
                entries = self.storage.list_files('/', recursive=False)
                entry_map = {e['name']: e for e in entries if not e.get('is_dir')}
            except Exception:
                entry_map = {}

            with session_scope(self.engine) as session:
                names = list(mapping.keys())
                existing_mems = {}
                if names:
                    rows = session.exec(select(Meme).where(Meme.filename.in_(names))).all()
                    existing_mems = {r.filename: r for r in rows}

                for name, value in mapping.items():
                    m = existing_mems.get(name)
                    if m:
                        if value and not m.description:
                            m.category = value.get('kategoria')
                            m.description = value.get('opis')
                            kw = value.get('keywordy')
                            if isinstance(kw, list):
                                m.keywords = ','.join(kw)
                            elif isinstance(kw, str):
                                m.keywords = kw
                            m.text_in_image = value.get('tekst')
                            m.status = 'filled'
                            updated += 1
                    else:
                        source_url = self.settings.webdav_url.rstrip('/') + '/' + self.settings.webdav_path.lstrip('/') + '/' + name
                        m = Meme(filename=name, source_url=source_url, status='filled' if value else 'pending')

                        created_dt = None
                        try:
                            if value:
                                created_str = value.get('created_at')
                                if created_str:
                                    try:
                                        created_dt = datetime.datetime.fromisoformat(created_str)
                                    except Exception:
                                        try:
                                            created_dt = email.utils.parsedate_to_datetime(created_str)
                                        except Exception:
                                            created_dt = None

                            if not created_dt:
                                entry = entry_map.get(name)
                                if entry:
                                        date_str = entry.get('getlastmodified') or entry.get('modified') or entry.get('creationdate') or entry.get('getcreationdate')
                                        if date_str:
                                            if isinstance(date_str, datetime.datetime):
                                                created_dt = date_str
                                            else:
                                                try:
                                                    created_dt = email.utils.parsedate_to_datetime(date_str)
                                                except Exception:
                                                    try:
                                                        created_dt = datetime.datetime.fromisoformat(date_str)
                                                    except Exception:
                                                        created_dt = None
                        except Exception:
                            created_dt = None

                        if created_dt:
                            m.created_at = created_dt
                        else:
                            m.created_at = datetime.datetime.now(datetime.timezone.utc)

                        if value:
                            m.category = value.get('kategoria')
                            m.description = value.get('opis')
                            kw = value.get('keywordy')
                            if isinstance(kw, list):
                                m.keywords = ','.join(kw)
                            elif isinstance(kw, str):
                                m.keywords = kw
                            m.text_in_image = value.get('tekst')
                        
                        try:
                            ph = None
                            if value:
                                ph = value.get('phash')
                            if ph:
                                m.phash = ph
                            else:
                                try:
                                    file_bytes = None
                                    try:
                                        file_bytes = self.storage.download_file(name)
                                    except Exception as e:
                                        logger.debug("Could not download %s to compute phash: %s", name, e)
                                        file_bytes = None
                                    if file_bytes:
                                        computed = calculate_phash(file_bytes)
                                        if computed:
                                            m.phash = computed
                                except Exception as e:
                                    logger.debug("Failed to compute phash for %s during import: %s", name, e)
                        except Exception:
                            pass
                        session.add(m)
                        created += 1
                session.commit()
        except Exception:
            logger.exception("Failed to import listing into DB")
        return {"created": created, "updated": updated, "skipped": 0}

    @staticmethod
    def _detect_media(filename: str) -> Tuple[str, types.MediaResolution]:
        ext = str(filename).lower().split('.')[-1] if '.' in filename else ''
        if ext in IMAGE_EXTENSIONS:
            mime_type = "image/jpeg" if ext in {"jpg", "jpeg"} else f"image/{ext}"
            return mime_type, types.MediaResolution.MEDIA_RESOLUTION_HIGH
        if ext in VIDEO_EXTENSIONS:
            mime_type = "video/mp4" if ext == "mp4" else f"video/{ext}"
            return mime_type, types.MediaResolution.MEDIA_RESOLUTION_MEDIUM
        return "application/octet-stream", types.MediaResolution.MEDIA_RESOLUTION_HIGH

    @staticmethod
    def _extract_json_from_text(text: str) -> Optional[Dict[str, Any]]:
        m = re.search(r"```json\s*(\{.*?\})\s*```", text, flags=re.DOTALL | re.IGNORECASE)
        if not m:
            m = re.search(r"```\s*(\{.*?\})\s*```", text, flags=re.DOTALL)
        if not m:
            m = re.search(r"(\{.*\})", text, flags=re.DOTALL)
        if not m:
            return None
        candidate = m.group(1)
        try:
            return json.loads(candidate)
        except Exception:
            try:
                cleaned = re.sub(r",\s*([}\]])", r"\1", candidate)
                return json.loads(cleaned)
            except Exception:
                return None

    @staticmethod
    def _text_candidates_from_response(response: Any) -> List[str]:
        texts: List[str] = []
        try:
            if hasattr(response, "outputs") and response.outputs:
                for out in response.outputs:
                    if hasattr(out, "content") and out.content:
                        for c in out.content:
                            if isinstance(c, str):
                                texts.append(c)
                            elif isinstance(c, dict) and c.get("text"):
                                texts.append(c.get("text"))
                            elif hasattr(c, "text"):
                                texts.append(getattr(c, "text"))
            if hasattr(response, "output") and response.output:
                for out in response.output:
                    if hasattr(out, "content") and out.content:
                        for c in out.content:
                            if isinstance(c, str):
                                texts.append(c)
                            elif isinstance(c, dict) and c.get("text"):
                                texts.append(c.get("text"))
                            elif hasattr(c, "text"):
                                texts.append(getattr(c, "text"))
            if hasattr(response, "content") and response.content:
                if isinstance(response.content, str):
                    texts.append(response.content)
                elif isinstance(response.content, list):
                    for c in response.content:
                        if isinstance(c, str):
                            texts.append(c)
                        elif isinstance(c, dict) and c.get("text"):
                            texts.append(c.get("text"))
        except Exception:
            pass
        texts.append(str(response))
        return texts


if __name__ == "__main__":
    main()


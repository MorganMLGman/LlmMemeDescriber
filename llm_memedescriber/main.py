import asyncio
import datetime
import email.utils
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

from google.genai import types
from sqlmodel import select
from .db_helpers import session_scope

from .config import load_settings, configure_logging
from .constants import *
from .models import Meme, DuplicateGroup as DBDuplicateGroup, MemeDuplicateGroup as DBDupeLink
from .deduplication import find_duplicate_groups
from .storage import WebDavStorage
from .storage_workers import StorageWorkerPool
from .storage_helpers import compute_and_persist_phash
from .preview_helpers import cleanup_orphaned_cache
from .llm import get_client, clear_client
from .llm.types import MediaContent, DescriptionRequest
from .llm.exceptions import RateLimitError, UnsupportedMediaError, LLMProviderError
from .llm.providers.config import GeminiConfig, OpenAIConfig, AnthropicConfig
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
    username = settings.webdav_username.get_secret_value() if settings.webdav_username else None
    password = settings.webdav_password.get_secret_value() if settings.webdav_password else None
    base_storage = WebDavStorage(base_url, auth=(username, password))
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
                # Get provider configuration from settings
                provider = getattr(settings, 'llm_provider', 'gemini')
                api_key_attr = f'{provider}_api_key'
                model_attr = f'{provider}_model'

                api_key = getattr(settings, api_key_attr, None)
                if api_key:
                    api_key = api_key.get_secret_value()

                model = getattr(settings, model_attr, 'gemini-3-flash-preview')

                # Get provider-specific config
                config = None
                if provider == 'gemini':
                    config = GeminiConfig()
                elif provider == 'openai':
                    config = OpenAIConfig()
                elif provider == 'anthropic':
                    config = AnthropicConfig()

                self.genai_client = get_client(provider, api_key, model, config)
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
        self._current_operation: Optional[str] = None  # Current operation name (syncing, transcoding, etc)
        self._operation_progress: Dict[str, Any] = {}  # Progress details for current operation
        self._operation_lock = threading.Lock()  # Lock for operation status access

    def set_operation_status(self, operation: Optional[str], progress: Optional[Dict[str, Any]] = None) -> None:
        """Set current operation status for progress tracking.

        Args:
            operation: Current operation name ('syncing', 'transcoding', 'completed', or None)
            progress: Progress details dict (e.g., {'transcoded': 3, 'total': 10})
        """
        with self._operation_lock:
            self._current_operation = operation
            self._operation_progress = progress or {}

    def get_operation_status(self) -> Dict[str, Any]:
        """Get current operation status.

        Returns:
            Dict with 'operation' (str or None) and 'progress' (dict) keys
        """
        with self._operation_lock:
            return {
                'operation': self._current_operation,
                'progress': self._operation_progress.copy()
            }

    def clear_operation_status(self) -> None:
        """Clear operation status."""
        with self._operation_lock:
            self._current_operation = None
            self._operation_progress = {}

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

    def _worker(self):
        logger.info("Worker started")
        while not self.stop_event.is_set():
            try:
                summary = self.sync_and_process()
                logger.debug("Sync: added=%d, removed=%d, unfilled=%d", summary['added'], summary['removed'], summary['unfilled'])
                if summary.get('saved') or summary.get('failed'):
                    logger.info("Generated: saved=%d, failed=%d, unsupported=%d", summary.get('saved'), summary.get('failed'), summary.get('unsupported'))
            except (TimeoutError, ConnectionError) as e:
                logger.warning("Storage connection timeout/error (will retry on next cycle): %s", str(e))
            except Exception:
                logger.exception("Worker error")
            if self.stop_event.wait(self.interval_seconds):
                break


    def _db_operation_with_retry(self, operation, max_retries: int = MAX_DB_RETRY_ATTEMPTS, initial_backoff: float = INITIAL_DB_BACKOFF):
        """Execute a DB operation with exponential backoff retry for SQLite locked errors.

        operation: callable that performs DB operation, should raise Exception on failure
        max_retries: maximum number of retry attempts (including initial)
        initial_backoff: initial backoff in seconds

        Returns the result of operation() if successful, None otherwise.
        """
        last_exc = None
        for attempt in range(max_retries):
            try:
                return operation()
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
                return None

        logger.exception("DB operation failed after %d attempts: %s", max_retries, last_exc)
        return None

    def _update_meme_attempt(
        self,
        filename: str,
        error: Optional[str] = None,
        status: Optional[str] = None
    ) -> None:
        """Update meme record with attempt counter, timestamp, and error info.
        
        Args:
            filename: The meme filename
            error: Error message to set (None to keep existing, empty string to clear)
            status: Optional status to set (e.g., 'unsupported')
        """
        try:
            with session_scope(self.engine) as session:
                m = session.exec(select(Meme).where(Meme.filename == filename)).first()
                if m:
                    m.attempts = (m.attempts or 0) + 1
                    m.last_attempt_at = datetime.datetime.now(datetime.timezone.utc)
                    if error is not None:
                        m.last_error = error
                    if status is not None:
                        m.status = status
                    session.add(m)
                    session.commit()
        except Exception:
            logger.exception("Failed to update meme attempt for %s", filename)

    def _process_single_meme(self, name: str) -> Dict[str, Any]:
        """Process a single meme: transcode if MKV, generate description and save to DB only.
        Returns dict with 'saved', 'unsupported', 'rate_limited', or 'failed' keys, and 'desc' with description.
        """
        if not is_supported(name):
            logger.debug("Skipping %s: file type not supported", name)
            return {'unsupported': True}

        # Check if file is MKV and needs transcoding
        if name.lower().endswith('.mkv'):
            try:
                transcode_result = self._transcode_and_replace_mkv(name)
                if not transcode_result['success']:
                    logger.error("Failed to transcode %s: %s", name, transcode_result.get('error'))
                    return {'failed': True}

                # Update name to new MP4 filename for description generation
                name = transcode_result['new_filename']
                logger.info("Transcoded to %s, proceeding with description generation", name)

            except Exception as exc:
                logger.exception("Exception during MKV transcoding for %s: %s", name, exc)
                self._update_meme_attempt(name, error=f"Transcode failed: {str(exc)}")
                return {'failed': True}

        try:
            desc = self.generate_description(name)
            
            if desc.get('rate_limited'):
                logger.warning("Rate limited while processing %s", name)
                return {'rate_limited': True}
            
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

    def _transcode_and_replace_mkv(self, mkv_filename: str) -> Dict[str, Any]:
        """Transcode MKV to MP4, upload to WebDAV, delete MKV, update database.

        Returns:
            Dict with 'success' (bool), 'new_filename' (str), and 'error' (str) keys
        """
        try:
            logger.info("Starting MKV transcoding workflow for %s", mkv_filename)

            # Step 1: Transcode MKV to MP4
            mp4_bytes, new_filename = self.storage.transcode_mkv_to_mp4(mkv_filename)
            logger.info("Transcoded %s to %s (%d bytes)", mkv_filename, new_filename, len(mp4_bytes))

            # Step 2: Upload MP4 to WebDAV
            self.storage.upload_fileobj(new_filename, mp4_bytes, overwrite=True)
            logger.info("Uploaded MP4: %s", new_filename)

            # Step 3: Update database filename (.mkv → .mp4)
            def update_db_filename():
                with session_scope(self.engine) as session:
                    m = session.exec(select(Meme).where(Meme.filename == mkv_filename)).first()
                    if not m:
                        raise Exception(f"Database record not found for {mkv_filename}")

                    # Check if MP4 record already exists (due to duplicate processing or manual upload)
                    existing_mp4 = session.exec(select(Meme).where(Meme.filename == new_filename)).first()

                    if existing_mp4:
                        # MP4 already in DB - delete the MKV record, keep MP4
                        logger.info(f"MP4 {new_filename} already exists in DB - deleting duplicate MKV record")
                        session.delete(m)
                    else:
                        # Update MKV record with new MP4 filename
                        m.filename = new_filename
                        if m.source_url:
                            m.source_url = m.source_url.replace(mkv_filename, new_filename)
                        m.updated_at = datetime.datetime.now(datetime.timezone.utc)
                        session.add(m)

                    session.commit()

            if not self._db_operation_with_retry(update_db_filename, max_retries=3):
                logger.error("Failed to update database filename")
                return {'success': False, 'error': "Database update failed"}

            logger.info("Updated database: %s -> %s", mkv_filename, new_filename)

            # Step 4: Delete original MKV from WebDAV
            try:
                self.storage.delete_file(mkv_filename)
                logger.info("Deleted original MKV: %s", mkv_filename)
            except Exception as delete_exc:
                logger.warning("Failed to delete MKV %s: %s", mkv_filename, delete_exc)
                # Non-critical - MP4 is already uploaded and DB updated

            return {'success': True, 'new_filename': new_filename}

        except Exception as exc:
            logger.exception("Transcoding workflow failed for %s: %s", mkv_filename, exc)
            self._update_meme_attempt(mkv_filename, error=f"Transcode failed: {str(exc)}")
            return {'success': False, 'error': str(exc)}

    def generate_description(self, filename: str) -> Dict[str, Any]:
        """Generate a description for `filename` using the instance LLM client and storage.

        Returns dict with description if successful, empty dict otherwise.
        Updates DB with error info and increments attempts counter.
        """
        # Download file from storage
        try:
            file_bytes = self.storage.download_file(filename)
        except Exception as exc:
            error_info = str(exc)
            logger.error("Error reading file %s from storage: %s", filename, exc)
            self._update_meme_attempt(filename, error=error_info)
            return {}

        # Detect MIME type for validation
        mime_type, _ = self._detect_media(filename)

        # Check if LLM client is configured
        if not self.genai_client:
            logger.warning("LLM client is not configured; skipping generation for %s", filename)
            return {}

        # Check if provider supports this media type
        if not self.genai_client.is_media_supported(mime_type):
            logger.info("Provider doesn't support MIME type %s for %s", mime_type, filename)
            self._update_meme_attempt(filename, error=f"unsupported_mime_{mime_type}", status='unsupported')
            return {}

        # Create description request
        try:
            request = DescriptionRequest(
                media=MediaContent(
                    data=file_bytes,
                    mime_type=mime_type,
                    filename=filename
                ),
                prompt=PROMPT
            )

            # Generate description using provider
            result = self.genai_client.generate_description(request)

            if result:
                self._update_meme_attempt(filename, error="")
                return result
            else:
                self._update_meme_attempt(filename, error="no_json_extracted")
                return {}

        except RateLimitError as exc:
            logger.warning(f"Rate limited for {filename}: {exc}")
            self._update_meme_attempt(filename, error=str(exc))
            return {'rate_limited': True, 'error': 'Rate limit exceeded'}

        except UnsupportedMediaError as exc:
            logger.info(f"Unsupported media {filename}: {exc}")
            self._update_meme_attempt(filename, error=str(exc), status='unsupported')
            return {}

        except LLMProviderError as exc:
            logger.error(f"Provider error for {filename}: {exc}")
            self._update_meme_attempt(filename, error=str(exc))
            return {}

        except Exception as exc:
            logger.exception(f"Unexpected error generating description for {filename}: {exc}")
            self._update_meme_attempt(filename, error=str(exc))
            return {}


    def sync_and_process(self) -> Dict[str, Any]:
        """Run a single sync and generate descriptions for unfilled files, then transcode existing MKVs."""

        if not self._sync_lock.acquire(blocking=False):
            logger.warning("Previous sync job still in progress; skipping this cycle")
            return {
                'added': 0, 'removed': 0, 'unfilled': 0,
                'saved': 0, 'failed': 0, 'unsupported': 0
            }

        try:
            # Set status to syncing
            self.set_operation_status('syncing', {'phase': 'WebDAV sync'})
            logger.info("Starting manual sync and process...")

            # Run main sync workflow
            sync_result = self._sync_and_process_impl()

            # After main sync, transcode existing MKVs
            self.set_operation_status('transcoding', {'phase': 'Scanning for MKVs'})
            logger.info("Main sync complete, now scanning for existing MKVs to transcode")
            transcode_result = self.transcode_existing_mkvs()

            # Combine results
            combined_result = {**sync_result, 'mkv_transcoding': transcode_result}
            self.set_operation_status('completed', {'result': combined_result})
            logger.info("Sync and MKV transcoding workflow complete")
            return combined_result
        finally:
            self._sync_lock.release()
            # Clear status after a delay to allow final poll
            threading.Timer(5.0, self.clear_operation_status).start()

    def transcode_existing_mkvs(self) -> Dict[str, Any]:
        """Scan database for existing MKV files and transcode them to MP4.

        This handles already-processed MKV files that were added before transcoding
        was implemented. Runs as part of manual sync operation.

        Returns:
            Dict with 'total_found', 'transcoded', 'failed', 'skipped' counts
        """
        try:
            logger.info("Scanning for existing MKV files to transcode")

            # Query database for all MKV files
            def get_mkv_files():
                with session_scope(self.engine) as session:
                    # Get all memes and filter for .mkv extension in Python (case-insensitive)
                    all_memes = session.exec(select(Meme)).all()
                    return [m.filename for m in all_memes if m.filename.lower().endswith('.mkv')]

            mkv_files = self._db_operation_with_retry(get_mkv_files) or []

            if not mkv_files:
                logger.info("No MKV files found to transcode")
                return {'total_found': 0, 'transcoded': 0, 'failed': 0, 'skipped': 0}

            logger.info("Found %d MKV files to transcode", len(mkv_files))

            # Set initial progress
            self.set_operation_status('transcoding', {
                'phase': 'Transcoding MKVs',
                'transcoded': 0,
                'total': len(mkv_files)
            })

            # Transcode each MKV in parallel using thread pool
            results = []
            completed = 0
            with ThreadPoolExecutor(max_workers=BATCH_PROCESS_WORKERS) as executor:
                futures = {executor.submit(self._transcode_and_replace_mkv, mkv): mkv
                          for mkv in mkv_files}

                for future in as_completed(futures):
                    mkv_name = futures[future]
                    try:
                        result = future.result(timeout=TRANSCODE_TIMEOUT + 30)  # Extra buffer
                        results.append(result)
                        completed += 1

                        # Update progress
                        self.set_operation_status('transcoding', {
                            'phase': 'Transcoding MKVs',
                            'transcoded': completed,
                            'total': len(mkv_files),
                            'current_file': mkv_name if completed < len(mkv_files) else None
                        })
                    except Exception as exc:
                        logger.exception("Exception transcoding %s: %s", mkv_name, exc)
                        results.append({'success': False, 'error': str(exc)})
                        completed += 1
                        self.set_operation_status('transcoding', {
                            'phase': 'Transcoding MKVs',
                            'transcoded': completed,
                            'total': len(mkv_files)
                        })

            # Calculate statistics
            transcoded = sum(1 for r in results if r.get('success'))
            failed = sum(1 for r in results if not r.get('success'))

            logger.info("Transcoding complete: %d succeeded, %d failed out of %d total",
                       transcoded, failed, len(mkv_files))

            return {
                'total_found': len(mkv_files),
                'transcoded': transcoded,
                'failed': failed,
                'skipped': 0
            }

        except Exception as exc:
            logger.exception("Failed to transcode existing MKVs: %s", exc)
            return {
                'total_found': 0,
                'transcoded': 0,
                'failed': 0,
                'skipped': 0,
                'error': str(exc)
            }

    def _sync_and_process_impl(self) -> Dict[str, int]:
        """Implementation of sync and process (called with lock held)."""
        
        # Load existing memes from database
        existing = {}
        try:
            with session_scope(self.engine) as session:
                memes = session.exec(select(Meme).where(Meme.status != 'removed')).all()
                for meme in memes:
                    existing[meme.filename] = {}
        except Exception as e:
            logger.warning("Failed to load existing memes from database: %s", e)

        entries = self.storage.list_files('/', recursive=False)
        server_names = {e['name'] for e in entries if not e['is_dir'] and is_supported(e['name'])}
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
            updated_path = None

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
            logger.exception("Failed to check DB status for unfilled detection")
            unfilled = []

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
            newly_added_memes = []  # Track newly added memes for phash calculation
            
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
                                            except Exception as e:
                                                logger.debug("Failed to parse date %s: %s", date_str, e)
                        except Exception as e:
                            logger.debug("Failed to update meme metadata: %s", e)
                        session.add(m)
                        newly_added_memes.append(name)
                
                for name in to_remove:
                    existing_m = existing_map.get(name)
                    if existing_m:
                        existing_m.status = 'removed'
                        session.add(existing_m)
                session.commit()
            
            # Calculate phash for newly added memes
            if newly_added_memes:
                logger.info("Calculating phash for %d newly added memes", len(newly_added_memes))
                for name in newly_added_memes:
                    try:
                        try:
                            asyncio.get_running_loop()
                            # If there's a running loop, skip phash calculation (shouldn't happen here but defensive)
                            logger.debug("Running loop detected, skipping phash calculation for %s", name)
                        except RuntimeError:
                            # No running loop, safe to use asyncio.run()
                            phash_result = asyncio.run(compute_and_persist_phash(name, self.storage, self.engine, timestamp=1.0))
                            if phash_result:
                                logger.debug("Calculated phash for %s: %s", name, phash_result)
                            else:
                                logger.debug("Failed to calculate phash for %s (likely unsupported format)", name)
                    except (TimeoutError, ConnectionError) as e:
                        logger.debug("Storage timeout/connection error calculating phash for %s: %s", name, str(e))
                    except Exception as e:
                        logger.debug("Error calculating phash for %s: %s", name, e)
        except Exception:
            logger.exception("Failed to persist listing changes to DB")

        saved_count = 0
        failed_count = 0
        unsupported_count = 0
        rate_limited = False
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
                    if result.get('rate_limited'):
                        logger.warning("Rate limit exceeded; pausing batch processing. Will retry on next sync cycle.")
                        rate_limited = True
                        failed_count += 1
                        break
                    elif result.get('saved'):
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

        if to_add:
            logger.info("Scheduling phash calculation for %d newly added memes", len(to_add))

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
            'rate_limited': rate_limited,
        }
        
        if result['added'] > 0:
            logger.info("Sync job completed: %d memes added", result['added'])
        
        return result

    @staticmethod
    def _detect_media(filename: str) -> Tuple[str, types.MediaResolution]:
        """Detect MIME type and media resolution from filename extension.

        Note: Media resolution is kept for compatibility but is now handled
        by individual providers in their configurations.
        """
        ext = str(filename).lower().split('.')[-1] if '.' in filename else ''
        if ext in IMAGE_EXTENSIONS:
            mime_type = "image/jpeg" if ext in {"jpg", "jpeg"} else f"image/{ext}"
            return mime_type, types.MediaResolution.MEDIA_RESOLUTION_HIGH
        if ext in VIDEO_EXTENSIONS:
            mime_type = "video/mp4" if ext == "mp4" else f"video/{ext}"
            return mime_type, types.MediaResolution.MEDIA_RESOLUTION_MEDIUM
        return "application/octet-stream", types.MediaResolution.MEDIA_RESOLUTION_HIGH


if __name__ == "__main__":
    main()


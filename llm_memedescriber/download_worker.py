import datetime
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from sqlmodel import Session, select
from sqlalchemy import Engine

from .download import download_video, DownloadProgress
from .models import DownloadJob, Meme
from .storage_workers import StorageWorkerPool
from .config import Settings

logger = logging.getLogger(__name__)


class DownloadWorker:
    """Background worker service for processing video download jobs."""

    def __init__(
        self,
        storage: StorageWorkerPool,
        engine: Engine,
        settings: Settings,
        max_workers: int = 2
    ):
        """
        Initialize download worker.

        Args:
            storage: StorageWorkerPool instance for WebDAV uploads
            engine: SQLAlchemy engine for database access
            settings: Application settings
            max_workers: Maximum concurrent downloads (default 2)
        """
        self._storage = storage
        self._engine = engine
        self._settings = settings
        self._max_workers = max_workers
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._stop_event = threading.Event()
        self._worker_thread: Optional[threading.Thread] = None
        self._poll_interval = 5  # seconds

        logger.info("DownloadWorker initialized with %d workers", max_workers)

    def start(self):
        """Start the background worker thread."""
        if self._worker_thread is not None and self._worker_thread.is_alive():
            logger.warning("DownloadWorker already running")
            return

        self._stop_event.clear()
        self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker_thread.start()
        logger.info("DownloadWorker started")

    def stop(self):
        """Stop the background worker thread gracefully."""
        if self._worker_thread is None or not self._worker_thread.is_alive():
            logger.warning("DownloadWorker not running")
            return

        logger.info("Stopping DownloadWorker...")
        self._stop_event.set()
        self._worker_thread.join(timeout=30)

        # Shutdown executor
        self._executor.shutdown(wait=True, cancel_futures=False)
        logger.info("DownloadWorker stopped")

    def _worker_loop(self):
        """Main worker loop that polls for pending jobs."""
        logger.info("DownloadWorker loop started (poll interval: %ds)", self._poll_interval)

        while not self._stop_event.is_set():
            try:
                # Query for pending jobs
                with Session(self._engine) as session:
                    statement = select(DownloadJob).where(
                        DownloadJob.status == "pending"
                    ).order_by(DownloadJob.created_at)

                    jobs = session.exec(statement).all()

                    if jobs:
                        logger.info("Found %d pending download job(s)", len(jobs))

                        for job in jobs:
                            # Submit to thread pool for processing
                            self._executor.submit(self._process_download_job, job.id)

            except Exception as e:
                logger.error("Error in download worker loop: %s", e, exc_info=True)

            # Wait before next poll
            self._stop_event.wait(self._poll_interval)

        logger.info("DownloadWorker loop exited")

    def _process_download_job(self, job_id: int):
        """
        Process a single download job.

        Args:
            job_id: DownloadJob ID to process
        """
        try:
            with Session(self._engine) as session:
                # Fetch job
                job = session.get(DownloadJob, job_id)
                if not job:
                    logger.warning("DownloadJob %d not found", job_id)
                    return

                # Check if job is still pending (avoid race conditions)
                if job.status != "pending":
                    logger.info("DownloadJob %d already processed (status: %s)", job_id, job.status)
                    return

                # Update status to downloading
                job.status = "downloading"
                job.started_at = datetime.datetime.now(datetime.timezone.utc)
                session.add(job)
                session.commit()

                logger.info("Processing DownloadJob %d: %s", job_id, job.url)

                # Progress callback to update database
                def progress_callback(progress: DownloadProgress):
                    try:
                        with Session(self._engine) as progress_session:
                            progress_job = progress_session.get(DownloadJob, job_id)
                            if progress_job:
                                progress_job.progress_percent = progress.progress_percent
                                progress_session.add(progress_job)
                                progress_session.commit()
                    except Exception as e:
                        logger.warning("Failed to update progress for job %d: %s", job_id, e)

                # Download video
                try:
                    video_bytes, filename, metadata = download_video(
                        url=job.url,
                        max_filesize_mb=self._settings.download_max_filesize_mb,
                        progress_callback=progress_callback,
                        ytdlp_options={
                            'format': self._settings.ytdlp_format,
                        }
                    )

                    logger.info(
                        "Downloaded video for job %d: %s (%.2f MB)",
                        job_id,
                        filename,
                        len(video_bytes) / (1024 * 1024)
                    )

                except Exception as e:
                    logger.error("Download failed for job %d: %s", job_id, e, exc_info=True)

                    # Update job as failed
                    with Session(self._engine) as fail_session:
                        fail_job = fail_session.get(DownloadJob, job_id)
                        if fail_job:
                            fail_job.status = "failed"
                            fail_job.error_message = str(e)
                            fail_job.completed_at = datetime.datetime.now(datetime.timezone.utc)
                            fail_session.add(fail_job)
                            fail_session.commit()

                    return

                # Upload to WebDAV
                try:
                    # Ensure filename starts with /
                    webdav_path = f"/{filename}" if not filename.startswith('/') else filename

                    self._storage.upload_fileobj(
                        path=webdav_path,
                        data=video_bytes,
                        overwrite=False  # Don't overwrite existing files
                    )

                    logger.info("Uploaded to WebDAV: %s", webdav_path)

                except Exception as e:
                    logger.error("WebDAV upload failed for job %d: %s", job_id, e, exc_info=True)

                    # Update job as failed
                    with Session(self._engine) as fail_session:
                        fail_job = fail_session.get(DownloadJob, job_id)
                        if fail_job:
                            fail_job.status = "failed"
                            fail_job.error_message = f"WebDAV upload failed: {str(e)}"
                            fail_job.completed_at = datetime.datetime.now(datetime.timezone.utc)
                            fail_session.add(fail_job)
                            fail_session.commit()

                    return

                # Create Meme record
                try:
                    with Session(self._engine) as meme_session:
                        # Check if meme already exists
                        existing_meme = meme_session.exec(
                            select(Meme).where(Meme.filename == filename)
                        ).first()

                        if existing_meme:
                            logger.info("Meme already exists for filename: %s", filename)
                        else:
                            # Create new Meme record
                            meme = Meme(
                                filename=filename,
                                source_url=job.url,  # Store original video URL
                                status="pending",  # Will be processed by existing worker
                                created_at=datetime.datetime.now(datetime.timezone.utc),
                                updated_at=datetime.datetime.now(datetime.timezone.utc)
                            )
                            meme_session.add(meme)
                            meme_session.commit()

                            logger.info("Created Meme record for: %s", filename)

                except Exception as e:
                    logger.error("Failed to create Meme record for job %d: %s", job_id, e, exc_info=True)

                    # Update job as failed
                    with Session(self._engine) as fail_session:
                        fail_job = fail_session.get(DownloadJob, job_id)
                        if fail_job:
                            fail_job.status = "failed"
                            fail_job.error_message = f"Failed to create Meme record: {str(e)}"
                            fail_job.completed_at = datetime.datetime.now(datetime.timezone.utc)
                            fail_session.add(fail_job)
                            fail_session.commit()

                    return

                # Update job as completed
                with Session(self._engine) as success_session:
                    success_job = success_session.get(DownloadJob, job_id)
                    if success_job:
                        success_job.status = "completed"
                        success_job.progress_percent = 100.0
                        success_job.filename = filename
                        success_job.video_title = metadata.get('title')
                        success_job.video_duration = metadata.get('duration')
                        success_job.file_size_bytes = metadata.get('filesize')
                        success_job.completed_at = datetime.datetime.now(datetime.timezone.utc)
                        success_session.add(success_job)
                        success_session.commit()

                        logger.info("DownloadJob %d completed successfully", job_id)

        except Exception as e:
            logger.error("Unexpected error processing job %d: %s", job_id, e, exc_info=True)

            # Try to mark as failed
            try:
                with Session(self._engine) as error_session:
                    error_job = error_session.get(DownloadJob, job_id)
                    if error_job:
                        error_job.status = "failed"
                        error_job.error_message = f"Unexpected error: {str(e)}"
                        error_job.completed_at = datetime.datetime.now(datetime.timezone.utc)
                        error_session.add(error_job)
                        error_session.commit()
            except Exception as db_error:
                logger.error("Failed to update job status after error: %s", db_error)

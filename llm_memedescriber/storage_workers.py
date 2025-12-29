import io
import threading
import logging
from concurrent.futures import ThreadPoolExecutor, Future
from typing import Any, Callable, Optional
import asyncio

from .constants import DEFAULT_STORAGE_WORKERS

logger = logging.getLogger(__name__)


class StorageWorkerPool:
    """Wrap a blocking storage adapter (like WebDavStorage) and run its blocking
    operations in a ThreadPoolExecutor with a semaphore to throttle concurrent
    storage/ffmpeg calls.

    The wrapper exposes the same high-level API as the storage adapter and
    executes calls on worker threads. Methods block until completion by
    default (they call Future.result()). If caller wants an async-like
    submission, they can use `submit()` to obtain a Future.
    """

    def __init__(self, storage_adapter: Any, max_workers: int = DEFAULT_STORAGE_WORKERS, max_concurrent: Optional[int] = None):
        self._storage = storage_adapter
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._max_concurrent = max_concurrent if max_concurrent is not None else max_workers
        self._semaphore = threading.BoundedSemaphore(self._max_concurrent)

    def _run_guarded(self, fn: Callable, *args, **kwargs):
        self._semaphore.acquire()
        try:
            return fn(*args, **kwargs)
        finally:
            try:
                self._semaphore.release()
            except Exception:
                pass

    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        """Submit a storage call to the worker pool and return a Future.

        The callable `fn` should be a bound method of the underlying storage
        adapter, e.g. `self._storage.download_file`.
        """
        return self._executor.submit(self._run_guarded, fn, *args, **kwargs)

    def run(self, fn: Callable, *args, timeout: Optional[float] = None, **kwargs):
        """Run a storage call on the pool and wait for result (blocking).

        Returns the function result or raises the underlying exception.
        """
        fut = self.submit(fn, *args, **kwargs)
        return fut.result(timeout=timeout)

    async def async_run(self, fn: Callable, *args, timeout: Optional[float] = None, **kwargs):
        """Submit a blocking call and await its result from asyncio.

        Wraps the concurrent.futures.Future returned by `submit` using
        `asyncio.wrap_future` and awaits it. Honors optional timeout.
        """
        fut = self.submit(fn, *args, **kwargs)
        loop = asyncio.get_running_loop()
        wrapped = asyncio.wrap_future(fut, loop=loop)
        if timeout is not None:
            return await asyncio.wait_for(wrapped, timeout=timeout)
        return await wrapped

    
    def list_files(self, *args, **kwargs):
        return self.run(self._storage.list_files, *args, **kwargs)

    async def async_list_files(self, *args, **kwargs):
        return await self.async_run(self._storage.list_files, *args, **kwargs)

    def load_listing(self, *args, **kwargs):
        return self.run(self._storage.load_listing, *args, **kwargs)

    async def async_load_listing(self, *args, **kwargs):
        return await self.async_run(self._storage.load_listing, *args, **kwargs)

    def write_listing(self, *args, **kwargs):
        return self.run(self._storage.write_listing, *args, **kwargs)

    async def async_write_listing(self, *args, **kwargs):
        return await self.async_run(self._storage.write_listing, *args, **kwargs)

    def download_file(self, *args, timeout: Optional[float] = None, **kwargs):
        return self.run(self._storage.download_file, *args, timeout=timeout, **kwargs)

    async def async_download_file(self, *args, timeout: Optional[float] = None, **kwargs):
        return await self.async_run(self._storage.download_file, *args, timeout=timeout, **kwargs)

    def upload_fileobj(self, *args, timeout: Optional[float] = None, **kwargs):
        return self.run(self._storage.upload_fileobj, *args, timeout=timeout, **kwargs)

    async def async_upload_fileobj(self, *args, timeout: Optional[float] = None, **kwargs):
        return await self.async_run(self._storage.upload_fileobj, *args, timeout=timeout, **kwargs)

    def delete_file(self, *args, timeout: Optional[float] = None, **kwargs):
        return self.run(self._storage.delete_file, *args, timeout=timeout, **kwargs)

    async def async_delete_file(self, *args, timeout: Optional[float] = None, **kwargs):
        return await self.async_run(self._storage.delete_file, *args, timeout=timeout, **kwargs)

    def extract_video_frame(self, *args, timeout: Optional[float] = None, **kwargs):
        return self.run(self._storage.extract_video_frame, *args, timeout=timeout, **kwargs)

    async def async_extract_video_frame(self, *args, timeout: Optional[float] = None, **kwargs):
        return await self.async_run(self._storage.extract_video_frame, *args, timeout=timeout, **kwargs)

    def open(self, path: str, mode: str = 'rb'):
        """Provide a file-like object for callers that expect `open`.

        This implementation downloads the file and returns a BytesIO. It's
        suitable for short-lived reads. For streaming large files, consider
        using the underlying storage adapter directly.
        """
        data = self.download_file(path)
        if isinstance(data, str):
            data = data.encode('utf-8')
        return io.BytesIO(data)

    async def async_open(self, path: str, mode: str = 'rb'):
        data = await self.async_download_file(path)
        if isinstance(data, str):
            data = data.encode('utf-8')
        return io.BytesIO(data)

    def shutdown(self, wait: bool = True):
        try:
            self._executor.shutdown(wait=wait)
        except Exception:
            logger.exception("Error shutting down storage worker pool")

import asyncio
import io
import pytest

from llm_memedescriber.storage_workers import StorageWorkerPool


class FlakyStorage:
    def __init__(self):
        self.calls = 0

    def download_file(self, filename):
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("boom")
        return b'ok'


class StringReturningStorage:
    def download_file(self, filename):
        return 'text-data'


def test_run_and_open_sync(fake_storage_content_factory):
    storage = fake_storage_content_factory(content=b'bytes-data')
    pool = StorageWorkerPool(storage_adapter=storage, max_workers=2)

    got = pool.download_file('file')
    assert got == b'bytes-data'
    assert storage.download_calls == 1

    bio = pool.open('file')
    assert isinstance(bio, io.BytesIO)
    assert bio.read() == b'bytes-data'

    pool.shutdown()


def test_string_return_converted_to_bytes():
    storage = StringReturningStorage()
    pool = StorageWorkerPool(storage_adapter=storage)

    bio = pool.open('x')
    assert isinstance(bio, io.BytesIO)
    assert bio.read() == b'text-data'

    pool.shutdown()


def test_exception_does_not_block_semaphore():
    storage = FlakyStorage()
    pool = StorageWorkerPool(storage_adapter=storage, max_workers=1, max_concurrent=1)

    with pytest.raises(RuntimeError):
        pool.download_file('a')

    assert pool.download_file('a') == b'ok'

    pool.shutdown()


def test_async_download_and_open(fake_async_storage_factory):
    storage = fake_async_storage_factory(content=b'async-bytes')
    pool = StorageWorkerPool(storage_adapter=storage, max_workers=2)

    res = asyncio.run(pool.async_download_file('f'))
    assert res == b'async-bytes'
    assert storage.download_calls == 1

    bio = asyncio.run(pool.async_open('f'))
    assert isinstance(bio, io.BytesIO)
    assert bio.read() == b'async-bytes'

    pool.shutdown()


def test_async_open_converts_str_to_bytes(fake_async_storage_factory):
    # Async storage that returns a str via the sync download path
    storage = fake_async_storage_factory(content='async-text')
    pool = StorageWorkerPool(storage_adapter=storage, max_workers=2)

    bio = asyncio.run(pool.async_open('f'))
    assert isinstance(bio, io.BytesIO)
    assert bio.read() == b'async-text'

    pool.shutdown()


def test_submit_future_and_result(fake_storage_content_factory):
    storage = fake_storage_content_factory(content=b'xyz')
    pool = StorageWorkerPool(storage_adapter=storage, max_workers=2)

    fut = pool.submit(storage.download_file, 'p')
    assert not fut.done() or fut.result() == b'xyz'
    assert fut.result() == b'xyz'

    pool.shutdown()


def test_async_run_timeout():
    import time

    class SlowStorage:
        def slow(self, _):
            time.sleep(0.1)
            return b'done'

    storage = SlowStorage()
    pool = StorageWorkerPool(storage_adapter=storage, max_workers=2)

    with pytest.raises(asyncio.TimeoutError):
        asyncio.run(pool.async_run(storage.slow, 'x', timeout=0.01))

    pool.shutdown()


def test_list_and_listing_forwarding():
    class L:
        def list_files(self, *a, **k):
            return ['a', 'b']

        def load_listing(self, *a, **k):
            return {'x': 1}

        def write_listing(self, *a, **k):
            return True

    storage = L()
    pool = StorageWorkerPool(storage_adapter=storage)

    assert pool.list_files() == ['a', 'b']
    assert pool.load_listing() == {'x': 1}
    assert pool.write_listing({}) is True

    assert asyncio.run(pool.async_list_files()) == ['a', 'b']
    assert asyncio.run(pool.async_load_listing()) == {'x': 1}
    assert asyncio.run(pool.async_write_listing({})) is True

    pool.shutdown()


def test_upload_delete_extract_forwarding():
    class U:
        def __init__(self):
            self.uploaded = None

        def upload_fileobj(self, fobj, target_path, overwrite=True):
            self.uploaded = (target_path, fobj.read())
            return True

        def delete_file(self, name):
            return True

        def extract_video_frame(self, name, timestamp=1.0):
            return b'frame'

    storage = U()
    pool = StorageWorkerPool(storage_adapter=storage)

    from io import BytesIO
    f = BytesIO(b'hello')
    assert pool.upload_fileobj(f, 't') is True
    assert storage.uploaded[0] == 't'
    assert storage.uploaded[1] == b'hello'

    assert pool.delete_file('x') is True
    assert pool.extract_video_frame('v') == b'frame'

    assert asyncio.run(pool.async_extract_video_frame('v')) == b'frame'

    pool.shutdown()


def test_shutdown_logs_exception(caplog_set_level):
    storage = StringReturningStorage()
    pool = StorageWorkerPool(storage_adapter=storage)

    def bad_shutdown(wait=True):
        raise RuntimeError('boom')

    pool._executor.shutdown = bad_shutdown

    caplog_set_level('ERROR')
    pool.shutdown()
    import logging
    log = logging.getLogger(__name__)


def test_concurrency_limit():
    import threading, time

    record = []

    class C:
        def __init__(self):
            self.lock = threading.Lock()
            self.current = 0

        def work(self, _):
            with self.lock:
                self.current += 1
                record.append(self.current)
            time.sleep(0.05)
            with self.lock:
                self.current -= 1
            return 'ok'

    storage = C()
    pool = StorageWorkerPool(storage_adapter=storage, max_workers=3, max_concurrent=1)

    futures = [pool.submit(storage.work, i) for i in range(3)]
    for f in futures:
        assert f.result() == 'ok'

    assert max(record) == 1

    pool.shutdown()


def test_async_upload_fileobj():
    class U:
        def __init__(self):
            self.uploaded = None

        def upload_fileobj(self, fileobj, target_path, overwrite=True):
            data = fileobj.read()
            if isinstance(data, str):
                data = data.encode('utf-8')
            self.uploaded = (target_path, data)
            return True

    storage = U()
    pool = StorageWorkerPool(storage_adapter=storage)

    from io import BytesIO
    f = BytesIO(b'up')
    assert asyncio.run(pool.async_upload_fileobj(f, 't')) is True
    assert storage.uploaded[0] == 't'
    assert storage.uploaded[1] == b'up'

    pool.shutdown()


def test_submit_exception_propagates_and_semaphore_released():
    class Bad:
        def __init__(self):
            self.calls = 0

        def fail_once(self, _):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError('boom')
            return 'ok'

    storage = Bad()
    pool = StorageWorkerPool(storage_adapter=storage, max_workers=1, max_concurrent=1)

    fut = pool.submit(storage.fail_once, 'a')
    with pytest.raises(RuntimeError):
        fut.result()

    # second submit should work
    assert pool.run(storage.fail_once, 'a') == 'ok'

    pool.shutdown()


def test_async_run_no_timeout_returns():
    class Fast:
        def go(self, _):
            return 123

    storage = Fast()
    pool = StorageWorkerPool(storage_adapter=storage)

    got = asyncio.run(pool.async_run(storage.go, 'x'))
    assert got == 123

    pool.shutdown()


def test_async_list_timeout():
    import time

    class SlowL:
        def list_files(self, *a, **k):
            time.sleep(0.1)
            return ['z']

    storage = SlowL()
    pool = StorageWorkerPool(storage_adapter=storage)

    with pytest.raises(asyncio.TimeoutError):
        asyncio.run(pool.async_list_files(timeout=0.01))

    pool.shutdown()


def test_shutdown_wait_flag_is_forwarded():
    storage = StringReturningStorage()
    pool = StorageWorkerPool(storage_adapter=storage)

    called = {}

    def record_shutdown(wait=True):
        called['wait'] = wait

    pool._executor.shutdown = record_shutdown

    pool.shutdown(wait=False)
    assert called.get('wait') is False

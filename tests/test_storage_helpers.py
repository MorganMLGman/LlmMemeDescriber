import asyncio
import sqlite3
import time
from contextlib import contextmanager

import pytest

from sqlmodel import SQLModel, create_engine, select

from llm_memedescriber import storage_helpers
from llm_memedescriber.models import Meme


class AsyncStorage:
    def __init__(self, data):
        self._data = data

    async def async_download_file(self, path):
        return self._data


class SyncStorage:
    def __init__(self, data):
        self._data = data

    def download_file(self, path):
        return self._data


def test_call_storage_prefers_async():
    s = AsyncStorage(b'X')
    res = asyncio.run(storage_helpers.call_storage(s, 'download_file', 'f'))
    assert res == b'X'


def test_call_storage_executes_sync_in_executor():
    s = SyncStorage(b'Y')
    res = asyncio.run(storage_helpers.call_storage(s, 'download_file', 'f'))
    assert res == b'Y'


def test_call_storage_missing_method_raises():
    class Empty: ...

    with pytest.raises(AttributeError):
        asyncio.run(storage_helpers.call_storage(Empty(), 'download_file', 'f'))


@contextmanager
def make_engine():
    eng = create_engine("sqlite:///:memory:", echo=False)
    SQLModel.metadata.create_all(eng)
    try:
        yield eng
    finally:
        try:
            eng.dispose()
        except Exception:
            pass


def test_compute_and_persist_phash_no_db_record(monkeypatch):
    storage = SyncStorage(b'data')

    monkeypatch.setattr(storage_helpers, '_db_readonly_detected', False)

    monkeypatch.setattr(storage_helpers, 'calculate_phash', lambda data: 'phash123')

    with make_engine() as eng:
        got = asyncio.run(storage_helpers.compute_and_persist_phash('a.png', storage, eng))
        assert got is None


def test_compute_and_persist_phash_success_persists(monkeypatch):
    storage = SyncStorage(b'data')
    monkeypatch.setattr(storage_helpers, '_db_readonly_detected', False)
    monkeypatch.setattr(storage_helpers, 'calculate_phash', lambda data: 'phashXYZ')

    with make_engine() as eng:
        from sqlmodel import Session
        with Session(eng) as s:
            m = Meme(filename='ok.png')
            s.add(m)
            s.commit()

        got = asyncio.run(storage_helpers.compute_and_persist_phash('ok.png', storage, eng))
        assert got == 'phashXYZ'

        from sqlmodel import Session
        with Session(eng) as s:
            m2 = s.exec(select(Meme).where(Meme.filename == 'ok.png')).first()
            assert m2.phash == 'phashXYZ'


def test_compute_and_persist_phash_empty_data(monkeypatch):
    storage = SyncStorage(b'')
    monkeypatch.setattr(storage_helpers, '_db_readonly_detected', False)
    monkeypatch.setattr(storage_helpers, 'calculate_phash', lambda data: 'x')

    with make_engine() as eng:
        from sqlmodel import Session
        with Session(eng) as s:
            s.add(Meme(filename='e.png'))
            s.commit()

        got = asyncio.run(storage_helpers.compute_and_persist_phash('e.png', storage, eng))
        assert got is None


def test_compute_and_persist_phash_video_extract_failure(monkeypatch):
    class VideoStorage:
        VIDEO_EXTENSIONS = ['mp4']

        async def async_extract_video_frame(self, filename, timestamp=1.0):
            raise RuntimeError('ffmpeg fail')

    monkeypatch.setattr(storage_helpers, '_db_readonly_detected', False)
    s = VideoStorage()
    with make_engine() as eng:
        from sqlmodel import Session
        with Session(eng) as sess:
            sess.add(Meme(filename='v.mp4'))
            sess.commit()

        got = asyncio.run(storage_helpers.compute_and_persist_phash('v.mp4', s, eng))
        assert got is None


def test_compute_and_persist_phash_commit_readonly_sets_flag(monkeypatch):
    class FakeSession:
        def __init__(self):
            self.added = None

        def exec(self, q):
            class Q:
                def first(inner):
                    return Meme(filename='r.png')
            return Q()

        def add(self, m):
            self.added = m

        def refresh(self, m):
            pass

        def commit(self):
            raise sqlite3.OperationalError('attempt to write a readonly database')

        def close(self):
            pass

    @contextmanager
    def fake_scope(engine):
        yield FakeSession()

    monkeypatch.setattr(storage_helpers, 'session_scope', fake_scope)
    monkeypatch.setattr(storage_helpers, 'calculate_phash', lambda data: 'p')
    monkeypatch.setattr(storage_helpers, '_db_readonly_detected', False)

    class S:
        def download_file(self, f):
            return b'd'

    got = asyncio.run(storage_helpers.compute_and_persist_phash('r.png', S(), object()))
    assert got is None
    assert storage_helpers._db_readonly_detected is True


def test_compute_and_persist_phash_commit_retries_then_succeeds(monkeypatch):
    class FakeSession:
        def __init__(self):
            self.commit_calls = 0

        def exec(self, q):
            class Q:
                def first(inner):
                    return Meme(filename='retry.png')
            return Q()

        def add(self, m):
            self._m = m

        def refresh(self, m):
            pass

        def commit(self):
            self.commit_calls += 1
            if self.commit_calls < 3:
                raise Exception('transient I/O')

        def close(self):
            pass

    @contextmanager
    def fake_scope(engine):
        yield FakeSession()

    monkeypatch.setattr(storage_helpers, 'session_scope', fake_scope)
    monkeypatch.setattr(storage_helpers, 'calculate_phash', lambda data: 'phashR')
    monkeypatch.setattr(storage_helpers, '_db_readonly_detected', False)
    # avoid sleeping delays
    monkeypatch.setattr(storage_helpers, 'time', type('T', (), {'sleep': lambda *_: None}))

    class S:
        def download_file(self, f):
            return b'x'

    got = asyncio.run(storage_helpers.compute_and_persist_phash('retry.png', S(), object()))
    assert got == 'phashR'


def test_compute_and_persist_phash_updated_at_assignment_error_is_ignored(monkeypatch):
    class M:
        def __init__(self, filename):
            self.filename = filename
            self.phash = None

        def __setattr__(self, name, value):
            if name == 'updated_at':
                raise RuntimeError('cannot set updated_at')
            object.__setattr__(self, name, value)

    class FakeSession2:
        def exec(self, q):
            class Q:
                def first(inner):
                    return M('u.png')
            return Q()

        def add(self, m):
            self.m = m

        def refresh(self, m):
            pass

        def commit(self):
            self._committed = True

        def close(self):
            pass

    @contextmanager
    def fake_scope2(engine):
        yield FakeSession2()

    monkeypatch.setattr(storage_helpers, 'session_scope', fake_scope2)
    monkeypatch.setattr(storage_helpers, 'calculate_phash', lambda data: 'phashU')
    monkeypatch.setattr(storage_helpers, '_db_readonly_detected', False)

    class S2:
        def download_file(self, f):
            return b'd'

    got = asyncio.run(storage_helpers.compute_and_persist_phash('u.png', S2(), object()))
    assert got == 'phashU'


def test_compute_and_persist_phash_phash_none(monkeypatch):
    storage = SyncStorage(b'data')
    monkeypatch.setattr(storage_helpers, '_db_readonly_detected', False)
    monkeypatch.setattr(storage_helpers, 'calculate_phash', lambda data: None)

    with make_engine() as eng:
        from sqlmodel import Session
        with Session(eng) as s:
            s.add(Meme(filename='n.png'))
            s.commit()

        got = asyncio.run(storage_helpers.compute_and_persist_phash('n.png', storage, eng))
        assert got is None


def test_compute_and_persist_phash_video_extract_returns_str(monkeypatch):
    class VideoStorage2:
        VIDEO_EXTENSIONS = ['mp4']

        async def async_extract_video_frame(self, filename, timestamp=1.0):
            return 'frame-str'

    monkeypatch.setattr(storage_helpers, '_db_readonly_detected', False)
    monkeypatch.setattr(storage_helpers, 'calculate_phash', lambda data: 'phashS')

    with make_engine() as eng:
        from sqlmodel import Session
        with Session(eng) as s:
            s.add(Meme(filename='vs.mp4'))
            s.commit()

        got = asyncio.run(storage_helpers.compute_and_persist_phash('vs.mp4', VideoStorage2(), eng))
        assert got == 'phashS'


def test_compute_and_persist_phash_calculate_phash_raises(monkeypatch):
    storage = SyncStorage(b'data')
    monkeypatch.setattr(storage_helpers, '_db_readonly_detected', False)

    def bad_phash(data):
        raise RuntimeError("boom")
    monkeypatch.setattr(storage_helpers, 'calculate_phash', bad_phash)

    with make_engine() as eng:
        from sqlmodel import Session
        with Session(eng) as s:
            s.add(Meme(filename='err.png'))
            s.commit()

        got = asyncio.run(storage_helpers.compute_and_persist_phash('err.png', storage, eng))
        assert got is None


def test_compute_and_persist_phash_commit_sets_flag_on_readonly_in_message(monkeypatch):
    class FakeSession:
        def exec(self, q):
            class Q:
                def first(inner):
                    return Meme(filename='r2.png')
            return Q()

        def add(self, m):
            self.added = m

        def refresh(self, m):
            pass

        def commit(self):
            raise Exception('something readonly happened')

        def close(self):
            pass

    @contextmanager
    def fake_scope(engine):
        yield FakeSession()

    monkeypatch.setattr(storage_helpers, 'session_scope', fake_scope)
    monkeypatch.setattr(storage_helpers, 'calculate_phash', lambda data: 'p2')
    monkeypatch.setattr(storage_helpers, '_db_readonly_detected', False)

    class S:
        def download_file(self, f):
            return b'd'

    got = asyncio.run(storage_helpers.compute_and_persist_phash('r2.png', S(), object()))
    assert got is None
    assert storage_helpers._db_readonly_detected is True


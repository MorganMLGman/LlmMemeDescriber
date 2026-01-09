"""Shared test helpers for image and DB utilities used across tests."""
from contextlib import contextmanager
from pathlib import Path
from typing import List
from sqlmodel import SQLModel, create_engine, Session

DATA_DIR = Path(__file__).parent / "data"


@contextmanager
def create_in_memory_session():
    engine = create_engine("sqlite:///:memory:", echo=False)
    SQLModel.metadata.create_all(engine)
    try:
        with Session(engine) as session:
            yield session
    finally:
        try:
            engine.dispose()
        except Exception:
            pass


def load_test_image_bytes(name: str) -> bytes:
    path = DATA_DIR / name
    with open(path, "rb") as f:
        return f.read()


# ---- pytest fixtures ----
import pytest
from sqlmodel import Session, create_engine


@pytest.fixture
def in_memory_session():
    """Provide a SQLModel session backed by a fresh in-memory sqlite DB."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    SQLModel.metadata.create_all(engine)
    try:
        with Session(engine) as session:
            yield session
    finally:
        try:
            engine.dispose()
        except Exception:
            pass


@pytest.fixture
def load_test_image():
    """Fixture that returns a callable to load test images by name."""
    def _loader(name: str) -> bytes:
        return load_test_image_bytes(name)
    return _loader


@pytest.fixture
def fake_storage_factory():
    def _make(fail_on=None):
        return FakeDeleteStorage(fail_on=fail_on)
    return _make


@pytest.fixture
def fake_client_factory():
    def _make(mapping):
        return FakeClient(mapping)
    return _make


@pytest.fixture
def fake_client_open_factory():
    def _make(content=None, raise_on_open=False):
        return FakeClientOpen(content=content, raise_on_open=raise_on_open)
    return _make


@pytest.fixture
def fake_upload_client_factory():
    def _make(fail_times=0, fail_exc=None):
        return FakeUploadClient(fail_times=fail_times, fail_exc=fail_exc)
    return _make


@pytest.fixture
def caplog_set_level(caplog):
    def _set(level, logger=None):
        if logger:
            caplog.set_level(level, logger=logger)
        else:
            caplog.set_level(level)
    return _set


def mask_ones_val(k: int) -> int:
    if k <= 0:
        return 0
    return (1 << k) - 1


def hex_from_val(val: int) -> str:
    return f"{val & ((1 << 64) - 1):016x}"


def hex_ones(k: int, shift: int = 0) -> str:
    val = mask_ones_val(k) << shift
    return hex_from_val(val)


class FakeDeleteStorage:
    def __init__(self, fail_on=None):
        self.deleted = []
        self.fail_on = set(fail_on or [])

    def delete_file(self, name):
        if name in self.fail_on:
            raise RuntimeError("storage failure")
        self.deleted.append(name)

class FakeStorage:
    def __init__(self, content: bytes = None):
        self.content = content
        self.download_calls = 0
        self.extract_calls = 0

    def download_file(self, filename):
        self.download_calls += 1
        return self.content

    def extract_video_frame(self, filename, timestamp=1.0):
        self.extract_calls += 1
        return self.content


class AsyncFakeStorage(FakeStorage):
    async def async_download_file(self, filename):
        self.download_calls += 1
        return self.content

    async def async_extract_video_frame(self, filename, timestamp=1.0):
        self.extract_calls += 1
        return self.content


@pytest.fixture
def fake_storage_content_factory():
    def _make(content=None):
        return FakeStorage(content=content)
    return _make


@pytest.fixture
def fake_async_storage_factory():
    def _make(content=None):
        return AsyncFakeStorage(content=content)
    return _make


class FakeClient:
    def __init__(self, mapping):
        self.mapping = mapping

    def ls(self, path):
        if path == "RAISE":
            raise RuntimeError("boom")
        return self.mapping.get(path, [])


class FakeClientOpen:
    def __init__(self, content=None, raise_on_open=False):
        self.content = content
        self.raise_on_open = raise_on_open
        self.open_calls = []

    def open(self, path, mode='r'):
        self.open_calls.append((path, mode))
        if self.raise_on_open:
            raise RuntimeError("open fail")
        if isinstance(self.content, (bytes, bytearray)):
            from io import BytesIO
            return BytesIO(self.content)
        from io import StringIO
        return StringIO(self.content if self.content is not None else "")


class FakeUploadClient:
    def __init__(self, fail_times=0, fail_exc=None):
        self.fail_times = fail_times
        self.fail_exc = fail_exc
        self.calls = 0
        self.last_uploaded = None

    def upload_fileobj(self, fileobj, target_path, overwrite=True):
        self.calls += 1
        data = fileobj.read()
        if isinstance(data, str):
            data = data.encode('utf-8')
        self.last_uploaded = (target_path, data)
        if self.calls <= self.fail_times:
            if self.fail_exc is not None:
                raise self.fail_exc
            raise Exception('Locked or temporarily unavailable (423)')
        return True


def snapshot_logging():
    import logging
    root = logging.getLogger()
    snapshot = {
        'root_level': root.level,
        'root_handlers': list(root.handlers),
        'levels': {},
        'handlers': {},
    }
    names = ['alembic', 'alembic.runtime', 'google_genai', 'google_genai.models', 'uvicorn', 'uvicorn.error']
    for n in names:
        lg = logging.getLogger(n)
        snapshot['levels'][n] = lg.level
        snapshot['handlers'][n] = list(lg.handlers)
    return snapshot


def restore_logging(snapshot):
    import logging
    root = logging.getLogger()
    root.handlers[:] = []
    for h in snapshot['root_handlers']:
        root.addHandler(h)
    root.setLevel(snapshot['root_level'])

    for n, lvl in snapshot['levels'].items():
        lg = logging.getLogger(n)
        lg.setLevel(lvl)
        lg.handlers[:] = []
        for h in snapshot['handlers'][n]:
            lg.addHandler(h)


def make_fake_open(secret_path: str, secret_content: str):
    import builtins, io, os
    real_open = builtins.open

    def fake_open(path, mode='r', encoding=None, *args, **kwargs):
        if os.path.normpath(path) == os.path.normpath(secret_path):
            return io.StringIO(secret_content)
        return real_open(path, mode, encoding=encoding, *args, **kwargs)

    return fake_open


def create_memes(session, items, model=None):
    """Create multiple model instances from dicts; defaults to Meme if model not provided."""
    if model is None:
        from llm_memedescriber.models import Meme
        model = Meme
    objs = [model(**i) for i in items]
    session.add_all(objs)
    return objs


def set_caplog_level(caplog, level, logger=None):
    if logger:
        caplog.set_level(level, logger=logger)
    else:
        caplog.set_level(level)

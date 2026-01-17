import os
import shutil
from whoosh.filedb.filestore import FileStorage

import pytest

from llm_memedescriber import search
from llm_memedescriber.models import Meme
from tests._helpers import create_in_memory_session


@pytest.fixture(autouse=True)
def ensure_filestorage_has_has_index(monkeypatch):
    class FSWrapper:
        def __init__(self, path):
            self._path = str(path)
            self._fs = FileStorage(self._path)
        def has_index(self):
            if hasattr(self._fs, 'index_exists'):
                return self._fs.index_exists()
            try:
                self._fs.open_index()
                return True
            except Exception:
                return False
        def create_index(self, schema):
            return self._fs.create_index(schema)
        def open_index(self):
            return self._fs.open_index()
    monkeypatch.setattr(search, 'FileStorage', FSWrapper)
    yield


def test_get_schema_has_expected_fields():
    schema = search.get_schema()
    fields = set(schema.names())
    assert 'id' in fields
    assert 'filename' in fields
    assert 'description' in fields
    assert 'category' in fields
    assert 'keywords' in fields


def test_init_index_creates_and_reopens(tmp_path, monkeypatch):
    idx = tmp_path / 'whooshidx'
    monkeypatch.setattr(search, 'INDEX_DIR', str(idx))

    search.init_index()
    storage = search.FileStorage(str(idx))
    assert storage.has_index()

    search.init_index()




def test_rebuild_index_indexes_memes(tmp_path):
    idx = tmp_path / 'whooshidx'
    if idx.exists():
        shutil.rmtree(idx)
    os.makedirs(idx, exist_ok=True)

    memes = [Meme(filename='f1.png', description='findme one', status='filled'),
             Meme(filename='f2.png', description='other', status='filled')]

    import llm_memedescriber.search as s_mod
    s_mod.INDEX_DIR = str(idx)

    # use create_in_memory_session helper to ensure cleanup
    with create_in_memory_session() as sess:
        for m in memes:
            sess.add(m)
        sess.commit()
        eng = sess.get_bind()
        search.rebuild_index(eng)

    res = search.search_memes('findme')
    assert any('findme' in (m['description'] or '') or m['filename'] == 'f1.png' for m in res)


def test_rebuild_index_handles_rmtree_failure(tmp_path, monkeypatch):
    idx = tmp_path / 'whooshidx'
    os.makedirs(idx, exist_ok=True)

    monkeypatch.setattr(shutil, 'rmtree', lambda p: (_ for _ in ()).throw(RuntimeError('boom')))

    with create_in_memory_session() as sess:
        # no memes to add
        eng = sess.get_bind()
        search.rebuild_index(eng)


def test_search_memes_short_query_returns_empty():
    assert search.search_memes('a') == []


def test_search_memes_no_index_returns_empty(tmp_path, monkeypatch):
    idx = tmp_path / 'whooshidx'
    monkeypatch.setattr(search, 'INDEX_DIR', str(idx))
    if idx.exists():
        shutil.rmtree(idx)

    assert search.search_memes('querytext') == []


def test_search_memes_parse_error_fallback(tmp_path, monkeypatch):
    idx = tmp_path / 'whooshidx'
    monkeypatch.setattr(search, 'INDEX_DIR', str(idx))

    memes = [Meme(filename='f1.png', description='alpha beta', status='filled')]
    import llm_memedescriber.search as s_mod
    s_mod.INDEX_DIR = str(idx)

    with create_in_memory_session() as sess:
        for m in memes:
            sess.add(m)
        sess.commit()
        eng = sess.get_bind()
        search.rebuild_index(eng)

    class BadParser:
        def __init__(self, *args, **kwargs):
            pass
        def parse(self, text):
            raise RuntimeError('parse fail')

    monkeypatch.setattr(search, 'QueryParser', BadParser)

    res = search.search_memes('alpha')
    assert len(res) >= 1
    assert any(r['filename'] == 'f1.png' for r in res)


def test_search_memes_top_level_exception_returns_empty(monkeypatch, tmp_path):
    idx = tmp_path / 'whooshidx'
    monkeypatch.setattr(search, 'INDEX_DIR', str(idx))

    class BadStorage(search.FileStorage):
        def open_index(self):
            raise RuntimeError('broken')

    monkeypatch.setattr(search, 'FileStorage', BadStorage)

    assert search.search_memes('anything') == []


def test_add_and_remove_meme_to_index(tmp_path):
    idx = tmp_path / 'whooshidx'
    search.INDEX_DIR = str(idx)

    meme = Meme(id=1234, filename='x.png', description='desc', status='filled')
    search.add_meme_to_index(meme)

    res = search.search_memes('desc')
    assert any(r['filename'] == 'x.png' for r in res)
    
    search.remove_meme_from_index(1234)
    res2 = search.search_memes('desc')
    assert not any(r.get('id') == 1234 for r in res2)


def test_rebuild_index_writer_cancel_on_exception(monkeypatch, caplog):
    caplog.set_level('ERROR')
    called = {}

    class FakeWriter:
        def __init__(self):
            self.cancel_called = False
            self.added = []
        def add_document(self, **kwargs):
            self.added.append(kwargs)
        def commit(self):
            pass
        def cancel(self):
            self.cancel_called = True

    class FakeIndex:
        def writer(self):
            w = FakeWriter()
            called['writer'] = w
            return w

    class FakeFS:
        def __init__(self, path):
            pass
        def create_index(self, schema):
            return FakeIndex()

    monkeypatch.setattr(search, 'FileStorage', FakeFS)

    from contextlib import contextmanager
    @contextmanager
    def fake_scope(engine):
        class BadSession:
            def exec(self, *args, **kwargs):
                raise RuntimeError('db fail')
        yield BadSession()

    monkeypatch.setattr(search, 'session_scope', fake_scope)

    with pytest.raises(RuntimeError):
        search.rebuild_index(object())

    assert 'writer' in called and called['writer'].cancel_called is True
    assert any('Failed to rebuild index' in r.message for r in caplog.records)


def test_add_and_remove_meme_uses_open_index(tmp_path):
    idx = tmp_path / 'whooshidx'
    import llm_memedescriber.search as s_mod
    s_mod.INDEX_DIR = str(idx)
    s_mod.init_index()

    meme = Meme(id=5555, filename='open.png', description='open-index', status='filled')

    search.add_meme_to_index(meme)
    res = search.search_memes('open-index')
    assert any(r['filename'] == 'open.png' for r in res)

    search.remove_meme_from_index(5555)
    res2 = search.search_memes('open-index')
    assert not any(r.get('id') == 5555 for r in res2)


def test_add_meme_to_index_handles_storage_exception(monkeypatch, tmp_path, caplog):
    class BadFS:
        def __init__(self, path):
            pass
        def has_index(self):
            return False
        def create_index(self, schema):
            raise RuntimeError('create fail')
    monkeypatch.setattr(search, 'FileStorage', BadFS)

    meme = Meme(id=999, filename='bad.png', description='x', status='filled')
    caplog.set_level('WARNING')
    search.add_meme_to_index(meme)
    assert any('Failed to add meme to index' in r.message for r in caplog.records)


def test_remove_meme_from_index_handles_writer_exception(monkeypatch, tmp_path, caplog):
    class BadWriter:
        def delete_by_term(self, name, val):
            raise RuntimeError('delete fail')
        def commit(self):
            raise RuntimeError('commit fail')
    class FakeIndex:
        def writer(self):
            return BadWriter()
    class BadFS:
        def __init__(self, path):
            pass
        def open_index(self):
            return FakeIndex()
    monkeypatch.setattr(search, 'FileStorage', BadFS)

    caplog.set_level('WARNING')
    search.remove_meme_from_index(42)
    # With new try/except implementation, exceptions are caught and logged as warning
    assert any('Failed to remove meme from index' in r.message for r in caplog.records)


def test_search_memes_handles_non_int_id(monkeypatch, tmp_path, caplog):
    class FakeResult(dict):
        def __init__(self):
            super().__init__({'id': 'not-an-int'})
        @property
        def score(self):
            return 1.0
    class FakeSearcher:
        def search(self, query, limit=None):
            return [FakeResult()]
        def close(self):
            pass
    class FakeIndex:
        def searcher(self):
            return FakeSearcher()
    class BadFS:
        def __init__(self, path):
            pass
        def has_index(self):
            return True
        def open_index(self):
            return FakeIndex()
    monkeypatch.setattr(search, 'FileStorage', BadFS)

    caplog.set_level('ERROR')
    res = search.search_memes('anything')
    assert res == []
    assert any('Search failed' in r.message for r in caplog.records)

from sqlmodel import Session
import os

from llm_memedescriber.db import init_db, get_meme_by_filename, get_stats
from llm_memedescriber.models import Meme


def test_init_db_creates_file_and_tables(tmp_path):
    db_file = tmp_path / "sub" / "test.db"
    url = f"sqlite:///{db_file}"

    eng = init_db(url)
    assert eng is not None

    assert db_file.exists()

    with Session(eng) as s:
        m = Meme(filename='f.db')
        s.add(m)
        s.commit()

    eng.dispose()


def test_get_meme_by_filename_returns_expected(in_memory_session: Session):
    s = in_memory_session
    m = Meme(filename='findme.png', description='desc')
    s.add(m)
    s.commit()

    found = get_meme_by_filename(s, 'findme.png')
    assert found is not None
    assert found.filename == 'findme.png'

    not_found = get_meme_by_filename(s, 'noexist.png')
    assert not_found is None


def test_get_stats_computes_aggregates(in_memory_session: Session):
    s = in_memory_session
    items = [
        Meme(filename='a.png', status='filled'),
        Meme(filename='b.png', status='filled'),
        Meme(filename='c.png', status='pending'),
        Meme(filename='d.png', status='failed'),
        Meme(filename='e.png', status='unsupported'),
    ]
    for m in items:
        s.add(m)
    s.commit()

    stats = get_stats(s)
    assert stats['total'] == 5
    assert stats['filled'] == 2
    assert stats['pending'] == 1
    assert stats['failed'] == 1
    assert stats['unsupported'] == 1
    # completion percent = 2/5 = 40.0
    assert stats['completion_percent'] == 40.0


def test_get_stats_zero_total_returns_zero(in_memory_session: Session):
    s = in_memory_session
    stats = get_stats(s)
    assert stats['total'] == 0
    assert stats['completion_percent'] == 0


def test_init_db_pragmas_failure_is_handled(monkeypatch, tmp_path, caplog):
    import llm_memedescriber.db as dbmod
    caplog.set_level('DEBUG')

    real_create = dbmod.create_engine

    def fake_create_engine(url, **kwargs):
        eng = real_create(url, **kwargs)
        original_connect = eng.connect
        state = {'first': True}
        def bad_connect():
            if state['first']:
                state['first'] = False
                class Ctx:
                    def __enter__(self):
                        raise RuntimeError('connect fail')
                    def __exit__(self, exc_type, exc, tb):
                        return False
                return Ctx()
            return original_connect()
        eng.connect = bad_connect
        return eng

    monkeypatch.setattr(dbmod, 'create_engine', fake_create_engine)

    engine = dbmod.init_db(f"sqlite:///{tmp_path/'x.db'}")
    assert engine is not None
    assert any('Unable to set SQLite pragmas' in r.message for r in caplog.records)


def test_init_db_handles_os_path_exists_exception(monkeypatch, tmp_path):
    import llm_memedescriber.db as dbmod
    db_file = tmp_path / 'sub' / 'test2.db'
    dirpath = str(db_file.parent)
    os.makedirs(dirpath, exist_ok=True)

    orig_exists = os.path.exists
    def bad_exists(p):
        if os.path.normpath(str(p)) == os.path.normpath(dirpath):
            raise RuntimeError('boom')
        return orig_exists(p)

    monkeypatch.setattr(os.path, 'exists', bad_exists)

    eng = dbmod.init_db(f"sqlite:///{db_file}")
    assert eng is not None
    eng.dispose()

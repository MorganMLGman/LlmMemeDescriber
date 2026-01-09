import logging
import pytest
from sqlmodel import SQLModel, Session, Field, create_engine
from typing import Optional

import llm_memedescriber.db_helpers as db_helpers
from llm_memedescriber.db_helpers import session_scope


def test_session_scope_logs_open_and_close(caplog):
    caplog.set_level(logging.DEBUG)
    engine = create_engine("sqlite:///:memory:")

    with session_scope(engine) as sess:
        assert sess is not None

    messages = "\n".join(r.getMessage() for r in caplog.records)
    assert "Opening DB session" in messages
    assert "Closed DB session" in messages


def test_session_scope_closes_even_on_error(caplog):
    caplog.set_level(logging.DEBUG)
    engine = create_engine("sqlite:///:memory:")

    with pytest.raises(RuntimeError):
        with session_scope(engine) as sess:
            raise RuntimeError("boom")

    messages = "\n".join(r.getMessage() for r in caplog.records)
    assert "Opening DB session" in messages
    assert "Closed DB session" in messages


def test_session_scope_handles_close_exception(monkeypatch, caplog):
    caplog.set_level(logging.ERROR)

    class BadSession:
        def __init__(self, engine):
            self._engine = engine

        def close(self):
            raise RuntimeError("close failed")

    monkeypatch.setattr(db_helpers, "Session", BadSession)

    engine = object()

    with session_scope(engine) as sess:
        assert isinstance(sess, BadSession)

    messages = "\n".join(r.getMessage() for r in caplog.records)
    assert "Failed to close DB session" in messages


class TestItem(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str


def test_session_scope_commits_and_persists(tmp_path):
    db_file = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_file}")
    SQLModel.metadata.create_all(engine)

    with session_scope(engine) as sess:
        item = TestItem(name="persisted")
        sess.add(item)
        sess.commit()

    # verify using a new session
    with Session(engine) as s:
        rows = s.exec(SQLModel.metadata.tables["testitem"].select()).all()
        # There should be at least one row
        assert len(rows) >= 1


def test_session_scope_does_not_persist_without_commit(tmp_path):
    db_file = tmp_path / "test2.db"
    engine = create_engine(f"sqlite:///{db_file}")
    SQLModel.metadata.create_all(engine)

    with pytest.raises(RuntimeError):
        with session_scope(engine) as sess:
            item = TestItem(name="notpersisted")
            sess.add(item)
            # don't commit; raise an error to exit
            raise RuntimeError("boom")

    # verify nothing was persisted
    with Session(engine) as s:
        rows = s.exec(SQLModel.metadata.tables["testitem"].select()).all()
        assert len(rows) == 0
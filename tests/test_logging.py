import logging

import pytest

from llm_memedescriber.config import Settings, configure_logging


def _snapshot_logging():
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


def _restore_logging(snapshot):
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


def test_configure_logging_unknown_level_defaults_to_info():
    snap = _snapshot_logging()
    try:
        logging.getLogger().handlers[:] = []
        s = Settings(logging_level="NOT_A_LEVEL")
        configure_logging(s)
        assert logging.getLogger().level == logging.INFO
    finally:
        _restore_logging(snap)


def test_configure_logging_debug_sets_dependent_loggers():
    snap = _snapshot_logging()
    try:
        logging.getLogger().handlers[:] = []
        s = Settings(logging_level="DEBUG")
        configure_logging(s)
        assert logging.getLogger('alembic').level == logging.DEBUG
        assert logging.getLogger('alembic.runtime').level == logging.DEBUG
        assert logging.getLogger('google_genai').level == logging.DEBUG
        assert logging.getLogger('google_genai.models').level == logging.DEBUG
    finally:
        _restore_logging(snap)


def test_uvicorn_handlers_cleared_and_propagate_set():
    snap = _snapshot_logging()
    try:
        uv = logging.getLogger('uvicorn')
        h = logging.StreamHandler()
        uv.addHandler(h)
        uv.error = uv.getChild('error')

        s = Settings(logging_level="INFO")
        configure_logging(s)

        assert logging.getLogger('uvicorn').handlers == []
        assert logging.getLogger('uvicorn').propagate is True
    finally:
        _restore_logging(snap)


def test_localisoformatter_with_valid_and_invalid_tz():
    from llm_memedescriber.config import LocalISOFormatter

    class R:
        created = 1673000000.0

    import re
    f = LocalISOFormatter(tz_name="UTC")
    s = f.formatTime(R())
    assert "T" in s
    assert re.search(r"[+-]\d{2}:\d{2}$", s) or s.endswith("Z")

    f2 = LocalISOFormatter(tz_name="NoSuchTimeZone")
    s2 = f2.formatTime(R())
    assert isinstance(s2, str)
    
    f3 = LocalISOFormatter(tz_name=None)
    s3 = f3.formatTime(R())
    assert isinstance(s3, str)

    f4 = LocalISOFormatter(tz_name="America/New_York")
    s4 = f4.formatTime(R())
    assert "T" in s4
    assert re.search(r"[+-]\d{2}:\d{2}$", s) or s.endswith("Z")

def test_configure_logging_does_not_add_duplicate_handlers():
    snap = _snapshot_logging()
    try:
        root = logging.getLogger()
        root.handlers[:] = []
        s = Settings(logging_level="INFO")
        configure_logging(s)
        assert len(root.handlers) == 1
        configure_logging(s)
        assert len(root.handlers) == 1
    finally:
        _restore_logging(snap)


def test_configure_logging_sets_noisy_loggers_to_warning():
    snap = _snapshot_logging()
    try:
        logging.getLogger().handlers[:] = []
        s = Settings(logging_level="INFO")
        configure_logging(s)
        for n in ['httpx', 'httpcore', 'webdav4', 'urllib3']:
            assert logging.getLogger(n).level == logging.WARNING
    finally:
        _restore_logging(snap)


def test_configure_logging_with_none_settings_defaults_info():
    snap = _snapshot_logging()
    try:
        logging.getLogger().handlers[:] = []
        configure_logging(None)
        assert logging.getLogger().level == logging.INFO
        assert len(logging.getLogger().handlers) == 1
    finally:
        _restore_logging(snap)


def test_uvicorn_error_handlers_cleared():
    snap = _snapshot_logging()
    try:
        err = logging.getLogger('uvicorn.error')
        err.addHandler(logging.StreamHandler())
        err.propagate = False

        s = Settings(logging_level="INFO")
        configure_logging(s)

        assert logging.getLogger('uvicorn.error').handlers == []
        assert logging.getLogger('uvicorn.error').propagate is True
    finally:
        _restore_logging(snap)


def test_uvicorn_handlers_idempotent():
    snap = _snapshot_logging()
    try:
        u = logging.getLogger('uvicorn')
        ue = logging.getLogger('uvicorn.error')
        u.addHandler(logging.StreamHandler())
        ue.addHandler(logging.StreamHandler())

        s = Settings(logging_level="INFO")
        configure_logging(s)
        assert logging.getLogger('uvicorn').handlers == []
        assert logging.getLogger('uvicorn.error').handlers == []

        configure_logging(s)
        assert logging.getLogger('uvicorn').handlers == []
        assert logging.getLogger('uvicorn.error').handlers == []
    finally:
        _restore_logging(snap)


def test_uvicorn_level_unchanged_by_configure():
    snap = _snapshot_logging()
    try:
        u = logging.getLogger('uvicorn')
        u.setLevel(logging.DEBUG)
        s = Settings(logging_level="INFO")
        configure_logging(s)
        assert logging.getLogger('uvicorn').level == logging.DEBUG
    finally:
        _restore_logging(snap)

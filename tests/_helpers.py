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


def load_test_image(name: str) -> bytes:
    path = DATA_DIR / name
    with open(path, "rb") as f:
        return f.read()


def mask_ones_val(k: int) -> int:
    if k <= 0:
        return 0
    return (1 << k) - 1


def hex_from_val(val: int) -> str:
    return f"{val & ((1 << 64) - 1):016x}"


def hex_ones(k: int, shift: int = 0) -> str:
    val = mask_ones_val(k) << shift
    return hex_from_val(val)


class FakeStorage:
    def __init__(self, fail_on=None):
        self.deleted = []
        self.fail_on = set(fail_on or [])

    def delete_file(self, name):
        if name in self.fail_on:
            raise RuntimeError("storage failure")
        self.deleted.append(name)

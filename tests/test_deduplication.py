"""Tests for deduplication functionality (phash, grouping, merging).

This file intentionally keeps all deduplication-related tests in one place
for readability and easier local runs. Helper utilities are defined here to
avoid scattering small helpers across multiple files.
"""

import os
import logging
import numbers
import pytest
from contextlib import contextmanager
from sqlmodel import SQLModel, create_engine, Session, select

from llm_memedescriber.deduplication import (
    calculate_phash,
    hamming_distance,
    find_duplicate_groups,
    mark_false_positive,
    merge_duplicates,
    add_pair_exception,
    remove_pair_exception,
    list_pair_exceptions,
)
from llm_memedescriber.models import Meme, Duplicate, DuplicateGroup, MemeDuplicateGroup
from llm_memedescriber.constants import DUPLICATE_THRESHOLD

from tests._helpers import (
    create_in_memory_session,
    load_test_image,
    hex_ones,
    hex_from_val,
    DATA_DIR,
    FakeStorage,
)


def test_calculate_phash_returns_hex_for_rgb_image():
    data = load_test_image("rgb.png")
    phash = calculate_phash(data)
    assert phash is not None
    assert isinstance(phash, str)
    assert len(phash) == 16


def test_calculate_phash_handles_alpha_channel():
    data = load_test_image("rgba.png")
    phash = calculate_phash(data)
    assert phash is not None


def test_calculate_phash_handles_paletted_images():
    phash = calculate_phash(load_test_image("paletted.png"))
    assert phash is not None


def test_calculate_phash_is_same_for_identical_images():
    data1 = load_test_image("rgb.png")
    data2 = load_test_image("rgb.png")
    
    phash1 = calculate_phash(data1)
    phash2 = calculate_phash(data2)
    assert phash1 == phash2


def test_calculate_phash_returns_none_on_invalid_input():
    assert calculate_phash(b"") is None  # empty
    assert calculate_phash(b"12345") is None  # too small
    assert calculate_phash(b"not an image" * 20) is None  # corrupted


def test_calculate_phash_differs_for_different_images():
    data1 = load_test_image("rgb.png")
    data2 = load_test_image("rgb_variant2.png")

    phash1 = calculate_phash(data1)
    phash2 = calculate_phash(data2)
    assert phash1 != phash2


def test_calculate_phash_handles_grayscale_image():
    data = load_test_image("grayscale.png")
    phash = calculate_phash(data)
    assert phash is not None


def test_calculate_phash_handles_image_formats():
    files = os.listdir(DATA_DIR)
    for fname in files:
        path = os.path.join(DATA_DIR, fname)
        if not os.path.exists(path):
            Warning(f"Test image {fname} not found, skipping.")
            continue
        data = load_test_image(fname)
        phash = calculate_phash(data)
        assert phash is not None


def test_hamming_distance_zero_for_identical_hashes():
    phash = calculate_phash(load_test_image("rgb.png"))
    assert phash is not None
    assert hamming_distance(phash, phash) == 0


def test_hamming_distance_positive_for_different_images():
    phash1 = calculate_phash(load_test_image("rgb.png"))
    phash2 = calculate_phash(load_test_image("rgb_variant2.png"))
    assert phash1 is not None and phash2 is not None
    d = hamming_distance(phash1, phash2)
    assert d > 0 and d <= 64


def test_hamming_distance_returns_999_for_empty_or_none():
    assert hamming_distance("", "") == 999
    assert hamming_distance(None, "abc") == 999
    assert hamming_distance("abc", None) == 999


def test_hamming_distance_returns_999_on_invalid_hex():
    assert hamming_distance("zzzz", "abcd") == 999


def test_hamming_distance_is_symmetric():
    phash1 = calculate_phash(load_test_image("rgb.png"))
    phash2 = calculate_phash(load_test_image("rgb_variant2.png"))
    assert phash1 is not None and phash2 is not None
    d1 = hamming_distance(phash1, phash2)
    d2 = hamming_distance(phash2, phash1)
    assert d1 == d2


def test_hamming_distance_max_distance():
    phash1 = "0000000000000000"
    phash2 = "ffffffffffffffff"
    d = hamming_distance(phash1, phash2)
    assert d == 64


def test_hamming_distance_invalid_length():
    phash1 = "1234"
    phash2 = "abcd"
    d = hamming_distance(phash1, phash2)
    assert d == 999

def test_hamming_distance_not_str():
    d = hamming_distance(1234567890, "abcd")
    assert d == 999
    
    d = hamming_distance("abcd", 123.456)
    assert d == 999
    
    phash1 = "00000000000000.1"
    phash2 = "ffffffffffffffff"
    d = hamming_distance(phash1, phash2)
    assert d == 999


def test_find_duplicate_groups_detects_similar_memes():
    T = DUPLICATE_THRESHOLD
    small_k = 1 if T >= 1 else 0
    far_k = min(64, T + 1)

    with create_in_memory_session() as session:
        a = Meme(filename="a.png", phash=hex_ones(0))
        b = Meme(filename="b.png", phash=hex_ones(small_k))
        c = Meme(filename="c.png", phash=hex_ones(64))
        session.add_all([a, b, c])
        session.commit()

        # sanity checks based on dynamic threshold
        assert hamming_distance(a.phash, b.phash) <= T
        assert hamming_distance(a.phash, c.phash) > T

        groups = find_duplicate_groups(session)
        groups_sets = [set(m.filename for m in g) for g in groups]
        assert {"a.png", "b.png"} in groups_sets
        assert not any("c.png" in s for s in groups_sets)


def test_find_duplicate_groups_transitive_not_merged():
    T = DUPLICATE_THRESHOLD
    with create_in_memory_session() as session:
        a_ph = hex_ones(0)
        b_ph = hex_ones(1)  # one-bit diff
        c_ph = hex_ones(T + 1)  # includes b's bit; d(a,c) = T+1 > T, d(b,c) = T <= T

        m1 = Meme(filename="ta.png", phash=a_ph)
        m2 = Meme(filename="tb.png", phash=b_ph)
        m3 = Meme(filename="tc.png", phash=c_ph)
        session.add_all([m1, m2, m3])
        session.commit()

        assert hamming_distance(a_ph, b_ph) <= T
        assert hamming_distance(b_ph, c_ph) <= T
        assert hamming_distance(a_ph, c_ph) > T

        groups = find_duplicate_groups(session)
        groups_sets = [set(m.filename for m in g) for g in groups]
        assert {"ta.png", "tb.png"} in groups_sets
        assert not any("tc.png" in s for s in groups_sets)


def test_find_duplicate_groups_multiple_groups():
    T = DUPLICATE_THRESHOLD
    with create_in_memory_session() as session:
        # group1: low bits
        a = Meme(filename="g1_a.png", phash=hex_ones(0))
        b = Meme(filename="g1_b.png", phash=hex_ones(1))
        # group2: high bits (shifted) with T+1 ones -> far from group1
        shift = 64 - (T + 1)
        c = Meme(filename="g2_c.png", phash=hex_ones(T + 1, shift=shift))
        d_val = (int(c.phash, 16) ^ 1)  # flip one low bit of c -> distance 1
        d = Meme(filename="g2_d.png", phash=hex_from_val(d_val))

        session.add_all([a, b, c, d])
        session.commit()

        assert hamming_distance(a.phash, b.phash) <= T
        assert hamming_distance(c.phash, d.phash) <= T
        assert hamming_distance(a.phash, c.phash) > T

        groups = find_duplicate_groups(session)
        groups_sets = [set(m.filename for m in g) for g in groups]
        assert {"g1_a.png", "g1_b.png"} in groups_sets
        assert {"g2_c.png", "g2_d.png"} in groups_sets


def test_find_duplicate_groups_pair_exception_with_alternate_partner():
    T = DUPLICATE_THRESHOLD
    with create_in_memory_session() as session:
        a = Meme(filename="ex_a.png", phash=hex_ones(0))
        b = Meme(filename="ex_b.png", phash=hex_ones(1))
        c = Meme(filename="ex_c.png", phash=hex_ones(2))
        session.add_all([a, b, c])
        session.commit()

        # sanity
        assert hamming_distance(a.phash, b.phash) <= T
        assert hamming_distance(a.phash, c.phash) <= T

        # add pairwise exception between a and b
        dup = Duplicate(filename_a="ex_a.png", filename_b="ex_b.png", is_false_positive=True)
        session.add(dup)
        session.commit()

        groups = find_duplicate_groups(session)
        groups_sets = [set(m.filename for m in g) for g in groups]
        assert {"ex_a.png", "ex_c.png"} in groups_sets
        assert not any("ex_b.png" in s for s in groups_sets)


def test_find_duplicate_groups_includes_false_positive_flagged_memes():
    with create_in_memory_session() as session:
        m1 = Meme(filename="fp_a.png", phash="0000000000000000", is_false_positive=True)
        m2 = Meme(filename="fp_b.png", phash="0000000000000001")
        session.add_all([m1, m2])
        session.commit()

        groups = find_duplicate_groups(session)
        groups_sets = [set(m.filename for m in g) for g in groups]
        assert {"fp_a.png", "fp_b.png"} in groups_sets


def test_find_duplicate_groups_no_duplicates():
    with create_in_memory_session() as session:
        a = Meme(filename="solo_a.png", phash=hex_ones(0))
        b = Meme(filename="solo_b.png", phash=hex_ones(64))
        session.add_all([a, b])
        session.commit()

        assert hamming_distance(a.phash, b.phash) > DUPLICATE_THRESHOLD

        groups = find_duplicate_groups(session)
        assert len(groups) == 0


def test_find_duplicate_groups_empty_db():
    with create_in_memory_session() as session:
        groups = find_duplicate_groups(session)
        assert len(groups) == 0


def test_find_duplicate_groups_single_meme():
    with create_in_memory_session() as session:
        a = Meme(filename="only.png", phash=hex_ones(0))
        session.add(a)
        session.commit()

        groups = find_duplicate_groups(session)
        assert len(groups) == 0


def test_find_duplicate_groups_no_phashes():
    with create_in_memory_session() as session:
        a = Meme(filename="nophash1.png", phash=None)
        b = Meme(filename="nophash2.png", phash=None)
        session.add_all([a, b])
        session.commit()

        groups = find_duplicate_groups(session)
        assert len(groups) == 0


def test_find_duplicate_groups_ignores_memes_without_phash():
    T = DUPLICATE_THRESHOLD
    with create_in_memory_session() as session:
        a = Meme(filename="nophash_a.png", phash=None)
        b = Meme(filename="nophash_b.png", phash=hex_ones(0))
        c = Meme(filename="nophash_c.png", phash=hex_ones(T - 1))
        session.add_all([a, b, c])
        session.commit()

        groups = find_duplicate_groups(session)
        groups_sets = [set(m.filename for m in g) for g in groups]
        assert not any("nophash_a.png" in s for s in groups_sets)
        assert {"nophash_b.png", "nophash_c.png"} in groups_sets


def test_mark_false_positive_returns_false_for_missing():
    with create_in_memory_session() as session:
        assert mark_false_positive(session, "no_such_file.png") is False


def test_mark_false_positive_sets_flag_and_removes_links():
    with create_in_memory_session() as session:
        m = Meme(filename="mark_me.png", phash=hex_ones(0))
        session.add(m)
        link1 = MemeDuplicateGroup(group_id=1, filename="mark_me.png")
        link2 = MemeDuplicateGroup(group_id=2, filename="mark_me.png")
        other_link = MemeDuplicateGroup(group_id=3, filename="other.png")
        session.add_all([link1, link2, other_link])
        session.commit()

        orig = session.exec(select(Meme).where(Meme.filename == "mark_me.png")).first()
        assert orig is not None
        orig_updated = orig.updated_at

        res = mark_false_positive(session, "mark_me.png")
        assert res is True

        m_after = session.exec(select(Meme).where(Meme.filename == "mark_me.png")).first()
        assert m_after.is_false_positive is True
        assert m_after.updated_at is not None
        assert m_after.updated_at >= orig_updated
        remaining = session.exec(select(MemeDuplicateGroup).where(MemeDuplicateGroup.filename == "mark_me.png")).all()
        assert remaining == []

        other_remaining = session.exec(select(MemeDuplicateGroup).where(MemeDuplicateGroup.filename == "other.png")).all()
        assert len(other_remaining) == 1


def test_mark_false_positive_handles_link_removal_exception(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG, logger='llm_memedescriber.deduplication')
    with create_in_memory_session() as session:
        m = Meme(filename="err_me.png", phash=hex_ones(0))
        link = MemeDuplicateGroup(group_id=1, filename="err_me.png")
        session.add_all([m, link])
        session.commit()

        original_exec = session.exec

        import llm_memedescriber.deduplication as dedup
        called = {}
        def fake_debug(msg, *args, **kwargs):
            called['msg'] = msg
        monkeypatch.setattr(dedup.logger, 'debug', fake_debug)

        original_delete = session.delete
        def fake_delete(obj):
            if getattr(obj, 'filename', None) == 'err_me.png':
                raise RuntimeError("boom during delete")
            return original_delete(obj)

        monkeypatch.setattr(session, "delete", fake_delete)

        res = mark_false_positive(session, "err_me.png")
        assert res is True

        m_after = original_exec(select(Meme).where(Meme.filename == "err_me.png")).first()
        assert m_after is not None
        assert m_after.is_false_positive is True

        assert 'msg' in called and "Failed to remove meme-group links for false-positive marking" in called['msg']




def test_merge_duplicates_primary_missing_returns_false():
    with create_in_memory_session() as session:
        storage = FakeStorage()
        assert merge_duplicates(session, storage, "nope.png", ["a.png"]) is False


def test_merge_duplicates_no_duplicates_returns_false():
    with create_in_memory_session() as session:
        primary = Meme(filename="p.png", phash=hex_ones(0))
        session.add(primary)
        session.commit()

        storage = FakeStorage()
        assert merge_duplicates(session, storage, "p.png", ["missing.png"]) is False


def test_merge_duplicates_merges_metadata_and_deletes():
    with create_in_memory_session() as session:
        primary = Meme(filename="prim.png", phash=hex_ones(0), keywords="k1", description="desc1")
        d1 = Meme(filename="dup1.png", phash=hex_ones(0), keywords="k2", description="desc2")
        d2 = Meme(filename="dup2.png", phash=hex_ones(0), keywords="k3", description="desc3")
        session.add_all([primary, d1, d2])
        session.commit()

        storage = FakeStorage()
        res = merge_duplicates(session, storage, "prim.png", ["dup1.png", "dup2.png"], merge_metadata=True)
        assert res is True

        p = session.exec(select(Meme).where(Meme.filename == "prim.png")).first()
        assert p is not None
        assert "k1" in (p.keywords or "")
        assert "k2" in (p.keywords or "")
        assert "k3" in (p.keywords or "")
        assert "desc2" in (p.description or "")
        assert "desc3" in (p.description or "")

        assert session.exec(select(Meme).where(Meme.filename == "dup1.png")).first() is None
        assert session.exec(select(Meme).where(Meme.filename == "dup2.png")).first() is None
        assert "dup1.png" in storage.deleted and "dup2.png" in storage.deleted


def test_merge_duplicates_respects_metadata_sources():
    with create_in_memory_session() as session:
        primary = Meme(filename="prim2.png", phash=hex_ones(0), keywords=None, description=None)
        d1 = Meme(filename="dupA.png", phash=hex_ones(0), keywords="ka", description="da")
        d2 = Meme(filename="dupB.png", phash=hex_ones(0), keywords="kb", description="db")
        session.add_all([primary, d1, d2])
        session.commit()

        storage = FakeStorage()
        res = merge_duplicates(session, storage, "prim2.png", ["dupA.png", "dupB.png"], merge_metadata=True, metadata_sources=["dupA.png"])
        assert res is True

        p = session.exec(select(Meme).where(Meme.filename == "prim2.png")).first()
        assert p is not None
        assert "ka" in (p.keywords or "")
        assert "kb" not in (p.keywords or "")


def test_merge_duplicates_storage_delete_exception_is_logged(caplog):
    caplog.set_level(logging.WARNING)
    with create_in_memory_session() as session:
        primary = Meme(filename="p3.png", phash=hex_ones(0))
        dup = Meme(filename="bad.png", phash=hex_ones(0))
        session.add_all([primary, dup])
        session.commit()

        storage = FakeStorage(fail_on={"bad.png"})
        res = merge_duplicates(session, storage, "p3.png", ["bad.png"])
        assert res is True

        assert any('Failed to delete bad.png' in r.getMessage() for r in caplog.records)


def test_merge_duplicates_cleans_up_groups():
    with create_in_memory_session() as session:
        g = DuplicateGroup()
        session.add(g)
        session.commit()

        primary = Meme(filename="gprim.png", phash=hex_ones(0))
        dup1 = Meme(filename="gdup1.png", phash=hex_ones(0))
        dup2 = Meme(filename="gdup2.png", phash=hex_ones(0))
        session.add_all([primary, dup1, dup2])
        session.commit()

        l1 = MemeDuplicateGroup(group_id=g.id, filename="gprim.png")
        l2 = MemeDuplicateGroup(group_id=g.id, filename="gdup1.png")
        l3 = MemeDuplicateGroup(group_id=g.id, filename="gdup2.png")
        session.add_all([l1, l2, l3])
        session.commit()

        storage = FakeStorage()
        res = merge_duplicates(session, storage, "gprim.png", ["gdup1.png"], merge_metadata=False)
        assert res is True

        remaining = session.exec(select(MemeDuplicateGroup).where(MemeDuplicateGroup.group_id == g.id)).all()
        assert len(remaining) == 2

        res2 = merge_duplicates(session, storage, "gprim.png", ["gdup2.png"], merge_metadata=False)
        assert res2 is True

        groups = session.exec(select(DuplicateGroup)).all()
        assert all(session.exec(select(MemeDuplicateGroup).where(MemeDuplicateGroup.group_id == g.id)).all() == [] for g in groups) or len(groups) == 0


def test_merge_duplicates_rolls_back_on_exception(monkeypatch):
    with create_in_memory_session() as session:
        primary = Meme(filename="rprim.png", phash=hex_ones(0))
        dup = Meme(filename="rdup.png", phash=hex_ones(0))
        session.add_all([primary, dup])
        session.commit()

        storage = FakeStorage()

        def bad_commit():
            raise RuntimeError("commit failed")
        monkeypatch.setattr(session, 'commit', bad_commit)

        called = {}
        def fake_rb():
            called['rb'] = True
        monkeypatch.setattr(session, 'rollback', fake_rb)

        res = merge_duplicates(session, storage, "rprim.png", ["rdup.png"], merge_metadata=False)
        assert res is False
        assert 'rb' in called


def test_list_pair_exceptions_empty():
    with create_in_memory_session() as session:
        res = list_pair_exceptions(session)
        assert isinstance(res, list)
        assert res == []


def test_list_pair_exceptions_returns_all():
    with create_in_memory_session() as session:
        d1 = Duplicate(filename_a="a.png", filename_b="b.png", is_false_positive=True)
        d2 = Duplicate(filename_a="c.png", filename_b="d.png", is_false_positive=False)
        session.add_all([d1, d2])
        session.commit()

        res = list_pair_exceptions(session)
        assert isinstance(res, list)
        assert len(res) == 2
        pairs = {(r.filename_a, r.filename_b, r.is_false_positive) for r in res}
        assert ("a.png", "b.png", True) in pairs
        assert ("c.png", "d.png", False) in pairs


def test_add_pair_exception_creates_new_duplicate():
    with create_in_memory_session() as session:
        dup = add_pair_exception(session, "a.png", "b.png")
        assert isinstance(dup, Duplicate)
        assert dup.is_false_positive is True

        found = session.exec(select(Duplicate).where((Duplicate.filename_a == "a.png") & (Duplicate.filename_b == "b.png"))).first()
        assert found is not None


def test_add_pair_exception_updates_existing_and_is_idempotent():
    with create_in_memory_session() as session:
        existing = Duplicate(filename_a="x.png", filename_b="y.png", is_false_positive=False)
        session.add(existing)
        session.commit()

        dup = add_pair_exception(session, "y.png", "x.png")
        assert dup.id == existing.id
        assert dup.is_false_positive is True

        dup2 = add_pair_exception(session, "x.png", "y.png")
        assert dup2.id == existing.id


def test_remove_pair_exception_returns_false_when_missing():
    with create_in_memory_session() as session:
        assert remove_pair_exception(session, "no.png", "no2.png") is False


def test_remove_pair_exception_deletes_and_returns_true_order_insensitive():
    with create_in_memory_session() as session:
        d = Duplicate(filename_a="r1.png", filename_b="r2.png", is_false_positive=True)
        session.add(d)
        session.commit()

        res = remove_pair_exception(session, "r2.png", "r1.png")
        assert res is True
        remaining = session.exec(select(Duplicate).where((Duplicate.filename_a == "r1.png") | (Duplicate.filename_b == "r1.png"))).all()
        assert remaining == []
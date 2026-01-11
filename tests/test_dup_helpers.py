import pytest
from sqlmodel import select

from llm_memedescriber.dup_helpers import (
    get_group_members,
    get_groups_for_filename,
    set_group_links,
    clear_group_links_for_filename,
)
from llm_memedescriber.models import MemeDuplicateGroup


def test_get_group_members_and_groups_for_filename_empty(in_memory_session):
    session = in_memory_session
    assert get_group_members(session, 1) == []
    assert get_groups_for_filename(session, "nope.png") == []


def test_set_group_links_and_getters(in_memory_session):
    session = in_memory_session
    set_group_links(session, 10, ["a.png", "b.png"])
    session.commit()

    members = get_group_members(session, 10)
    assert set(members) == {"a.png", "b.png"}

    groups = get_groups_for_filename(session, "a.png")
    assert 10 in groups


def test_clear_group_links_for_filename_removes_only_target(in_memory_session):
    session = in_memory_session
    set_group_links(session, 1, ["x.png", "y.png"])
    set_group_links(session, 2, ["x.png", "z.png"])
    session.commit()

    assert set(get_groups_for_filename(session, "x.png")) == {1, 2}

    clear_group_links_for_filename(session, "x.png")
    session.commit()

    assert get_groups_for_filename(session, "x.png") == []
    assert set(get_group_members(session, 2)) == {"z.png"}


def test_set_group_links_allows_duplicate_entries_and_clear_handles_none(in_memory_session):
    session = in_memory_session
    set_group_links(session, 3, ["dup.png", "dup.png"])
    set_group_links(session, 3, ["dup.png"])
    session.commit()

    members = get_group_members(session, 3)
    assert "dup.png" in members

    clear_group_links_for_filename(session, "dup.png")
    session.commit()
    assert get_group_members(session, 3) == []


def test_get_group_members_raises_on_non_int(in_memory_session):
    session = in_memory_session
    with pytest.raises(TypeError):
        get_group_members(session, "not-an-int")


def test_get_groups_for_filename_raises_on_non_str(in_memory_session):
    session = in_memory_session
    with pytest.raises(TypeError):
        get_groups_for_filename(session, None)


def test_set_group_links_raises_on_non_list_or_non_str_members(in_memory_session):
    session = in_memory_session
    with pytest.raises(TypeError):
        set_group_links(session, 1, "not-a-list")
    with pytest.raises(TypeError):
        set_group_links(session, 1, ["good.png", 123])


def test_clear_group_links_raises_on_non_str_filename(in_memory_session):
    session = in_memory_session
    with pytest.raises(TypeError):
        clear_group_links_for_filename(session, 123)



def test_set_group_links_with_empty_list_is_noop(in_memory_session):
    session = in_memory_session
    set_group_links(session, 4, [])
    session.commit()
    assert get_group_members(session, 4) == []


def test_get_group_members_and_groups_with_multiple_groups(in_memory_session):
    session = in_memory_session
    set_group_links(session, 5, ["m.png", "n.png"])
    set_group_links(session, 6, ["n.png", "o.png"])
    session.commit()

    assert set(get_group_members(session, 5)) == {"m.png", "n.png"}
    assert set(get_group_members(session, 6)) == {"n.png", "o.png"}
    assert set(get_groups_for_filename(session, "n.png")) == {5, 6}


def test_set_group_links_inserts_duplicate_rows(in_memory_session):
    session = in_memory_session
    set_group_links(session, 7, ["dup.png", "dup.png"])
    set_group_links(session, 7, ["dup.png"])
    session.commit()

    rows = session.exec(select(MemeDuplicateGroup).where(MemeDuplicateGroup.group_id == 7)).all()
    assert sum(1 for r in rows if r.filename == "dup.png") >= 2


def test_clear_group_links_noop_when_missing(in_memory_session):
    session = in_memory_session
    clear_group_links_for_filename(session, "missing.png")
    session.commit()
    assert get_groups_for_filename(session, "missing.png") == []

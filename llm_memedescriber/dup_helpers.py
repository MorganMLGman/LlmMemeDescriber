from typing import List
from sqlmodel import select

from .models import MemeDuplicateGroup


def get_group_members(session, group_id: int) -> List[str]:
    if not isinstance(group_id, int):
        raise TypeError("group_id must be an int")
    rows = session.exec(select(MemeDuplicateGroup).where(MemeDuplicateGroup.group_id == group_id)).all()
    return [r.filename for r in rows]


def get_groups_for_filename(session, filename: str) -> List[int]:
    if not isinstance(filename, str):
        raise TypeError("filename must be a str")
    rows = session.exec(select(MemeDuplicateGroup).where(MemeDuplicateGroup.filename == filename)).all()
    return [r.group_id for r in rows]


def clear_group_links_for_filename(session, filename: str) -> None:
    if not isinstance(filename, str):
        raise TypeError("filename must be a str")
    rows = session.exec(select(MemeDuplicateGroup).where(MemeDuplicateGroup.filename == filename)).all()
    for r in rows:
        session.delete(r)


def set_group_links(session, group_id: int, filenames: List[str]) -> None:
    if not isinstance(group_id, int):
        raise TypeError("group_id must be an int")
    if not isinstance(filenames, list):
        raise TypeError("filenames must be a list of strings")
    for fn in filenames:
        if not isinstance(fn, str):
            raise TypeError("each filename in filenames must be a str")
    for fn in filenames:
        session.add(MemeDuplicateGroup(group_id=group_id, filename=fn))

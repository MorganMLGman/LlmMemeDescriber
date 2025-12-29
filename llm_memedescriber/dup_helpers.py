from typing import List
from sqlmodel import select

from .models import MemeDuplicateGroup


def get_group_members(session, group_id: int) -> List[str]:
    rows = session.exec(select(MemeDuplicateGroup).where(MemeDuplicateGroup.group_id == group_id)).all()
    return [r.filename for r in rows]


def get_groups_for_filename(session, filename: str) -> List[int]:
    rows = session.exec(select(MemeDuplicateGroup).where(MemeDuplicateGroup.filename == filename)).all()
    return [r.group_id for r in rows]


def clear_group_links_for_filename(session, filename: str) -> None:
    rows = session.exec(select(MemeDuplicateGroup).where(MemeDuplicateGroup.filename == filename)).all()
    for r in rows:
        session.delete(r)


def set_group_links(session, group_id: int, filenames: List[str]) -> None:
    for fn in filenames:
        session.add(MemeDuplicateGroup(group_id=group_id, filename=fn))

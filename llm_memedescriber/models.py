import datetime
from typing import Optional

from sqlmodel import Field, SQLModel

class Meme(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    filename: str = Field(index=True, unique=True)
    category: Optional[str] = None
    description: Optional[str] = None
    keywords: Optional[str] = None
    text_in_image: Optional[str] = None
    source_url: Optional[str] = None
    status: str = Field(default="pending", index=True)
    attempts: int = Field(default=0)
    last_error: Optional[str] = None
    last_attempt_at: Optional[datetime.datetime] = None
    created_at: datetime.datetime = Field(default_factory=lambda: datetime.datetime.now(datetime.timezone.utc))
    updated_at: datetime.datetime = Field(default_factory=lambda: datetime.datetime.now(datetime.timezone.utc))
    
    phash: Optional[str] = Field(default=None, index=True)  # perceptual hash
    
    is_false_positive: bool = Field(default=False)  # user marked as "not a duplicate"


class Duplicate(SQLModel, table=True):
    """Represents a duplicate link between two files.

    `is_false_positive` marks that this specific duplicate link
    should be ignored when forming duplicate groups.
    """
    id: Optional[int] = Field(default=None, primary_key=True)
    filename_a: str = Field(index=True)
    filename_b: str = Field(index=True)
    is_false_positive: bool = Field(default=False, index=True)
    created_at: datetime.datetime = Field(default_factory=lambda: datetime.datetime.now(datetime.timezone.utc))


class DuplicateGroup(SQLModel, table=True):
    """Represents a detected group of visually similar memes."""
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime.datetime = Field(default_factory=lambda: datetime.datetime.now(datetime.timezone.utc))


class MemeDuplicateGroup(SQLModel, table=True):
    """Association table linking memes to duplicate groups.

    We store `filename` to avoid strict foreign key dependencies on numeric IDs,
    and keep it simple to insert/delete rows.
    """
    id: Optional[int] = Field(default=None, primary_key=True)
    group_id: int = Field(index=True)
    filename: str = Field(index=True)
    created_at: datetime.datetime = Field(default_factory=lambda: datetime.datetime.now(datetime.timezone.utc))


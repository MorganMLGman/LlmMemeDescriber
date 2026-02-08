"""Common types and data structures for LLM providers."""

from dataclasses import dataclass
from typing import List, Dict, Any


@dataclass
class MediaContent:
    """Represents media content to be analyzed."""
    data: bytes
    mime_type: str
    filename: str


@dataclass
class DescriptionRequest:
    """Request for generating a description."""
    media: MediaContent
    prompt: str


@dataclass
class DescriptionResponse:
    """Standardized response format for descriptions."""
    kategoria: str
    opis: str
    keywordy: List[str]
    tekst: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary matching existing format."""
        return {
            'kategoria': self.kategoria,
            'opis': self.opis,
            'keywordy': self.keywordy,
            'tekst': self.tekst
        }

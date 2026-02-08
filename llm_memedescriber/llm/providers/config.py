"""Configuration classes for LLM providers."""

from dataclasses import dataclass, field
from typing import List


@dataclass
class ProviderConfig:
    """Base configuration class for all providers."""
    timeout: int = 120
    max_retries: int = 3


@dataclass
class GeminiConfig(ProviderConfig):
    """Configuration for Google Gemini provider."""
    # Safety settings - all set to BLOCK_NONE for meme content
    block_harassment: str = "BLOCK_NONE"
    block_hate_speech: str = "BLOCK_NONE"
    block_sexually_explicit: str = "BLOCK_NONE"
    block_dangerous_content: str = "BLOCK_NONE"

    # Media resolution settings
    image_resolution: str = "MEDIA_RESOLUTION_HIGH"
    video_resolution: str = "MEDIA_RESOLUTION_MEDIUM"

    # Supported MIME types
    supported_image_types: List[str] = field(default_factory=lambda: [
        "image/jpeg",
        "image/png",
        "image/webp",
        "image/gif"
    ])
    supported_video_types: List[str] = field(default_factory=lambda: [
        "video/mp4",
        "video/mpeg"
    ])


@dataclass
class OpenAIConfig(ProviderConfig):
    """Configuration for OpenAI GPT provider."""
    vision_detail: str = "high"  # low, high, or auto
    max_tokens: int = 1000
    temperature: float = 0.7

    supported_image_types: List[str] = field(default_factory=lambda: [
        "image/jpeg",
        "image/png",
        "image/webp",
        "image/gif"
    ])
    # OpenAI doesn't support video in vision API
    supported_video_types: List[str] = field(default_factory=list)


@dataclass
class AnthropicConfig(ProviderConfig):
    """Configuration for Anthropic Claude provider."""
    max_tokens: int = 1024
    temperature: float = 0.7

    supported_image_types: List[str] = field(default_factory=lambda: [
        "image/jpeg",
        "image/png",
        "image/webp",
        "image/gif"
    ])
    # Claude supports PDF but not video for vision
    supported_video_types: List[str] = field(default_factory=list)

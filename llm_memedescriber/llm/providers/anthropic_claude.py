"""Anthropic Claude provider implementation (stub)."""

import logging
import base64
import re
import json
from typing import Dict, Any

from ..base import LLMProvider
from ..types import DescriptionRequest
from ..exceptions import RateLimitError, UnsupportedMediaError, LLMProviderError
from .config import AnthropicConfig

logger = logging.getLogger(__name__)


class ClaudeProvider(LLMProvider):
    """Anthropic Claude implementation."""

    def _default_config(self) -> AnthropicConfig:
        """Return default configuration for Claude."""
        return AnthropicConfig()

    def _initialize_client(self):
        """Initialize Anthropic client."""
        try:
            from anthropic import Anthropic
            return Anthropic(api_key=self.api_key)
        except ImportError as exc:
            logger.error("anthropic package not installed. Install with: pip install anthropic")
            raise LLMProviderError(
                "anthropic package not installed. Install with: pip install anthropic"
            ) from exc
        except Exception as exc:
            logger.exception("Failed to initialize Anthropic client")
            raise LLMProviderError(f"Failed to initialize Anthropic: {exc}") from exc

    @classmethod
    def provider_name(cls) -> str:
        """Return the provider identifier."""
        return "anthropic"

    def is_media_supported(self, mime_type: str) -> bool:
        """Check if MIME type is supported by Claude.

        Note: Claude supports images but not video.
        """
        config = self.config
        return mime_type in config.supported_image_types

    def generate_description(self, request: DescriptionRequest) -> Dict[str, Any]:
        """Generate description using Anthropic Claude.

        TODO: Implement full Anthropic Claude Vision integration

        Args:
            request: DescriptionRequest containing media and prompt

        Returns:
            Dict with keys: 'kategoria', 'opis', 'keywordy', 'tekst'
            or {'rate_limited': True, 'error': '...'} on rate limit
            or {} on other errors

        Raises:
            RateLimitError: When rate limit is exceeded
            UnsupportedMediaError: When media type is not supported
            LLMProviderError: On other provider errors
        """
        mime_type = request.media.mime_type

        if not self.is_media_supported(mime_type):
            logger.warning(f"Unsupported MIME type for Claude: {mime_type}")
            raise UnsupportedMediaError(f"Claude doesn't support {mime_type}")

        try:
            # Encode image to base64
            image_b64 = base64.b64encode(request.media.data).decode('utf-8')

            # TODO: Adjust the prompt format to match expected JSON response format
            # The prompt should request the same structure as Gemini:
            # {'kategoria': str, 'opis': str, 'keywordy': list, 'tekst': str}

            # Claude uses a different message format
            response = self.client.messages.create(
                model=self.model,
                max_tokens=self.config.max_tokens,
                temperature=self.config.temperature,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image",
                                "source": {
                                    "type": "base64",
                                    "media_type": mime_type,
                                    "data": image_b64,
                                },
                            },
                            {
                                "type": "text",
                                "text": request.prompt
                            }
                        ],
                    }
                ],
            )

            # Extract and parse JSON response
            text = response.content[0].text
            return self._parse_json_response(text)

        except Exception as exc:
            error_str = str(exc)

            # Check for rate limiting
            if '429' in error_str or 'rate_limit' in error_str.lower():
                logger.warning(f"Claude rate limit hit: {exc}")
                raise RateLimitError(f"Rate limit exceeded: {exc}") from exc

            logger.exception(f"Claude API call failed: {exc}")
            raise LLMProviderError(f"Claude error: {exc}") from exc

    def _parse_json_response(self, text: str) -> Dict[str, Any]:
        """Parse JSON from Claude response.

        TODO: Implement robust JSON extraction similar to Gemini provider.
        May need to handle different response formats from Claude models.
        """
        # Try code blocks first
        m = re.search(r"```json\s*(\{.*?\})\s*```", text, flags=re.DOTALL | re.IGNORECASE)
        if not m:
            m = re.search(r"```\s*(\{.*?\})\s*```", text, flags=re.DOTALL)
        if not m:
            m = re.search(r"(\{.*\})", text, flags=re.DOTALL)
        if not m:
            logger.warning("Failed to extract JSON from Claude response")
            return {}

        candidate = m.group(1)
        try:
            return json.loads(candidate)
        except Exception:
            try:
                # Try to fix trailing commas
                cleaned = re.sub(r",\s*([}\]])", r"\1", candidate)
                return json.loads(cleaned)
            except Exception:
                logger.warning("Failed to parse JSON from Claude response")
                return {}

"""OpenAI GPT provider implementation (stub)."""

import logging
import base64
import re
import json
from typing import Dict, Any

from ..base import LLMProvider
from ..types import DescriptionRequest
from ..exceptions import RateLimitError, UnsupportedMediaError, LLMProviderError
from .config import OpenAIConfig

logger = logging.getLogger(__name__)


class OpenAIProvider(LLMProvider):
    """OpenAI GPT implementation."""

    def _default_config(self) -> OpenAIConfig:
        """Return default configuration for OpenAI."""
        return OpenAIConfig()

    def _initialize_client(self):
        """Initialize OpenAI client."""
        try:
            from openai import OpenAI
            return OpenAI(api_key=self.api_key)
        except ImportError as exc:
            logger.error("openai package not installed. Install with: pip install openai")
            raise LLMProviderError(
                "openai package not installed. Install with: pip install openai"
            ) from exc
        except Exception as exc:
            logger.exception("Failed to initialize OpenAI client")
            raise LLMProviderError(f"Failed to initialize OpenAI: {exc}") from exc

    @classmethod
    def provider_name(cls) -> str:
        """Return the provider identifier."""
        return "openai"

    def is_media_supported(self, mime_type: str) -> bool:
        """Check if MIME type is supported by OpenAI.

        Note: OpenAI only supports images, no video.
        """
        config = self.config
        return mime_type in config.supported_image_types

    def generate_description(self, request: DescriptionRequest) -> Dict[str, Any]:
        """Generate description using OpenAI GPT Vision.

        TODO: Implement full OpenAI GPT Vision integration

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
            logger.warning(f"Unsupported MIME type for OpenAI: {mime_type}")
            raise UnsupportedMediaError(f"OpenAI doesn't support {mime_type}")

        try:
            # Encode image to base64
            image_b64 = base64.b64encode(request.media.data).decode('utf-8')

            # TODO: Adjust the prompt format to match expected JSON response format
            # The prompt should request the same structure as Gemini:
            # {'kategoria': str, 'opis': str, 'keywordy': list, 'tekst': str}

            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": request.prompt},
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:{mime_type};base64,{image_b64}",
                                    "detail": self.config.vision_detail
                                }
                            }
                        ]
                    }
                ],
                max_tokens=self.config.max_tokens,
                temperature=self.config.temperature
            )

            # Extract and parse JSON response
            text = response.choices[0].message.content
            return self._parse_json_response(text)

        except Exception as exc:
            error_str = str(exc)

            # Check for rate limiting
            if '429' in error_str or 'rate_limit' in error_str.lower():
                logger.warning(f"OpenAI rate limit hit: {exc}")
                raise RateLimitError(f"Rate limit exceeded: {exc}") from exc

            logger.exception(f"OpenAI API call failed: {exc}")
            raise LLMProviderError(f"OpenAI error: {exc}") from exc

    def _parse_json_response(self, text: str) -> Dict[str, Any]:
        """Parse JSON from OpenAI response.

        TODO: Implement robust JSON extraction similar to Gemini provider.
        May need to handle different response formats from GPT models.
        """
        # Try code blocks first
        m = re.search(r"```json\s*(\{.*?\})\s*```", text, flags=re.DOTALL | re.IGNORECASE)
        if not m:
            m = re.search(r"```\s*(\{.*?\})\s*```", text, flags=re.DOTALL)
        if not m:
            m = re.search(r"(\{.*\})", text, flags=re.DOTALL)
        if not m:
            logger.warning("Failed to extract JSON from OpenAI response")
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
                logger.warning("Failed to parse JSON from OpenAI response")
                return {}

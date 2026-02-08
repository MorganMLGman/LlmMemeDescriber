"""Google Gemini provider implementation."""

import logging
import re
import json
from typing import Dict, Any, Optional, List

from google import genai as google_genai
from google.genai import types

from ..base import LLMProvider
from ..types import DescriptionRequest
from ..exceptions import RateLimitError, UnsupportedMediaError, LLMProviderError
from .config import GeminiConfig

logger = logging.getLogger(__name__)


class GeminiProvider(LLMProvider):
    """Google Gemini implementation."""

    def _default_config(self) -> GeminiConfig:
        """Return default configuration for Gemini."""
        return GeminiConfig()

    def _initialize_client(self) -> google_genai.Client:
        """Initialize Google GenAI client."""
        try:
            return google_genai.Client(api_key=self.api_key)
        except Exception as exc:
            logger.exception("Failed to initialize Gemini client")
            raise LLMProviderError(f"Failed to initialize Gemini: {exc}") from exc

    @classmethod
    def provider_name(cls) -> str:
        """Return the provider identifier."""
        return "gemini"

    def is_media_supported(self, mime_type: str) -> bool:
        """Check if MIME type is supported by Gemini."""
        config = self.config
        return (mime_type in config.supported_image_types or
                mime_type in config.supported_video_types)

    def _detect_media_resolution(self, mime_type: str) -> types.MediaResolution:
        """Determine media resolution based on MIME type."""
        config = self.config
        if mime_type in config.supported_image_types:
            return getattr(types.MediaResolution, config.image_resolution)
        elif mime_type in config.supported_video_types:
            return getattr(types.MediaResolution, config.video_resolution)
        return types.MediaResolution.MEDIA_RESOLUTION_HIGH

    def generate_description(self, request: DescriptionRequest) -> Dict[str, Any]:
        """Generate description using Gemini.

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
            logger.warning(f"Unsupported MIME type for Gemini: {mime_type}")
            raise UnsupportedMediaError(f"Gemini doesn't support {mime_type}")

        try:
            # Create media part
            media_res = self._detect_media_resolution(mime_type)
            part = types.Part.from_bytes(
                data=request.media.data,
                mime_type=mime_type,
                media_resolution=media_res
            )

            # Configure safety settings
            config = self.config
            safety_settings = [
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_HARASSMENT,
                    threshold=getattr(types.HarmBlockThreshold, config.block_harassment),
                ),
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
                    threshold=getattr(types.HarmBlockThreshold, config.block_hate_speech),
                ),
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
                    threshold=getattr(types.HarmBlockThreshold, config.block_sexually_explicit),
                ),
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
                    threshold=getattr(types.HarmBlockThreshold, config.block_dangerous_content),
                ),
            ]

            # Generate content
            response = self.client.models.generate_content(
                model=self.model,
                contents=[part, request.prompt],
                config=types.GenerateContentConfig(safety_settings=safety_settings)
            )

            # Extract and parse JSON response
            return self._parse_response(response)

        except Exception as exc:
            error_str = str(exc)

            # Check for rate limiting
            if '429' in error_str or 'rate limit' in error_str.lower():
                logger.warning(f"Gemini rate limit hit: {exc}")
                raise RateLimitError(f"Rate limit exceeded: {exc}") from exc

            # Check for unsupported MIME
            if 'Unsupported MIME type' in error_str:
                logger.info(f"Gemini rejected MIME type: {mime_type}")
                raise UnsupportedMediaError(f"Unsupported MIME: {mime_type}") from exc

            logger.exception(f"Gemini API call failed: {exc}")
            raise LLMProviderError(f"Gemini error: {exc}") from exc

    def _parse_response(self, response: Any) -> Dict[str, Any]:
        """Parse Gemini response and extract JSON.

        Args:
            response: Gemini API response

        Returns:
            Parsed JSON dict or empty dict if parsing fails
        """
        # Extract text candidates
        texts = self._extract_text_candidates(response)

        for txt in texts:
            if not txt:
                continue
            parsed = self._extract_json_from_text(txt)
            if parsed is not None:
                logger.debug("Successfully parsed JSON response")
                return parsed

        logger.warning("Failed to extract valid JSON from Gemini response")
        return {}

    def _extract_text_candidates(self, response: Any) -> List[str]:
        """Extract text candidates from Gemini response.

        Tries multiple response formats to extract text content.
        """
        texts: List[str] = []
        try:
            if hasattr(response, "outputs") and response.outputs:
                for out in response.outputs:
                    if hasattr(out, "content") and out.content:
                        for c in out.content:
                            if isinstance(c, str):
                                texts.append(c)
                            elif isinstance(c, dict) and c.get("text"):
                                texts.append(c.get("text"))
                            elif hasattr(c, "text"):
                                texts.append(getattr(c, "text"))

            if hasattr(response, "output") and response.output:
                for out in response.output:
                    if hasattr(out, "content") and out.content:
                        for c in out.content:
                            if isinstance(c, str):
                                texts.append(c)
                            elif isinstance(c, dict) and c.get("text"):
                                texts.append(c.get("text"))
                            elif hasattr(c, "text"):
                                texts.append(getattr(c, "text"))

            if hasattr(response, "content") and response.content:
                if isinstance(response.content, str):
                    texts.append(response.content)
                elif isinstance(response.content, list):
                    for c in response.content:
                        if isinstance(c, str):
                            texts.append(c)
                        elif isinstance(c, dict) and c.get("text"):
                            texts.append(c.get("text"))
        except Exception as e:
            logger.debug(f"Failed to extract text candidates from response: {e}")

        # Fallback: convert whole response to string
        texts.append(str(response))
        return texts

    @staticmethod
    def _extract_json_from_text(text: str) -> Optional[Dict[str, Any]]:
        """Extract JSON from text response.

        Tries to find JSON in code blocks or as plain JSON.
        Also attempts to fix common JSON formatting issues.
        """
        # Try code blocks first
        m = re.search(r"```json\s*(\{.*?\})\s*```", text, flags=re.DOTALL | re.IGNORECASE)
        if not m:
            m = re.search(r"```\s*(\{.*?\})\s*```", text, flags=re.DOTALL)
        if not m:
            m = re.search(r"(\{.*\})", text, flags=re.DOTALL)
        if not m:
            return None

        candidate = m.group(1)
        try:
            return json.loads(candidate)
        except Exception:
            try:
                # Try to fix trailing commas
                cleaned = re.sub(r",\s*([}\]])", r"\1", candidate)
                return json.loads(cleaned)
            except Exception:
                return None

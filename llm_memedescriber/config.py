import datetime
import sys
import logging
import os
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from typing import Optional

from pydantic_settings import BaseSettings
from pydantic import field_validator, model_validator, ValidationError, ConfigDict, SecretStr

logger = logging.getLogger(__name__)

class Settings(BaseSettings):
    model_config = ConfigDict(
        env_ignore_empty=False,
        case_sensitive=False
    )
    
    logging_level: str = "INFO"

    # LLM Provider Selection
    llm_provider: str = "gemini"  # Options: gemini, openai, anthropic

    # Provider-specific API keys
    gemini_api_key: SecretStr | None = None
    openai_api_key: SecretStr | None = None
    anthropic_api_key: SecretStr | None = None

    # Provider-specific models
    gemini_model: str = "gemini-3-flash-preview"
    openai_model: str = "gpt-4o"
    anthropic_model: str = "claude-3-5-sonnet-20241022"

    webdav_url: str | None = None
    webdav_username: SecretStr | None = None
    webdav_password: SecretStr | None = None
    webdav_path: str | None = None
    run_interval: str = "15min"
    timezone: str = "UTC"
    max_generation_attempts: int = 3
    auto_start_worker: bool = True
    
    ssl_cert_file: str | None = None
    ssl_key_file: str | None = None
    ssl_hostname: str = "localhost"
    
    # CORS settings
    cors_origins: str = ""  # Comma-separated allowed origins, e.g. "https://example.com,https://app.example.com"

    # Security settings
    csrf_secret: SecretStr | None = None  # Required for CSRF protection; set via CSRF_SECRET env var or Docker secret
    debug_mode: bool = False  # Set to False in production to enforce HTTPS
    
    # Authentication modes (mutually exclusive - only one can be True)
    public_mode: bool = True  # No authentication, all endpoints public (default)
    oidc_enabled: bool = False  # OIDC authentication via external provider
    basic_auth: bool = False  # Basic HTTP authentication (future)
    oidc_provider_url: str | None = None
    oidc_client_id: str | None = None
    oidc_client_secret: SecretStr | None = None
    oidc_redirect_uri: str | None = None
    oidc_scopes: str = "openid profile email"
    oidc_verify_ssl: bool = True  # Verify OIDC provider SSL certificate (default: True)
    oidc_ca_bundle_path: str | None = None  # Path to CA bundle for OIDC provider verification (optional)
    
    jwt_secret: SecretStr | None = None
    jwt_expiry_days: int = 30
    session_expiry_seconds: int = 86400
    
    # Redis session storage (optional, falls back to in-memory if not configured)
    redis_url: str | None = None  # e.g., "redis://redis:6379/0"
    redis_password: SecretStr | None = None  # Required if redis_url is set

    @field_validator("llm_provider")
    @classmethod
    def validate_provider(cls, v):
        """Validate provider is supported."""
        valid = ['gemini', 'openai', 'anthropic']
        if v.lower() not in valid:
            raise ValueError(f"llm_provider must be one of: {valid}")
        return v.lower()

    @field_validator("run_interval")
    @classmethod
    def validate_intervals(cls, v, info):
        if v is None or (isinstance(v, str) and v.strip() == ""):
            raise ValueError(f"{info.field_name} cannot be None or empty string")
        try:
            parse_interval(str(v))
            return v
        except Exception as exc:
            raise ValueError(f"Invalid interval: {exc}") from exc

    @field_validator("max_generation_attempts")
    @classmethod
    def validate_max_attempts(cls, v):
        if int(v) < 1:
            raise ValueError("max_generation_attempts must be >= 1")
        if int(v) > 10:
            raise ValueError("max_generation_attempts must be <= 10")
        return int(v)

    @field_validator("jwt_expiry_days")
    @classmethod
    def validate_jwt_expiry(cls, v):
        if int(v) < 1:
            raise ValueError("jwt_expiry_days must be >= 1")
        if int(v) > 365:
            raise ValueError("jwt_expiry_days must be <= 365")
        return int(v)

    @model_validator(mode="after")
    def validate_auth_modes(self):
        """Ensure exactly one authentication mode is enabled."""
        modes_enabled = sum([
            self.public_mode,
            self.oidc_enabled,
            self.basic_auth
        ])

        if modes_enabled == 0:
            raise ValueError("At least one authentication mode must be enabled: public_mode, oidc_enabled, or basic_auth")

        if modes_enabled > 1:
            raise ValueError("Only one authentication mode can be enabled: public_mode, oidc_enabled, or basic_auth")

        return self
    
    @model_validator(mode="after")
    def validate_csrf_secret(self):
        """Require csrf_secret when authentication is enabled."""
        if not self.public_mode and not self.csrf_secret:
            raise ValueError("csrf_secret is required when authentication is enabled. Set CSRF_SECRET env var or Docker secret.")
        return self

    @model_validator(mode="after")
    def validate_jwt_secret(self):
        """Require jwt_secret for OIDC and Basic Auth modes."""
        if (self.oidc_enabled or self.basic_auth) and not self.jwt_secret:
            raise ValueError("jwt_secret is required for OIDC and Basic Auth modes. Set JWT_SECRET env var or Docker secret.")
        return self

    @model_validator(mode="after")
    def validate_redis_config(self):
        """If redis_url is set, redis_password must be set."""
        if self.redis_url and not self.redis_password:
            raise ValueError("redis_password is required when redis_url is configured")
        return self

    @model_validator(mode="after")
    def validate_provider_api_key(self):
        """Warn if no API key is configured for the selected provider."""
        provider = self.llm_provider.lower()
        api_key = getattr(self, f'{provider}_api_key', None)

        if not api_key:
            logger.warning(f"No API key configured for provider '{provider}'. "
                         f"Set {provider.upper()}_API_KEY environment variable.")

        return self

    @field_validator("oidc_enabled", mode="before")
    @classmethod
    def validate_oidc_config(cls, v, info):
        """If OIDC is enabled, verify all required settings are present."""
        if not v:
            return v
        
        return v
    
    def model_post_init(self, __context):
        """Check OIDC configuration after all fields are loaded."""
        if not self.oidc_enabled:
            return
        
        required_fields = [
            ('oidc_provider_url', self.oidc_provider_url),
            ('oidc_client_id', self.oidc_client_id),
            ('oidc_client_secret', self.oidc_client_secret),
            ('oidc_redirect_uri', self.oidc_redirect_uri),
            ('jwt_secret', self.jwt_secret)
        ]
        
        missing = [name for name, value in required_fields if not value]
        
        if missing:
            logger.warning("OIDC enabled but missing settings: %s", missing)
        else:
            logger.info("OIDC Configuration:")
            logger.info("  Provider URL: %s", self.oidc_provider_url)
            logger.info("  Client ID: %s...", str(self.oidc_client_id)[:30])
            logger.info("  Redirect URI: %s", self.oidc_redirect_uri)
            logger.info("  Scopes: %s", self.oidc_scopes)

    @field_validator("gemini_api_key", "openai_api_key", "anthropic_api_key", "webdav_url", "webdav_username", "webdav_password", "oidc_client_secret", "jwt_secret", "csrf_secret", mode="before")
    @classmethod
    def _prefer_docker_secret(cls, v, info):
        """
        Prefer Docker secrets mounted at /run/secrets/<NAME> over environment variables.
        Tries secret files with the field name upper-cased and as-is.
        """
        secret = None
        try:
            candidates = [info.field_name.upper(), info.field_name]
            for name in candidates:
                path = f"/run/secrets/{name}"
                if os.path.isfile(path):
                    with open(path, "r", encoding="utf-8") as f:
                        data = f.read().strip()
                    if data:
                        secret = data
                        break
        except Exception:
            secret = None
        if secret:
            logger.debug("Using docker secret for %s", info.field_name)
            return secret
        return v

    @field_validator("ssl_cert_file", "ssl_key_file", mode="before")
    @classmethod
    def _ignore_system_default_ssl_paths(cls, v, info):
        """
        Ignore system default SSL certificate paths that may be set in the environment.
        Only use SSL certificates if explicitly provided by the user.
        
        System defaults like /etc/ssl/certs/ca-certificates.crt should be treated as "not set".
        """
        if not v:
            return None
        
        system_defaults = {
            "/etc/ssl/certs/ca-certificates.crt",  # Debian/Ubuntu
            "/etc/pki/tls/certs/ca-bundle.crt",    # CentOS/RHEL
            "/etc/ssl/certs/ca-bundle.crt",        # OpenSUSE
            "/etc/ssl/ca-bundle.pem",               # OpenSUSE
        }
        
        if v in system_defaults:
            logger.debug("Ignoring system default SSL path for %s: %s", info.field_name, v)
            return None
        
        return v


_settings_cache: Optional[Settings] = None

def load_settings() -> Settings:
    """Load settings from environment. Settings are cached after first load to avoid repeated initialization logging."""
    global _settings_cache
    
    if _settings_cache is not None:
        return _settings_cache
    
    try:
        settings = Settings()
        _settings_cache = settings
        return settings
    except ValidationError as e:
        logger.error("Configuration error:")
        for err in e.errors():
            logger.error(" - %s: %s", err.get('loc'), err.get('msg'))
        sys.exit(1)


class LocalISOFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, tz_name: str | None = None):
        super().__init__(fmt=fmt, datefmt=datefmt)
        self._tz = None
        self._tz_name = tz_name
        if tz_name:
            try:
                self._tz = ZoneInfo(tz_name)
            except ZoneInfoNotFoundError:
                self._tz = None

    def formatTime(self, record, datefmt=None):
        if self._tz is not None:
            dt = datetime.datetime.fromtimestamp(record.created, tz=self._tz)
        else:
            dt = datetime.datetime.fromtimestamp(record.created).astimezone()
        return dt.isoformat(timespec='milliseconds')


def configure_logging(settings: Settings | None = None):
    root = logging.getLogger()
    if not root.handlers:
        handler = logging.StreamHandler()
        tzname = getattr(settings, 'timezone', None) if settings is not None else None
        formatter = LocalISOFormatter('%(asctime)s %(levelname)s %(name)s %(message)s', tz_name=tzname)
        handler.setFormatter(formatter)
        root.addHandler(handler)
    if settings is not None:
        lvl = str(getattr(settings, 'logging_level', 'INFO')).strip().upper()
        numeric = getattr(logging, lvl, None)
        if not isinstance(numeric, int):
            root.setLevel(logging.INFO)
        else:
            root.setLevel(numeric)
    else:
        root.setLevel(logging.INFO)

    logger.info("Logging configured; root level=%s", logging.getLevelName(root.level))

    noisy = ['httpx', 'httpcore', 'webdav4', 'urllib3']
    for n in noisy:
        logging.getLogger(n).setLevel(logging.WARNING)

    for logger_name in ['uvicorn', 'uvicorn.error']:
        uvicorn_logger = logging.getLogger(logger_name)
        uvicorn_logger.handlers.clear()
        uvicorn_logger.propagate = True

    if root.level <= logging.DEBUG:
        logging.getLogger('alembic').setLevel(logging.DEBUG)
        logging.getLogger('alembic.runtime').setLevel(logging.DEBUG)
        logging.getLogger('google_genai').setLevel(logging.DEBUG)
        logging.getLogger('google_genai.models').setLevel(logging.DEBUG)
    else:
        logging.getLogger('alembic').setLevel(logging.WARNING)
        logging.getLogger('alembic.runtime').setLevel(logging.WARNING)
        logging.getLogger('google_genai').setLevel(logging.INFO)
        logging.getLogger('google_genai.models').setLevel(logging.WARNING)


def parse_interval(interval: str) -> int:
    if not interval:
        raise ValueError("Empty interval")
    s = str(interval).strip().lower()

    m = __import__('re').fullmatch(r"([+-]?\d+)\s*(s|sec|secs|second|seconds|m|min|mins|minute|minutes|h|hr|hrs|hour|hours)?", s)
    if not m:
        raise ValueError(f"Invalid interval '{interval}'")
    raw_num = m.group(1)
    num = int(raw_num)
    unit = m.group(2) or "s"
    
    if raw_num.startswith('-') or num < 0:
        raise ValueError("Interval must be non-negative")
    if num == 0:
        raise ValueError("Interval must be positive")

    if unit.startswith("s"):
        return num
    if unit.startswith("m"):
        return num * 60
    if unit.startswith("h"):
        return num * 3600
    return num
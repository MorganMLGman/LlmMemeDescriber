import datetime
import logging
import sys
import os
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import field_validator, ValidationError
from pydantic_settings import BaseSettings
import logging

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    logging_level: str = "INFO"
    google_genai_api_key: str | None = None
    google_genai_model: str = "gemini-2.5-flash"
    webdav_url: str | None = None
    webdav_username: str | None = None
    webdav_password: str | None = None
    webdav_path: str | None = None
    run_interval: str = "15min"
    timezone: str = "UTC"
    export_listing_on_shutdown: bool = True
    export_listing_interval: str = "24h"
    max_generation_attempts: int = 3
    auto_start_worker: bool = True
    backfill_from_listing_on_empty_db: bool = True

    @field_validator("run_interval", "export_listing_interval")
    @classmethod
    def validate_intervals(cls, v, info):
        if v is None:
            raise ValueError(f"{info.field_name} cannot be None")
        if isinstance(v, str) and v.strip() == "":
            raise ValueError(f"{info.field_name} cannot be empty string")
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

    @field_validator("google_genai_api_key", "webdav_url", "webdav_username", "webdav_password", mode="before")
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


def load_settings() -> Settings:
    try:
        return Settings()
    except ValidationError as e:
        logger = logging.getLogger(__name__)
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

    m = __import__('re').fullmatch(r"(\d+)\s*(s|sec|secs|second|seconds|m|min|mins|minute|minutes|h|hr|hrs|hour|hours)?", s)
    if not m:
        raise ValueError(f"Invalid interval '{interval}'")
    num = int(m.group(1))
    unit = m.group(2) or "s"

    if unit.startswith("s"):
        return num
    if unit.startswith("m"):
        return num * 60
    if unit.startswith("h"):
        return num * 3600
    return num
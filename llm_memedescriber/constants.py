"""Global constants for file type definitions."""
from urllib.parse import unquote

IMAGE_EXTENSIONS = {"jpg", "jpeg", "png", "webp", "gif", "bmp", "tiff"}
VIDEO_EXTENSIONS = {"mp4", "webm", "mov", "mkv", "avi", "flv"}
SUPPORTED_EXTENSIONS = IMAGE_EXTENSIONS | VIDEO_EXTENSIONS

INDEX_DIR = "/data/whoosh_index"

CACHE_DIR = "/cache"

PREVIEW_SIZE = 400
PREVIEW_JPEG_QUALITY_IMAGE = 40
PREVIEW_JPEG_QUALITY_VIDEO = 8

VIDEO_FRAME_TIMESTAMP = 1.0
VIDEO_EXTRACTION_TIMEOUT = 30

MAX_FILENAME_LENGTH = 255
MIN_SEARCH_QUERY_LENGTH = 2

DEFAULT_LIST_LIMIT = 100
DEFAULT_SEARCH_LIMIT = 50
DEFAULT_OFFSET = 0

MAX_DB_RETRY_ATTEMPTS = 3
INITIAL_DB_BACKOFF = 0.1

MAX_WEBDAV_RETRY_ATTEMPTS = 3
INITIAL_WEBDAV_BACKOFF = 0.5

BATCH_PROCESS_WORKERS = 3

DUPLICATE_THRESHOLD = 15

DEFAULT_SYNC_MAX_RECORDS = None

DEFAULT_PREVIEW_WORKERS = 8

DEFAULT_STORAGE_WORKERS = 6
DEFAULT_STORAGE_CONCURRENCY = 2


def _get_extension(filename: str) -> str:
    """Extract file extension safely."""
    ext = str(filename).lower().split('.')[-1] if '.' in filename else ''
    return ext


def is_supported(filename: str) -> bool:
    """Check if file format is supported."""
    ext = _get_extension(filename)
    return ext in SUPPORTED_EXTENSIONS


def is_image(filename: str) -> bool:
    """Check if file is an image."""
    ext = _get_extension(filename)
    return ext in IMAGE_EXTENSIONS


def is_video(filename: str) -> bool:
    """Check if file is a video."""
    ext = _get_extension(filename)
    return ext in VIDEO_EXTENSIONS


def sanitize_filename(filename: str) -> str:
    """Sanitize filename to prevent path traversal attacks.
    
    Decodes URL-encoded characters (for Polish characters and spaces).
    Removes leading slashes, dots, and backslashes.
    Allows UTF-8 characters, spaces, alphanumeric, dash, underscore, dot (for extension).
    """
    # Decode URL-encoded characters (e.g., %20 -> space, %C4%99 -> ę)
    filename = unquote(filename)
    
    # Remove path traversal attempts
    sanitized = filename.split('/')[-1].split('\\')[-1]
    
    # Remove leading dots and slashes
    sanitized = sanitized.lstrip('.' + '/\\')
    
    if len(sanitized) > MAX_FILENAME_LENGTH:
        raise ValueError(f"Invalid filename: exceeds maximum length of {MAX_FILENAME_LENGTH}")
    
    # Allow UTF-8 characters, spaces, alphanumeric, dash, underscore, dot
    # Block only dangerous characters: < > : " | ? * and control characters
    dangerous_chars = set('<>:"|?*\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f')
    sanitized = ''.join(c for c in sanitized if c not in dangerous_chars)
    
    if not sanitized:
        raise ValueError("Invalid filename: empty after sanitization")
    
    return sanitized


import io
import logging
import re
import ipaddress
import tempfile
import os
import urllib.request
from typing import Optional, Dict, Any, Callable, Tuple, List
from urllib.parse import urlparse, urljoin
import yt_dlp

logger = logging.getLogger(__name__)

# Default yt-dlp options for highest quality video
DEFAULT_YTDLP_OPTIONS = {
    'format': 'bestvideo+bestaudio/best',  # Highest quality video+audio, merge if needed
    'merge_output_format': 'mp4',  # Always output MP4
    'quiet': True,
    'no_warnings': True,
    'extract_flat': False,
    'socket_timeout': 30,
    'retries': 3,
    'file_access_retries': 3,
    'fragment_retries': 3,
    'ignoreerrors': False,
    'no_color': True,
}


class DownloadProgress:
    """Track download progress from yt-dlp hooks."""

    def __init__(self):
        self.status = 'pending'
        self.progress_percent = 0.0
        self.downloaded_bytes = 0
        self.total_bytes = 0
        self.speed = 0  # bytes per second
        self.eta = 0  # estimated seconds remaining


def progress_hook(d: Dict, progress_obj: DownloadProgress):
    """Progress hook callback for yt-dlp."""
    if d['status'] == 'downloading':
        progress_obj.status = 'downloading'
        progress_obj.downloaded_bytes = d.get('downloaded_bytes', 0)
        progress_obj.total_bytes = d.get('total_bytes') or d.get('total_bytes_estimate', 0)

        if progress_obj.total_bytes > 0:
            progress_obj.progress_percent = (progress_obj.downloaded_bytes / progress_obj.total_bytes) * 100

        progress_obj.speed = d.get('speed', 0) or 0
        progress_obj.eta = d.get('eta', 0) or 0

    elif d['status'] == 'finished':
        progress_obj.status = 'finished'
        progress_obj.progress_percent = 100.0
        progress_obj.downloaded_bytes = d.get('total_bytes', 0)

    elif d['status'] == 'error':
        progress_obj.status = 'error'


def sanitize_filename(title: str, max_length: int = 200) -> str:
    """
    Sanitize video title for use as filename.

    Args:
        title: Video title from yt-dlp
        max_length: Maximum filename length (default 200 chars)

    Returns:
        Sanitized filename with .mp4 extension
    """
    # Remove invalid filesystem characters and URL problematic characters
    # Include # (hashtag) as it causes issues in WebDAV URLs
    invalid_chars = r'[<>:"/\\|?*#\x00-\x1f]'
    sanitized = re.sub(invalid_chars, '', title)

    # Replace spaces and multiple underscores with single underscore
    sanitized = re.sub(r'\s+', '_', sanitized)
    sanitized = re.sub(r'_+', '_', sanitized)

    # Remove leading/trailing underscores and dots
    sanitized = sanitized.strip('_.')

    # Limit length (reserve 4 chars for .mp4 extension)
    if len(sanitized) > max_length - 4:
        sanitized = sanitized[:max_length - 4]

    # Add .mp4 extension
    if not sanitized.lower().endswith('.mp4'):
        sanitized += '.mp4'

    # Fallback if empty
    if sanitized == '.mp4':
        sanitized = 'video.mp4'

    return sanitized


def extract_video_urls_from_html(page_url: str) -> List[Dict[str, str]]:
    """
    Extract video URLs from HTML page.
    
    Looks for:
    - <video> tags with src attribute
    - <source> tags with src attribute inside <video>
    - Custom attributes like 'source' in player components
    - Open Graph video tags
    
    Args:
        page_url: URL of the webpage to parse
        
    Returns:
        List of dictionaries with 'url' and 'source' (description) keys
    """
    video_urls = []
    
    try:
        # Validate URL first
        is_valid, error = validate_url(page_url)
        if not is_valid:
            logger.warning(f"Cannot extract from invalid URL: {error}")
            return video_urls
        
        # Fetch HTML content
        logger.info(f"Fetching HTML from: {page_url}")
        req = urllib.request.Request(
            page_url,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
        )
        
        with urllib.request.urlopen(req, timeout=10) as response:
            html_content = response.read().decode('utf-8', errors='ignore')
        
        # Pattern 1: Standard <video> and <source> tags with src
        video_src_pattern = r'<(?:video|source)[^>]+src=["\']([^"\'>]+)["\']'
        for match in re.finditer(video_src_pattern, html_content, re.IGNORECASE):
            url = match.group(1)
            # Make absolute URL if relative
            if url.startswith('/'):
                url = urljoin(page_url, url)
            elif not url.startswith(('http://', 'https://')):
                url = urljoin(page_url, url)
            
            if url.endswith(('.mp4', '.webm', '.mov', '.avi', '.mkv', '.flv')):
                video_urls.append({
                    'url': url,
                    'source': f'HTML <video>/<source> tag'
                })
        
        # Pattern 2: Custom 'source' attribute (like Vue player components)
        source_attr_pattern = r'source=["\']([^"\'>]+\.(?:mp4|webm|mov|avi|mkv|flv))["\']'
        for match in re.finditer(source_attr_pattern, html_content, re.IGNORECASE):
            url = match.group(1)
            if url.startswith('/'):
                url = urljoin(page_url, url)
            elif not url.startswith(('http://', 'https://')):
                url = urljoin(page_url, url)
            
            video_urls.append({
                'url': url,
                'source': 'Custom player component (source attribute)'
            })
        
        # Pattern 3: Open Graph video tags
        og_video_pattern = r'<meta[^>]+property=["\']og:video["\'][^>]+content=["\']([^"\'>]+)["\']'
        for match in re.finditer(og_video_pattern, html_content, re.IGNORECASE):
            url = match.group(1)
            video_urls.append({
                'url': url,
                'source': 'Open Graph meta tag'
            })
        
        # Pattern 4: data-src attribute (lazy loading)
        data_src_pattern = r'data-src=["\']([^"\'>]+\.(?:mp4|webm|mov|avi|mkv|flv))["\']'
        for match in re.finditer(data_src_pattern, html_content, re.IGNORECASE):
            url = match.group(1)
            if url.startswith('/'):
                url = urljoin(page_url, url)
            elif not url.startswith(('http://', 'https://')):
                url = urljoin(page_url, url)
            
            video_urls.append({
                'url': url,
                'source': 'Lazy-loaded video (data-src)'
            })
        
        # Remove duplicates while preserving order
        seen_urls = set()
        unique_videos = []
        for video in video_urls:
            if video['url'] not in seen_urls:
                seen_urls.add(video['url'])
                unique_videos.append(video)
        
        logger.info(f"Extracted {len(unique_videos)} unique video URL(s) from HTML")
        return unique_videos
        
    except urllib.error.URLError as e:
        logger.error(f"Failed to fetch HTML from {page_url}: {e}")
        return video_urls
    except Exception as e:
        logger.error(f"Error extracting video URLs from HTML: {e}")
        return video_urls


def validate_url(url: str) -> Tuple[bool, Optional[str]]:
    """
    Validate URL and check for SSRF attempts.

    Args:
        url: URL to validate

    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        parsed = urlparse(url)

        # Check scheme
        if parsed.scheme not in ['http', 'https']:
            return False, f"Invalid URL scheme: {parsed.scheme}. Only http and https are allowed."

        # Check if hostname exists
        if not parsed.hostname:
            return False, "Invalid URL: no hostname found."

        hostname = parsed.hostname.lower()

        # Block localhost and loopback
        if hostname in ['localhost', '127.0.0.1', '0.0.0.0', '::1', '0:0:0:0:0:0:0:1']:
            return False, "Access to localhost is not allowed."

        # Block private IP ranges
        try:
            ip = ipaddress.ip_address(hostname)
            if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
                return False, f"Access to private IP addresses is not allowed: {hostname}"
        except ValueError:
            # Not an IP address, that's fine - could be a domain name
            pass

        # Block common internal domains
        internal_domains = [
            'internal', 'local', 'corp', 'intranet', 'private',
            'metadata.google.internal',  # GCP metadata
            '169.254.169.254',  # AWS/Azure metadata
        ]

        for internal_domain in internal_domains:
            if internal_domain in hostname:
                return False, f"Access to internal domains is not allowed: {hostname}"

        # URL seems valid
        return True, None

    except Exception as e:
        logger.error("URL validation error: %s", e)
        return False, f"Invalid URL format: {str(e)}"


def get_video_info(url: str, ytdlp_options: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Extract video metadata without downloading.

    Args:
        url: Video URL
        ytdlp_options: Optional yt-dlp configuration overrides

    Returns:
        Dictionary with video metadata

    Raises:
        Exception: If metadata extraction fails
    """
    # Validate URL first
    is_valid, error = validate_url(url)
    if not is_valid:
        raise ValueError(error)

    # Merge options
    options = DEFAULT_YTDLP_OPTIONS.copy()
    if ytdlp_options:
        options.update(ytdlp_options)

    # Don't download, just extract info
    options['skip_download'] = True

    try:
        with yt_dlp.YoutubeDL(options) as ydl:
            info = ydl.extract_info(url, download=False)

            if info is None:
                raise ValueError("Could not extract video information")

            return {
                'title': info.get('title', 'Unknown'),
                'duration': info.get('duration', 0),  # seconds
                'filesize': info.get('filesize') or info.get('filesize_approx', 0),  # bytes
                'ext': info.get('ext', 'mp4'),
                'uploader': info.get('uploader', 'Unknown'),
                'upload_date': info.get('upload_date', ''),
                'webpage_url': info.get('webpage_url', url),
                'description': info.get('description', ''),
            }

    except yt_dlp.utils.DownloadError as e:
        error_str = str(e)
        logger.error("yt-dlp download error for URL %s: %s", url, error_str)
        
        # Check if it's an unsupported URL error
        if "Unsupported URL" in error_str:
            raise ValueError(f"This URL is not supported. yt-dlp cannot download from this source.")
        
        raise ValueError(f"Failed to get video info: {error_str}")

    except Exception as e:
        logger.error("Unexpected error getting video info for URL %s: %s", url, e)
        raise


def download_video(
    url: str,
    max_filesize_mb: Optional[int] = None,
    progress_callback: Optional[Callable[[DownloadProgress], None]] = None,
    ytdlp_options: Optional[Dict] = None
) -> Tuple[bytes, str, Dict]:
    """
    Download video from URL to memory.

    Args:
        url: Video URL to download
        max_filesize_mb: Maximum file size in MB (enforced before download)
        progress_callback: Optional callback function for progress updates
        ytdlp_options: Optional yt-dlp configuration overrides

    Returns:
        Tuple of (video_bytes, filename, metadata)

    Raises:
        ValueError: If URL is invalid or video is too large
        Exception: If download fails
    """
    # Validate URL first
    is_valid, error = validate_url(url)
    if not is_valid:
        raise ValueError(error)

    # Get video info first to check filesize
    try:
        info = get_video_info(url, ytdlp_options)
    except ValueError as e:
        # Re-raise ValueError messages as-is (they already have user-friendly text)
        raise
    except Exception as e:
        logger.error("Failed to get video info before download: %s", e)
        raise

    # Check filesize limit (if available)
    if max_filesize_mb:
        filesize = info.get('filesize') or info.get('filesize_approx') or 0
        if filesize and filesize > 0:
            estimated_size_mb = filesize / (1024 * 1024)
            if estimated_size_mb > max_filesize_mb:
                raise ValueError(
                    f"Video file size ({estimated_size_mb:.1f} MB) exceeds maximum allowed size ({max_filesize_mb} MB)"
                )
        else:
            logger.warning("Video filesize not available before download, will enforce limit during download")

    # Prepare filename
    filename = sanitize_filename(info.get('title', 'video'))

    # Merge options
    options = DEFAULT_YTDLP_OPTIONS.copy()
    if ytdlp_options:
        options.update(ytdlp_options)

    # Add filesize limit to yt-dlp options
    if max_filesize_mb:
        options['max_filesize'] = max_filesize_mb * 1024 * 1024

    # Setup progress tracking
    progress_obj = DownloadProgress()

    def _progress_hook(d):
        progress_hook(d, progress_obj)
        if progress_callback:
            progress_callback(progress_obj)

    options['progress_hooks'] = [_progress_hook]

    # Download to BytesIO
    video_buffer = io.BytesIO()

    # Create a temporary directory for download
    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            # Custom output template to control filename
            options['outtmpl'] = os.path.join(tmpdir, '%(id)s.%(ext)s')

            # Use yt-dlp to download
            with yt_dlp.YoutubeDL(options) as ydl:
                # Extract info and download
                info_dict = ydl.extract_info(url, download=True)

                if info_dict is None:
                    raise ValueError("Download failed: no info returned")

                # Get the downloaded file path
                downloaded_file = ydl.prepare_filename(info_dict)

                # Read the file into memory
                try:
                    with open(downloaded_file, 'rb') as f:
                        video_bytes = f.read()
                except FileNotFoundError:
                    # Try alternate filename patterns
                    import glob
                    pattern = os.path.join(tmpdir, f"{info_dict.get('id')}.*")
                    matches = glob.glob(pattern)
                    if matches:
                        with open(matches[0], 'rb') as f:
                            video_bytes = f.read()
                        downloaded_file = matches[0]
                    else:
                        raise ValueError("Downloaded file not found")

                # Clean up the downloaded file
                try:
                    if os.path.exists(downloaded_file):
                        os.remove(downloaded_file)
                except Exception as e:
                    logger.warning("Failed to delete temporary file %s: %s", downloaded_file, e)

                # Update metadata with actual file info
                metadata = {
                    'title': info_dict.get('title', 'Unknown'),
                    'duration': info_dict.get('duration', 0),
                    'filesize': len(video_bytes),
                    'ext': info_dict.get('ext', 'mp4'),
                    'uploader': info_dict.get('uploader', 'Unknown'),
                    'webpage_url': info_dict.get('webpage_url', url),
                }

                logger.info(
                    "Successfully downloaded video: %s (%.2f MB)",
                    filename,
                    len(video_bytes) / (1024 * 1024)
                )

                return video_bytes, filename, metadata

        except yt_dlp.utils.DownloadError as e:
            error_str = str(e)
            logger.error("yt-dlp download error for URL %s: %s", url, error_str)
            
            # Check if it's an unsupported URL error
            if "Unsupported URL" in error_str:
                raise ValueError(f"This URL is not supported. yt-dlp cannot download from this source.")
            
            raise ValueError(f"Download failed: {error_str}")

        except Exception as e:
            logger.error("Unexpected error downloading video from URL %s: %s", url, e)
            raise

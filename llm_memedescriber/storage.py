import io
import json
import logging
import os
import subprocess
import tempfile
import time
from typing import Any, Dict, List, Optional

from webdav4.client import Client
from .constants import *

logger = logging.getLogger(__name__)


class WebDavStorage:
    def __init__(self, base_url: str, auth: Optional[tuple] = None):
        self.client = Client(base_url, auth=auth)

    def list_files(self, path: str, recursive: bool = False) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        entries = self.client.ls(path)
        for entry in entries:
            if isinstance(entry, dict):
                rel = entry.get('name') or entry.get('href')
                if rel is None:
                    continue
                rel = str(rel)
                full_path = rel if rel.startswith('/') else path.rstrip('/') + '/' + rel.lstrip('/')
                name = rel.rstrip('/').split('/')[-1]
                typ = entry.get('type') or entry.get('resource_type')
                is_dir = None
                if typ is not None:
                    is_dir = str(typ).lower() == 'directory'
            elif isinstance(entry, str):
                rel = entry
                full_path = rel if rel.startswith('/') else path.rstrip('/') + '/' + rel.lstrip('/')
                name = full_path.rstrip('/').split('/')[-1]
                is_dir = None
            else:
                continue

            if is_dir is None:
                is_dir = str(full_path).endswith('/')

            meta = {
                'path': full_path,
                'name': name,
                'is_dir': is_dir,
            }
            try:
                if isinstance(entry, dict):
                    for k in ('getlastmodified', 'modified', 'creationdate', 'getcreationdate', 'getcontentlength', 'size'):
                        if k in entry and entry.get(k) is not None:
                            if k == 'getcontentlength':
                                try:
                                    meta['size'] = int(entry.get(k))
                                except (ValueError, TypeError):
                                    meta['size'] = 0
                            else:
                                meta[k] = entry.get(k)
            except Exception:
                pass
            results.append(meta)

            if recursive and meta['is_dir']:
                try:
                    subresults = self.list_files(full_path, recursive=recursive)
                    results.extend(subresults)
                except Exception:
                    pass

        return results

    def load_listing(self, dir_path: str, json_filename: str = "listing.json") -> Dict[str, Any]:
        target_path = dir_path.rstrip('/') + '/' + json_filename
        try:
            with self.client.open(target_path, mode='r') as f:
                data = json.load(f)
        except Exception:
            return {}

        if isinstance(data, list):
            return {str(k): {} for k in data}
        if isinstance(data, dict):
            return {str(k): v for k, v in data.items()}
        return {}

    def write_listing(self, dir_path: str, mapping: Dict[str, Any], json_filename: str = "listing.json") -> str:
        ordered = {k: mapping[k] for k in sorted(mapping.keys())}
        text = json.dumps(ordered, indent=4, ensure_ascii=False) + "\n"
        
        if dir_path == '.' or dir_path == '' or dir_path == '/':
            target_path = json_filename
        else:
            target_path = dir_path.rstrip('/') + '/' + json_filename
        
        max_retries = MAX_WEBDAV_RETRY_ATTEMPTS
        initial_backoff = INITIAL_WEBDAV_BACKOFF
        last_exc = None
        
        for attempt in range(max_retries):
            try:
                self.client.upload_fileobj(io.BytesIO(text.encode('utf-8')), target_path, overwrite=True)
                return target_path
            except FileNotFoundError:
                raise
            except Exception as exc:
                last_exc = exc
                exc_str = str(exc).lower()
                if ('423' in str(exc) or 'locked' in exc_str) and attempt < max_retries - 1:
                    backoff = initial_backoff * (2 ** attempt)
                    logger.debug(
                        "WebDAV locked on attempt %d; retrying after %.2fs", attempt + 1, backoff
                    )
                    time.sleep(backoff)
                    continue
                raise IOError(f"Failed to upload listing to {dir_path}/{json_filename}: {exc}") from exc
        
        raise IOError(f"Failed to upload listing to {dir_path}/{json_filename} after {max_retries} attempts: {last_exc}") from last_exc

    def download_file(self, path: str) -> bytes:
        remote = path if str(path).startswith('/') else '/' + str(path).lstrip('/')
        try:
            with self.client.open(remote, mode='rb') as f:
                data = f.read()
        except FileNotFoundError:
            raise
        except Exception as exc:
            error_str = str(exc).lower()
            if any(x in error_str for x in ['404', 'not found', 'does not exist', 'resource not found']):
                raise FileNotFoundError(f"File not found: {remote}") from exc
            
            if 'notfound' in exc.__class__.__name__.lower():
                raise FileNotFoundError(f"File not found: {remote}") from exc
            raise IOError(f"Failed to download {remote}: {exc}") from exc
        if isinstance(data, str):
            return data.encode('utf-8')
        return data

    def upload_fileobj(self, path: str, data: bytes, overwrite: bool = True) -> None:
        try:
            self.client.upload_fileobj(io.BytesIO(data), path if str(path).startswith('/') else '/' + str(path).lstrip('/'), overwrite=overwrite)
        except FileNotFoundError:
            raise
        except Exception as exc:
            raise IOError(f"Failed to upload {path}: {exc}") from exc

    def open(self, path: str, mode: str = 'rb'):
        remote = path if str(path).startswith('/') else '/' + str(path).lstrip('/')
        return self.client.open(remote, mode=mode)

    def delete_file(self, path: str) -> None:
        """Delete a file from WebDAV storage."""
        remote = path if str(path).startswith('/') else '/' + str(path).lstrip('/')
        try:
            self.client.remove(remote)
        except FileNotFoundError:
            raise
        except Exception as exc:
            error_str = str(exc).lower()
            
            if any(x in error_str for x in ['404', 'not found', 'does not exist', 'resource not found']):
                raise FileNotFoundError(f"File not found: {remote}") from exc
            
            if 'notfound' in exc.__class__.__name__.lower():
                raise FileNotFoundError(f"File not found: {remote}") from exc
            raise IOError(f"Failed to delete {remote}: {exc}") from exc
    def extract_video_frame(self, video_path: str, timestamp: float = VIDEO_FRAME_TIMESTAMP) -> bytes:
        """Extract frame from video file and return as JPEG bytes.
        
        Args:
            video_path: Path to video file on WebDAV
            timestamp: Timestamp in seconds to extract frame from (default from VIDEO_FRAME_TIMESTAMP)
                      Falls back to first frame (0s) if video is shorter than requested timestamp
            
        Returns:
            JPEG image bytes
            
        Raises:
            FileNotFoundError: If video file not found
            IOError: If ffmpeg processing fails
        """
        try:
            video_data = self.download_file(video_path)
            
            with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as tmp_video:
                tmp_video.write(video_data)
                tmp_video_path = tmp_video.name
            
            with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as tmp_frame:
                tmp_frame_path = tmp_frame.name
            
            try:
                cmd = [
                    'ffmpeg',
                    '-i', tmp_video_path,
                    '-ss', str(timestamp),
                    '-vframes', '1',
                    '-f', 'image2',
                    '-q:v', str(PREVIEW_JPEG_QUALITY_VIDEO),
                    '-y',
                    tmp_frame_path
                ]
                
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    timeout=VIDEO_EXTRACTION_TIMEOUT,
                    check=False
                )
                
                if result.returncode != 0:
                    error_msg = result.stderr.decode('utf-8', errors='ignore')
                    if 'Immediate exit requested' in error_msg or 'Invalid' in error_msg:
                        logger.info(f"Could not extract frame at {timestamp}s (video too short?), extracting first frame instead for {video_path}")
                        cmd_fallback = [
                            'ffmpeg',
                            '-i', tmp_video_path,
                            '-vframes', '1',
                            '-f', 'image2',
                            '-q:v', str(PREVIEW_JPEG_QUALITY_VIDEO),
                            '-y',
                            tmp_frame_path
                        ]
                        result = subprocess.run(
                            cmd_fallback,
                            capture_output=True,
                            timeout=VIDEO_EXTRACTION_TIMEOUT,
                            check=False
                        )
                        if result.returncode != 0:
                            error_msg = result.stderr.decode('utf-8', errors='ignore')
                            raise IOError(f"FFmpeg failed to extract even first frame: {error_msg}")
                    else:
                        raise IOError(f"FFmpeg failed to extract frame: {error_msg}")
                
                with open(tmp_frame_path, 'rb') as f:
                    frame_data = f.read()
                
                if not frame_data:
                    raise IOError("FFmpeg produced no output")
                
                return frame_data
                
            finally:
                try:
                    os.unlink(tmp_video_path)
                except Exception:
                    pass
                try:
                    os.unlink(tmp_frame_path)
                except Exception:
                    pass
                    
        except FileNotFoundError:
            raise
        except subprocess.TimeoutExpired:
            raise IOError(f"FFmpeg timeout while processing {video_path}")
        except Exception as exc:
            raise IOError(f"Failed to extract video frame from {video_path}: {exc}") from exc
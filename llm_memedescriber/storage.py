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


def _detect_hw_encoder() -> Optional[str]:
    """Detect available hardware video encoder by actually testing it.

    Returns:
        - 'h264_nvenc' for NVIDIA GPUs
        - 'h264_qsv' for Intel Quick Sync
        - 'h264_vaapi' for generic DRM/VAAPI (AMD, Intel integrated)
        - None if no hardware encoder available
    """
    try:
        result = subprocess.run(
            ['ffmpeg', '-codecs'],
            capture_output=True,
            timeout=5,
            text=True,
            check=False
        )
        output = result.stdout + result.stderr

        # Test encoders in order of preference (NVIDIA > Intel QSV > VAAPI)
        encoders_to_test = []
        
        if 'h264_nvenc' in output:
            encoders_to_test.append('h264_nvenc')
        if 'h264_qsv' in output:
            encoders_to_test.append('h264_qsv')
        if 'h264_vaapi' in output and os.path.exists('/dev/dri/renderD128'):
            encoders_to_test.append('h264_vaapi')
        
        # Actually test each encoder with a dummy encode
        for encoder in encoders_to_test:
            try:
                # Try to encode 1 frame to verify encoder works
                test_cmd = [
                    'ffmpeg', '-f', 'lavfi', '-i', 'testsrc=duration=0.1:size=320x240:rate=1',
                    '-c:v', encoder, '-frames:v', '1', '-f', 'null', '-'
                ]
                test_result = subprocess.run(
                    test_cmd,
                    capture_output=True,
                    timeout=5,
                    check=False
                )
                
                if test_result.returncode == 0:
                    encoder_name = {
                        'h264_nvenc': 'NVIDIA',
                        'h264_qsv': 'Intel Quick Sync',
                        'h264_vaapi': 'DRM/VAAPI'
                    }.get(encoder, encoder)
                    logger.info(f"GPU encoder detected and verified: {encoder} ({encoder_name})")
                    return encoder
                else:
                    error_msg = test_result.stderr.decode('utf-8', errors='ignore')
                    logger.debug(f"Encoder {encoder} test failed: {error_msg[:100]}")
            except Exception as e:
                logger.debug(f"Failed to test {encoder}: {e}")
                
    except Exception as e:
        logger.debug(f"GPU detection failed: {e}")

    return None


def _detect_hw_decoder() -> Optional[str]:
    """Detect available hardware video decoder for frame extraction.

    Returns:
        - 'cuda' for NVIDIA GPUs (nvdec)
        - 'qsv' for Intel Quick Sync
        - 'vaapi' for generic DRM/VAAPI (AMD, Intel integrated)
        - None if no hardware decoder available
    """
    try:
        result = subprocess.run(
            ['ffmpeg', '-hwaccels'],
            capture_output=True,
            timeout=5,
            text=True,
            check=False
        )
        output = result.stdout + result.stderr

        # Check in order of preference (NVIDIA > Intel QSV > VAAPI)
        if 'cuda' in output:
            logger.info("GPU decoder detected: cuda (NVIDIA)")
            return 'cuda'
        elif 'qsv' in output:
            logger.info("GPU decoder detected: qsv (Intel Quick Sync)")
            return 'qsv'
        elif 'vaapi' in output and os.path.exists('/dev/dri/renderD128'):
            logger.info("GPU decoder detected: vaapi (DRM/VAAPI)")
            return 'vaapi'
    except Exception as e:
        logger.debug(f"GPU decoder detection failed: {e}")

    return None


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
        """Extract frame from video file and return as JPEG bytes with GPU acceleration if available.

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
                # Detect GPU decoder for hardware acceleration
                hw_decoder = _detect_hw_decoder()

                # Build base FFmpeg command with GPU acceleration if available
                def build_cmd(use_gpu: bool = False, ts: float = timestamp) -> list:
                    cmd = ['ffmpeg']

                    # Add hardware acceleration if requested and available
                    if use_gpu and hw_decoder:
                        if hw_decoder == 'cuda':
                            cmd.extend(['-hwaccel', 'cuda'])
                        elif hw_decoder == 'qsv':
                            cmd.extend(['-hwaccel', 'qsv'])
                        elif hw_decoder == 'vaapi':
                            cmd.extend(['-hwaccel', 'vaapi', '-hwaccel_device', '/dev/dri/renderD128'])

                    cmd.extend([
                        '-i', tmp_video_path,
                        '-ss', str(ts),
                        '-vframes', '1',
                        '-f', 'image2',
                        '-q:v', str(PREVIEW_JPEG_QUALITY_VIDEO),
                        '-y',
                        tmp_frame_path
                    ])
                    return cmd

                # Try with GPU acceleration first
                result = None
                if hw_decoder:
                    logger.info(f"Extracting frame with GPU acceleration ({hw_decoder}) for {video_path}")
                    cmd = build_cmd(use_gpu=True, ts=timestamp)
                    result = subprocess.run(
                        cmd,
                        capture_output=True,
                        timeout=VIDEO_EXTRACTION_TIMEOUT,
                        check=False
                    )

                    # If GPU fails, fall back to CPU
                    if result.returncode != 0:
                        error_msg = result.stderr.decode('utf-8', errors='ignore')
                        if 'hwaccel' in error_msg.lower():
                            logger.warning(f"GPU decoding failed for {video_path}, falling back to CPU")
                            result = None  # Reset to try CPU

                # Try with CPU if GPU not available or failed
                if result is None:
                    logger.info(f"Extracting frame with CPU for {video_path}")
                    cmd = build_cmd(use_gpu=False, ts=timestamp)
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

                        # Retry at timestamp 0 (first frame)
                        result = None
                        if hw_decoder:
                            cmd = build_cmd(use_gpu=True, ts=0.0)
                            result = subprocess.run(
                                cmd,
                                capture_output=True,
                                timeout=VIDEO_EXTRACTION_TIMEOUT,
                                check=False
                            )
                            if result.returncode != 0:
                                error_msg = result.stderr.decode('utf-8', errors='ignore')
                                if 'hwaccel' in error_msg.lower():
                                    logger.warning(f"GPU decoding failed for first frame, falling back to CPU")
                                    result = None

                        if result is None:
                            cmd = build_cmd(use_gpu=False, ts=0.0)
                            result = subprocess.run(
                                cmd,
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

    def transcode_mkv_to_mp4(self, mkv_path: str) -> tuple[bytes, str]:
        """Transcode MKV video to MP4 format using FFmpeg with GPU acceleration.

        Args:
            mkv_path: Path to MKV file on WebDAV

        Returns:
            Tuple of (mp4_bytes, new_filename_with_mp4_extension)

        Raises:
            FileNotFoundError: If MKV file not found on WebDAV
            IOError: If transcoding fails or times out
        """
        try:
            # Download MKV file from WebDAV
            logger.info(f"Downloading MKV file: {mkv_path}")
            mkv_data = self.download_file(mkv_path)
            if mkv_data is None:
                raise FileNotFoundError(f"MKV file not found: {mkv_path}")

            logger.info(f"Downloaded {mkv_path} ({len(mkv_data)} bytes), starting transcode...")

            # Create temporary files for processing
            with tempfile.NamedTemporaryFile(suffix='.mkv', delete=False) as tmp_mkv:
                tmp_mkv.write(mkv_data)
                tmp_mkv_path = tmp_mkv.name

            # Generate output filename (.mkv -> .mp4)
            base_name = mkv_path.rsplit('.', 1)[0] if '.' in mkv_path else mkv_path
            new_filename = f"{base_name}.mp4"

            with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as tmp_mp4:
                tmp_mp4_path = tmp_mp4.name

            try:
                # Detect GPU encoder, fallback to CPU
                hw_encoder = _detect_hw_encoder()

                def build_transcode_cmd(use_gpu: bool, encoder: Optional[str] = None) -> list[str]:
                    """Build FFmpeg transcode command with or without GPU acceleration."""
                    cmd = ['ffmpeg', '-i', tmp_mkv_path]
                    
                    if use_gpu and encoder == 'h264_nvenc':
                        # NVIDIA GPU - no preset or CRF with NVENC, use -rc and -cq instead
                        cmd.extend([
                            '-c:v', 'h264_nvenc',
                            '-rc', 'vbr',  # Variable bitrate
                            '-cq', '25',   # Quality (0-51, lower=higher quality)
                        ])
                    elif use_gpu and encoder == 'h264_qsv':
                        # Intel Quick Sync
                        cmd.extend([
                            '-c:v', 'h264_qsv',
                            '-q', '25',    # Quality (1-51, lower=higher quality)
                        ])
                    elif use_gpu and encoder == 'h264_vaapi':
                        # Generic VAAPI (AMD, Intel integrated)
                        cmd.extend([
                            '-c:v', 'h264_vaapi',
                            '-q', '25',    # Quality for VAAPI
                        ])
                    else:
                        # Fallback to CPU encoding
                        cmd.extend([
                            '-c:v', TRANSCODE_VIDEO_CODEC,
                            '-crf', str(TRANSCODE_CRF),
                            '-preset', TRANSCODE_PRESET,
                        ])
                    
                    # Add audio codec and streaming flags
                    cmd.extend([
                        '-c:a', TRANSCODE_AUDIO_CODEC,
                        '-movflags', '+faststart',  # Enable streaming
                        '-y',  # Overwrite output
                        tmp_mp4_path
                    ])
                    return cmd

                # Try with GPU acceleration first if available
                result = None
                if hw_encoder:
                    logger.info(f"Using {hw_encoder} GPU acceleration")
                    cmd = build_transcode_cmd(use_gpu=True, encoder=hw_encoder)
                    logger.info(f"Running FFmpeg transcode command: {' '.join(cmd)}")
                    result = subprocess.run(
                        cmd,
                        capture_output=True,
                        timeout=TRANSCODE_TIMEOUT,
                        check=False
                    )

                    # If GPU fails, fall back to CPU
                    if result.returncode != 0:
                        error_msg = result.stderr.decode('utf-8', errors='ignore')
                        # Check for GPU-related errors
                        if any(err in error_msg.lower() for err in ['cannot load', 'hwaccel', 'cuda', 'nvenc', 'qsv', 'vaapi', 'no device']):
                            logger.warning(f"GPU encoding with {hw_encoder} failed, falling back to CPU: {error_msg[:200]}")
                            result = None  # Reset to try CPU

                # Try with CPU if GPU not available or failed
                if result is None:
                    logger.info("Using CPU encoding (libx264)")
                    cmd = build_transcode_cmd(use_gpu=False)
                    logger.info(f"Running FFmpeg transcode command: {' '.join(cmd)}")
                    result = subprocess.run(
                        cmd,
                        capture_output=True,
                        timeout=TRANSCODE_TIMEOUT,
                        check=False
                    )

                if result.returncode != 0:
                    error_msg = result.stderr.decode('utf-8', errors='ignore')
                    raise IOError(f"FFmpeg transcoding failed (exit code {result.returncode}): {error_msg}")

                # Read transcoded MP4
                with open(tmp_mp4_path, 'rb') as f:
                    mp4_data = f.read()

                if not mp4_data:
                    raise IOError("FFmpeg produced no output")

                logger.info(f"Successfully transcoded {mkv_path} to MP4 ({len(mp4_data)} bytes)")
                return mp4_data, new_filename

            finally:
                # Cleanup temp files
                for path in [tmp_mkv_path, tmp_mp4_path]:
                    try:
                        os.unlink(path)
                    except Exception:
                        pass

        except FileNotFoundError:
            raise
        except subprocess.TimeoutExpired:
            raise IOError(f"Transcoding timeout for {mkv_path} (>{TRANSCODE_TIMEOUT}s)")
        except Exception as exc:
            raise IOError(f"Failed to transcode {mkv_path}: {exc}") from exc
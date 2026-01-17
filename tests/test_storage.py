import io as _io
import json
import time
import subprocess

import pytest

from llm_memedescriber.storage import WebDavStorage
from llm_memedescriber.constants import MAX_WEBDAV_RETRY_ATTEMPTS

from tests._helpers import FakeClient, FakeClientOpen


def test_list_files_handles_dicts_and_strings_and_metadata():
    mapping = {
        '/root': [
            {'name': 'subdir', 'type': 'directory', 'getlastmodified': '2020'},
            {'href': '/abs/file.txt', 'modified': '2021'},
            {'name': 'file2.txt'},
            'relative.txt',
        ]
    }
    s = WebDavStorage('http://example')
    s.client = FakeClient(mapping)

    res = s.list_files('/root', recursive=False)

    names = {r['name']: r for r in res}
    assert 'subdir' in names
    assert names['subdir']['path'] == '/root/subdir'
    assert names['subdir']['is_dir'] is True
    assert names['subdir']['getlastmodified'] == '2020'

    assert 'file.txt' in names
    assert names['file.txt']['path'] == '/abs/file.txt'
    assert names['file.txt']['modified'] == '2021'

    assert 'file2.txt' in names
    assert names['file2.txt']['path'] == '/root/file2.txt'

    assert 'relative.txt' in names
    assert names['relative.txt']['path'] == '/root/relative.txt'


def test_list_files_recursive_calls_subdir_and_merges_results():
    mapping = {
        '/root': [
            {'name': 'subdir', 'type': 'directory'},
        ],
        '/root/subdir': [
            {'name': 'subfile.txt'}
        ]
    }
    s = WebDavStorage('http://example')
    s.client = FakeClient(mapping)

    res = s.list_files('/root', recursive=True)
    paths = {r['path'] for r in res}
    assert '/root/subdir' in paths
    assert '/root/subdir/subfile.txt' in paths


def test_list_files_recursive_handles_subcall_exceptions():
    mapping = {
        '/root': [
            {'name': 'subdir', 'type': 'directory'},
        ],
        '/root/subdir': 'RAISE'
    }
    class RaisingClient(FakeClient):
        def ls(self, path):
            if path == '/root/subdir':
                raise RuntimeError('boom')
            return super().ls(path)

    s = WebDavStorage('http://example')
    s.client = RaisingClient(mapping)

    res = s.list_files('/root', recursive=True)
    paths = [r['path'] for r in res]
    assert '/root/subdir' in paths
    assert not any(p.startswith('/root/subdir/') and p != '/root/subdir' for p in paths)

def test_list_files_top_level_exception_raises():
    s = WebDavStorage('http://example')
    s.client = FakeClient({})

    with pytest.raises(RuntimeError):
        s.list_files('RAISE', recursive=False)
        
    with pytest.raises(RuntimeError):
        s.list_files('RAISE', recursive=True)

def test_list_files_empty_directory_returns_empty_list():
    s = WebDavStorage('http://example')
    s.client = FakeClient({'/empty': []})

    res = s.list_files('/empty', recursive=False)
    assert res == []

    res = s.list_files('/empty', recursive=True)
    assert res == []

def test_list_files_nonexistent_directory_returns_empty_list():
    s = WebDavStorage('http://example')
    s.client = FakeClient({})

    res = s.list_files('/nonexistent', recursive=False)
    assert res == []

    res = s.list_files('/nonexistent', recursive=True)
    assert res == []

def test_list_files_handles_mixed_entry_types():
    mapping = {
        '/root': [
            {'name': 'validfile.txt', 'modified': '2021'},
            12345,  # invalid entry type
            None,   # invalid entry type
            {'type': 'directory'},  # missing name
            'relative.txt',
        ]
    }
    s = WebDavStorage('http://example')
    s.client = FakeClient(mapping)

    res = s.list_files('/root', recursive=False)

    names = {r['name']: r for r in res}
    assert 'validfile.txt' in names
    assert names['validfile.txt']['path'] == '/root/validfile.txt'
    assert names['validfile.txt']['modified'] == '2021'

    assert 'relative.txt' in names
    assert names['relative.txt']['path'] == '/root/relative.txt'

    assert len(names) == 2
    

def test_download_file_returns_bytes_and_uses_leading_slash():
    s = WebDavStorage('http://example')
    s.client = FakeClientOpen(content=b'hello')

    out = s.download_file('path/to/file')
    assert out == b'hello'
    assert s.client.open_calls[0][0] == '/path/to/file'


def test_download_file_returns_encoded_string_when_str_produced():
    s = WebDavStorage('http://example')
    s.client = FakeClientOpen(content='text-ąćę')

    out = s.download_file('/somefile')
    assert isinstance(out, bytes)
    assert out == 'text-ąćę'.encode('utf-8')


def test_download_file_propagates_filenotfound_from_client():
    class FakeClient:
        def open(self, path, mode='rb'):
            raise FileNotFoundError('not found')

    s = WebDavStorage('http://example')
    s.client = FakeClient()

    with pytest.raises(FileNotFoundError):
        s.download_file('/noexist')


def test_download_file_raises_filenotfound_on_404_error_message():
    class FakeClient:
        def open(self, path, mode='rb'):
            raise Exception('404 Not Found')

    s = WebDavStorage('http://example')
    s.client = FakeClient()

    with pytest.raises(FileNotFoundError):
        s.download_file('x')


def test_download_file_raises_filenotfound_on_exception_class_name_notfound():
    class NotFoundError(Exception):
        pass
    class FakeClient:
        def open(self, path, mode='rb'):
            raise NotFoundError('missing')

    s = WebDavStorage('http://example')
    s.client = FakeClient()

    with pytest.raises(FileNotFoundError):
        s.download_file('x')        


def test_download_file_raises_filenotfound_on_message_does_not_exist():
    class FakeClient:
        def open(self, path, mode='rb'):
            raise Exception('Does not exist')

    s = WebDavStorage('http://example')
    s.client = FakeClient()

    with pytest.raises(FileNotFoundError):
        s.download_file('x')


def test_download_file_raises_filenotfound_on_message_resource_not_found():
    class FakeClient:
        def open(self, path, mode='rb'):
            raise Exception('Resource not found')

    s = WebDavStorage('http://example')
    s.client = FakeClient()

    with pytest.raises(FileNotFoundError):
        s.download_file('x')


def test_download_file_raises_ioerror_on_other_exception():
    class FakeClient:
        def open(self, path, mode='rb'):
            raise Exception('permission denied')

    s = WebDavStorage('http://example')
    s.client = FakeClient()

    with pytest.raises(IOError):
        s.download_file('x')


def test_extract_video_frame_success(monkeypatch):
    s = WebDavStorage('http://example')
    monkeypatch.setattr(WebDavStorage, 'download_file', lambda self, p: b'video-data')
    def fake_run(cmd, *args, **kwargs):
        tmp_frame = cmd[-1]
        with open(tmp_frame, 'wb') as f:
            f.write(b'jpeg-data')
        class R:
            returncode = 0
            stderr = b''
        return R()
    monkeypatch.setattr(subprocess, 'run', fake_run)
    out = s.extract_video_frame('video.mp4', timestamp=1.0)
    assert out == b'jpeg-data'


def test_extract_video_frame_fallback_on_short_video(monkeypatch):
    s = WebDavStorage('http://example')
    monkeypatch.setattr(WebDavStorage, 'download_file', lambda self, p: b'video-data')
    def fake_run(cmd, *args, **kwargs):
        fake_run.calls += 1
        if fake_run.calls == 1:
            class R:
                returncode = 1
                stderr = b'Immediate exit requested'
            return R()
        else:
            tmp_frame = cmd[-1]
            with open(tmp_frame, 'wb') as f:
                f.write(b'jpeg-fallback')
            class R:
                returncode = 0
                stderr = b''
            return R()
    fake_run.calls = 0
    monkeypatch.setattr(subprocess, 'run', fake_run)
    out = s.extract_video_frame('short.mp4', timestamp=10.0)
    assert out == b'jpeg-fallback'


def test_extract_video_frame_fallback_fails_raises(monkeypatch):
    s = WebDavStorage('http://example')
    monkeypatch.setattr(WebDavStorage, 'download_file', lambda self, p: b'video-data')
    def fake_run(cmd, *args, **kwargs):
        fake_run.calls += 1
        if fake_run.calls == 1:
            class R:
                returncode = 1
                stderr = b'Immediate exit requested'
            return R()
        else:
            class R:
                returncode = 1
                stderr = b'fallback fail'
            return R()
    fake_run.calls = 0
    monkeypatch.setattr(subprocess, 'run', fake_run)
    with pytest.raises(IOError):
        s.extract_video_frame('short.mp4', timestamp=10.0)


def test_extract_video_frame_timeout_raises(monkeypatch):
    s = WebDavStorage('http://example')
    monkeypatch.setattr(WebDavStorage, 'download_file', lambda self, p: b'video-data')
    def fake_run(cmd, *args, **kwargs):
        raise subprocess.TimeoutExpired(cmd, kwargs.get('timeout', 1))
    monkeypatch.setattr(subprocess, 'run', fake_run)
    with pytest.raises(IOError):
        s.extract_video_frame('video.mp4', timestamp=1.0)


def test_extract_video_frame_no_output_raises(monkeypatch):
    s = WebDavStorage('http://example')
    monkeypatch.setattr(WebDavStorage, 'download_file', lambda self, p: b'video-data')
    def fake_run(cmd, *args, **kwargs):
        class R:
            returncode = 0
            stderr = b''
        return R()
    monkeypatch.setattr(subprocess, 'run', fake_run)
    with pytest.raises(IOError):
        s.extract_video_frame('video.mp4', timestamp=1.0)


def test_extract_video_frame_download_filenotfound(monkeypatch):
    s = WebDavStorage('http://example')
    def dl(self, p):
        raise FileNotFoundError('missing')
    monkeypatch.setattr(WebDavStorage, 'download_file', dl)
    with pytest.raises(FileNotFoundError):
        s.extract_video_frame('video.mp4', timestamp=1.0)


def test_extract_video_frame_ffmpeg_non_immediate_failure_raises(monkeypatch):
    s = WebDavStorage('http://example')
    monkeypatch.setattr(WebDavStorage, 'download_file', lambda self, p: b'video-data')
    def fake_run(cmd, *args, **kwargs):
        class R:
            returncode = 1
            stderr = b'unsupported codec or file'
        return R()
    monkeypatch.setattr(subprocess, 'run', fake_run)
    with pytest.raises(IOError) as excinfo:
        s.extract_video_frame('video.mp4', timestamp=1.0)
    assert 'Failed to extract video frame' in str(excinfo.value)
    assert 'FFmpeg failed to extract frame' in str(excinfo.value)


def test_extract_video_frame_unlink_exceptions_are_swallowed(monkeypatch):
    s = WebDavStorage('http://example')
    monkeypatch.setattr(WebDavStorage, 'download_file', lambda self, p: b'video-data')
    def fake_run(cmd, *args, **kwargs):
        tmp_frame = cmd[-1]
        with open(tmp_frame, 'wb') as f:
            f.write(b'jpeg-ok')
        class R:
            returncode = 0
            stderr = b''
        return R()
    monkeypatch.setattr(subprocess, 'run', fake_run)

    import os
    orig_unlink = os.unlink
    def bad_unlink(path):
        raise Exception('unlink fail')
    monkeypatch.setattr(os, 'unlink', bad_unlink)

    out = s.extract_video_frame('video.mp4', timestamp=1.0)
    assert out == b'jpeg-ok'

    monkeypatch.setattr(os, 'unlink', orig_unlink)


def test_upload_fileobj_propagates_filenotfound_from_client():
    s = WebDavStorage('http://example')
    s.client = FakeClient({}, upload_fail_exc=FileNotFoundError('no such dir'))

    with pytest.raises(FileNotFoundError):
        s.upload_fileobj('/dir/file', b'data')


def test_upload_fileobj_raises_ioerror_on_other_exception():
    s = WebDavStorage('http://example')
    s.client = FakeClient({}, upload_fail_exc=Exception('permission denied'))

    with pytest.raises(IOError):
        s.upload_fileobj('/dir/file', b'data')


def test_delete_file_raises_filenotfound_on_404_message():
    s = WebDavStorage('http://example')
    s.client = FakeClient({}, remove_fail_exc=Exception('404 Not Found'))

    with pytest.raises(FileNotFoundError):
        s.delete_file('/noexist')


def test_delete_file_raises_filenotfound_on_exception_class_name_notfound():
    class NotFoundError(Exception):
        pass
    s = WebDavStorage('http://example')
    s.client = FakeClient({}, remove_fail_exc=NotFoundError('missing'))

    with pytest.raises(FileNotFoundError):
        s.delete_file('/noexist')


def test_delete_file_raises_ioerror_on_other_exception():
    s = WebDavStorage('http://example')
    s.client = FakeClient({}, remove_fail_exc=Exception('permission denied'))

    with pytest.raises(IOError):
        s.delete_file('/noexist')

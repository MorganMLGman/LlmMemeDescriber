import io as _io
import json
import time

import pytest

from llm_memedescriber.storage import WebDavStorage
from llm_memedescriber.constants import MAX_WEBDAV_RETRY_ATTEMPTS

from tests._helpers import FakeClient, FakeClientOpen, FakeUploadClient


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
    
def test_load_listing_handles_empty_response():
    s = WebDavStorage('http://example')
    s.client = FakeClient({'/empty': []})

    res = s.load_listing('/empty')
    
    assert res == {}

def test_load_listing_returns_empty_on_open_exception():
    s = WebDavStorage('http://example')
    s.client = FakeClientOpen(raise_on_open=True)

    res = s.load_listing('/x')
    assert res == {}
    assert s.client.open_calls


def test_load_listing_returns_empty_on_invalid_json():
    s = WebDavStorage('http://example')
    s.client = FakeClientOpen(content="not json")

    res = s.load_listing('/x')
    assert res == {}
    assert s.client.open_calls[0][1] == 'r'


def test_load_listing_handles_list_and_dict_and_other_types():
    s = WebDavStorage('http://example')

    s.client = FakeClientOpen(content=json.dumps(["a", 2]))
    res = s.load_listing('/root')
    assert res == {"a": {}, "2": {}}

    s.client = FakeClientOpen(content=json.dumps({"a": {"x": 1}}))
    res = s.load_listing('/root')
    assert res == {"a": {"x": 1}}

    s.client = FakeClientOpen(content=json.dumps("stringvalue"))
    res = s.load_listing('/root')
    assert res == {}
    
    s.client = FakeClientOpen(content=json.dumps({"a": {"x": 1}, "b": ["listvalue"]}))
    res = s.load_listing('/root')
    assert res == {"a": {"x": 1}, "b": ["listvalue"]}
    
    s.client = FakeClientOpen(content=json.dumps({"a": {"x": None, "y": 2}}))
    res = s.load_listing('/root')
    assert res == {"a": {"x": None, "y": 2}}


@pytest.mark.parametrize(
    "dir_path, expected_target",
    [
        ("/foo", "/foo/listing.json"),
        ("/foo/", "/foo/listing.json"),
        (".", "./listing.json"),
        ("", "/listing.json"),
        ("/", "/listing.json"),
        ("/some/longer/path", "/some/longer/path/listing.json"),
    ],
)
def test_load_listing_uses_expected_target_path(dir_path, expected_target):
    client = FakeClientOpen(content=json.dumps({}))
    s = WebDavStorage('http://example')
    s.client = client

    res = s.load_listing(dir_path, json_filename="listing.json")
    assert client.open_calls, "client.open was not called"
    assert client.open_calls[0][0] == expected_target
    assert res == {}


def test_write_listing_uploads_sorted_json_and_returns_path():
    client = FakeUploadClient()
    s = WebDavStorage('http://example')
    s.client = client

    mapping = {'b': 2, 'a': 1}
    target = s.write_listing('/dir', mapping, json_filename='listing.json')
    assert target == '/dir/listing.json'
    assert client.last_uploaded is not None

    path, data = client.last_uploaded
    assert path == '/dir/listing.json'
    text = data.decode('utf-8')
    obj = json.loads(text)
    assert list(obj.keys()) == ['a', 'b']
    assert obj['a'] == 1
    assert obj['b'] == 2
    

def test_write_listing_handles_current_dir_and_root():
    client = FakeUploadClient()
    s = WebDavStorage('http://example')
    s.client = client

    mapping = {'x': 1}
    t1 = s.write_listing('.', mapping, json_filename='listing.json')
    assert t1 == 'listing.json'
    assert client.last_uploaded[0] == 'listing.json'

    t2 = s.write_listing('/', mapping, json_filename='listing.json')
    assert t2 == 'listing.json' or t2 == '/listing.json'
    
@pytest.mark.parametrize(
    "dir_path, expected_target",
    [
        ("/foo", "/foo/listing.json"),
        ("/foo/", "/foo/listing.json"),
        (".", "listing.json"),
        ("", "listing.json"),
        ("/", "listing.json"),
        ("/some/longer/path", "/some/longer/path/listing.json"),
    ],
)
def test_write_listing_subdirectory_paths(dir_path, expected_target):
    client = FakeUploadClient()
    s = WebDavStorage('http://example')
    s.client = client

    mapping = {'key': 'value'}
    target = s.write_listing(dir_path, mapping, json_filename='listing.json')
    assert target == expected_target
    assert client.last_uploaded[0] == expected_target

def test_write_listing_propagates_filenotfound():
    client = FakeUploadClient(fail_times=1, fail_exc=FileNotFoundError('no such dir'))
    s = WebDavStorage('http://example')
    s.client = client

    with pytest.raises(FileNotFoundError):
        s.write_listing('/dir', {'a': 1})


def test_write_listing_retries_on_locked_and_succeeds(monkeypatch):
    client = FakeUploadClient(fail_times=2, fail_exc=Exception('423 Locked'))
    s = WebDavStorage('http://example')
    s.client = client

    sleeps = []
    monkeypatch.setattr(time, 'sleep', lambda x: sleeps.append(x))

    res = s.write_listing('/dir', {'a': 1})
    assert res == '/dir/listing.json'
    assert client.calls == 3
    assert len(sleeps) == 2


def test_write_listing_raises_after_max_retries(monkeypatch):
    client = FakeUploadClient(fail_times=100, fail_exc=Exception('423 Locked'))
    s = WebDavStorage('http://example')
    s.client = client

    monkeypatch.setattr(time, 'sleep', lambda x: None)

    with pytest.raises(IOError):
        s.write_listing('/dir', {'a': 1})
        
    assert client.calls == MAX_WEBDAV_RETRY_ATTEMPTS


def test_write_listing_raises_immediately_on_nonlock():
    client = FakeUploadClient(fail_times=1, fail_exc=Exception('permission denied'))
    s = WebDavStorage('http://example')
    s.client = client

    with pytest.raises(IOError):
        s.write_listing('/dir', {'a': 1})
        
    assert client.calls == 1

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


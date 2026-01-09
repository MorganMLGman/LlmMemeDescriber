import asyncio
import os
from io import BytesIO
from PIL import Image

import pytest

from llm_memedescriber import preview_helpers


class FakeStorage:
    def __init__(self, content: bytes = None):
        self.content = content
        self.download_calls = 0
        self.extract_calls = 0

    def download_file(self, filename):
        self.download_calls += 1
        return self.content

    def extract_video_frame(self, filename, timestamp=1.0):
        self.extract_calls += 1
        return self.content


class AsyncFakeStorage(FakeStorage):
    async def async_download_file(self, filename):
        self.download_calls += 1
        return self.content

    async def async_extract_video_frame(self, filename, timestamp=1.0):
        self.extract_calls += 1
        return self.content


def make_png_bytes(mode='RGB', size=(64, 64), color=(128, 128, 128, 255)):
    img = Image.new(mode, size, color)
    with BytesIO() as bio:
        img.save(bio, format='PNG')
        return bio.getvalue()


def test_generate_preview_from_image_creates_rgb_jpeg_and_caches(tmp_path, caplog):
    cache_dir = tmp_path / "cache"
    preview_helpers.CACHE_DIR = str(cache_dir)

    data = make_png_bytes(mode='RGBA', color=(10, 20, 30, 128))
    storage = FakeStorage(content=data)

    caplog_set_level = caplog.set_level
    caplog_set_level('DEBUG')

    out = preview_helpers.generate_preview('img.png', is_vid=False, storage=storage, size=50)
    assert isinstance(out, (bytes, bytearray))

    img = Image.open(BytesIO(out))
    assert img.mode == 'RGB'
    assert img.format == 'JPEG'

    cache_path = preview_helpers._cache_path('img.png')
    assert os.path.exists(cache_path)

    # next call should use cache and not call storage again
    storage2 = FakeStorage(content=b'should-not-be-used')
    out2 = preview_helpers.generate_preview('img.png', is_vid=False, storage=storage2)
    assert out2 == out
    assert storage2.download_calls == 0


def test_generate_preview_missing_raises_FileNotFoundError(tmp_path):
    preview_helpers.CACHE_DIR = str(tmp_path / "cache2")
    storage = FakeStorage(content=None)

    with pytest.raises(FileNotFoundError):
        preview_helpers.generate_preview('missing.png', is_vid=False, storage=storage)


def test_generate_preview_video_frame(tmp_path):
    preview_helpers.CACHE_DIR = str(tmp_path / "cache3")
    frame = make_png_bytes(mode='RGB')
    storage = FakeStorage(content=frame)

    out = preview_helpers.generate_preview('vid.mp4', is_vid=True, storage=storage)
    img = Image.open(BytesIO(out))
    assert img.format == 'JPEG'


def test_generate_preview_cache_write_failure_logged(tmp_path, caplog):
    preview_helpers.CACHE_DIR = str(tmp_path / "cache4")
    data = make_png_bytes(mode='RGB')
    storage = FakeStorage(content=data)

    # Make os.makedirs raise to simulate failure writing cache
    def bad_makedirs(path, exist_ok=False):
        raise RuntimeError('cannot create dir')

    import llm_memedescriber.preview_helpers as ph
    caplog.set_level('DEBUG', logger=ph.__name__)

    monkey = pytest.MonkeyPatch()
    monkey.setattr(os, 'makedirs', bad_makedirs)
    try:
        out = preview_helpers.generate_preview('img2.png', is_vid=False, storage=storage)
        assert isinstance(out, (bytes, bytearray))
        assert any('Failed to write preview cache for' in r.getMessage() for r in caplog.records)
    finally:
        monkey.undo()


def test_async_generate_preview_uses_call_storage(tmp_path):
    preview_helpers.CACHE_DIR = str(tmp_path / "cache_async")
    data = make_png_bytes(mode='RGB')
    storage = FakeStorage(content=data)

    out = asyncio.run(preview_helpers.async_generate_preview('a.png', is_vid=False, storage=storage))
    assert isinstance(out, (bytes, bytearray))
    assert storage.download_calls == 1


def test_async_generate_preview_prefers_async_method(tmp_path):
    preview_helpers.CACHE_DIR = str(tmp_path / "cache_async2")
    data = make_png_bytes(mode='RGB')
    storage = AsyncFakeStorage(content=data)

    out = asyncio.run(preview_helpers.async_generate_preview('a2.png', is_vid=False, storage=storage))
    assert isinstance(out, (bytes, bytearray))
    assert storage.download_calls == 1


def test_async_generate_preview_missing_raises_FileNotFoundError(tmp_path):
    preview_helpers.CACHE_DIR = str(tmp_path / "cache_async3")
    storage = AsyncFakeStorage(content=None)

    with pytest.raises(FileNotFoundError):
        asyncio.run(preview_helpers.async_generate_preview('missing2.png', is_vid=False, storage=storage))


def make_paletted_bytes(size=(64, 64), color=(10, 20, 30)):
    img = Image.new('RGB', size, color)
    p = img.convert('P')
    with BytesIO() as bio:
        p.save(bio, format='PNG')
        return bio.getvalue()


def test_cache_file_contents_matches_output(tmp_path):
    preview_helpers.CACHE_DIR = str(tmp_path / "cache_contents")
    data = make_png_bytes(mode='RGB')
    storage = FakeStorage(content=data)

    out = preview_helpers.generate_preview('file.png', is_vid=False, storage=storage)
    cache_path = preview_helpers._cache_path('file.png')
    with open(cache_path, 'rb') as f:
        cached = f.read()

    assert out == cached
    assert cached.startswith(b'\xff\xd8\xff')  # JPEG magic bytes


def test_generate_preview_handles_paletted_and_grayscale(tmp_path):
    preview_helpers.CACHE_DIR = str(tmp_path / "cache_pal_gray")
    pal = make_paletted_bytes()
    gray = make_png_bytes(mode='L', color=128)

    storage_pal = FakeStorage(content=pal)
    storage_gray = FakeStorage(content=gray)

    out_pal = preview_helpers.generate_preview('pal.png', is_vid=False, storage=storage_pal)
    img_pal = Image.open(BytesIO(out_pal))
    assert img_pal.mode == 'RGB'

    out_gray = preview_helpers.generate_preview('gray.png', is_vid=False, storage=storage_gray)
    img_gray = Image.open(BytesIO(out_gray))
    # grayscale may remain 'L' or be converted; accept either but ensure a valid image
    assert img_gray.mode in ('L', 'RGB')


def test_generate_preview_large_image_resized(tmp_path):
    preview_helpers.CACHE_DIR = str(tmp_path / "cache_large")
    large = make_png_bytes(mode='RGB', size=(2048, 1024))
    storage = FakeStorage(content=large)

    out = preview_helpers.generate_preview('big.png', is_vid=False, storage=storage, size=200)
    img = Image.open(BytesIO(out))
    assert max(img.size) <= 200


def test_async_generate_preview_uses_cache(tmp_path):
    preview_helpers.CACHE_DIR = str(tmp_path / "cache_async_cache")
    data = make_png_bytes(mode='RGB')
    storage = AsyncFakeStorage(content=data)

    out1 = asyncio.run(preview_helpers.async_generate_preview('x.png', is_vid=False, storage=storage))
    assert storage.download_calls == 1

    storage2 = AsyncFakeStorage(content=b'different')
    out2 = asyncio.run(preview_helpers.async_generate_preview('x.png', is_vid=False, storage=storage2))
    assert out2 == out1
    assert storage2.download_calls == 0

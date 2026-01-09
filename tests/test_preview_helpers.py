import asyncio
import os
from io import BytesIO
from PIL import Image

import pytest

from llm_memedescriber import preview_helpers
from tests._helpers import FakeStorage, AsyncFakeStorage


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


def test_cache_with_invalid_contents_returns_cache_and_skips_storage(tmp_path):
    preview_helpers.CACHE_DIR = str(tmp_path / "cache_invalid")
    cache_path = preview_helpers._cache_path('img.png')
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    with open(cache_path, 'wb') as f:
        f.write(b'not an image')

    storage = FakeStorage(content=make_png_bytes(mode='RGB'))
    out = preview_helpers.generate_preview('img.png', is_vid=False, storage=storage)
    assert out == b'not an image'
    assert storage.download_calls == 0


def test_unreadable_cache_falls_back_to_storage(tmp_path, monkeypatch):
    preview_helpers.CACHE_DIR = str(tmp_path / "cache_invalid2")
    cache_path = preview_helpers._cache_path('img2.png')
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    with open(cache_path, 'wb') as f:
        f.write(b'not an image')

    import builtins as _builtins
    real_open = _builtins.open

    def fake_open(path, mode='r', *args, **kwargs):
        if path == cache_path and 'r' in mode:
            raise RuntimeError('cannot read cache')
        return real_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(_builtins, 'open', fake_open)

    storage = FakeStorage(content=make_png_bytes(mode='RGB'))
    out = preview_helpers.generate_preview('img2.png', is_vid=False, storage=storage)
    assert storage.download_calls == 1
    assert out != b'not an image'

    monkeypatch.setattr(_builtins, 'open', real_open)


def test_generate_preview_handles_LA_mode(tmp_path):
    preview_helpers.CACHE_DIR = str(tmp_path / "cache_LA")
    data = make_png_bytes(mode='LA', color=(128, 128))
    storage = FakeStorage(content=data)

    out = preview_helpers.generate_preview('la.png', is_vid=False, storage=storage)
    img = Image.open(BytesIO(out))
    assert img.mode == 'RGB'


def test_async_generate_preview_logs_and_raises_on_unexpected_exception(tmp_path, caplog):
    preview_helpers.CACHE_DIR = str(tmp_path / "cache_async_err")

    class BadAsyncStorage(AsyncFakeStorage):
        async def async_download_file(self, filename):
            raise RuntimeError('boom')

    storage = BadAsyncStorage(content=None)
    caplog.set_level('ERROR')

    with pytest.raises(RuntimeError):
        asyncio.run(preview_helpers.async_generate_preview('x.png', is_vid=False, storage=storage))

    assert any('Failed to generate preview for' in r.getMessage() for r in caplog.records)


def test_cache_path_md5(tmp_path):
    preview_helpers.CACHE_DIR = str(tmp_path / "cache_md5")
    import hashlib
    name = 'somefile.png'
    expected = hashlib.md5(name.encode()).hexdigest()
    path = preview_helpers._cache_path(name)
    assert path.endswith(expected + '.jpg')


def test_generate_preview_handles_animated_gif(tmp_path):
    preview_helpers.CACHE_DIR = str(tmp_path / "cache_gif")
    # create simple 2-frame GIF
    f1 = Image.new('RGB', (64, 64), (255, 0, 0))
    f2 = Image.new('RGB', (64, 64), (0, 255, 0))
    with BytesIO() as bio:
        f1.save(bio, format='GIF', save_all=True, append_images=[f2], loop=0)
        gif_bytes = bio.getvalue()

    storage = FakeStorage(content=gif_bytes)
    out = preview_helpers.generate_preview('anim.gif', is_vid=False, storage=storage)
    img = Image.open(BytesIO(out))
    assert img.format == 'JPEG'


def test_generate_preview_video_missing_raises_FileNotFoundError(tmp_path):
    preview_helpers.CACHE_DIR = str(tmp_path / "cache_vid_missing")
    storage = FakeStorage(content=None)

    with pytest.raises(FileNotFoundError):
        preview_helpers.generate_preview('novid.mp4', is_vid=True, storage=storage)


def test_generate_preview_raises_when_storage_missing_method_sync(tmp_path):
    preview_helpers.CACHE_DIR = str(tmp_path / "cache_nomethod")

    class NoExtract:  # no extract method
        pass

    with pytest.raises(AttributeError):
        preview_helpers.generate_preview('v.mp4', is_vid=True, storage=NoExtract())


def test_async_generate_preview_raises_when_storage_has_no_methods(tmp_path, caplog):
    preview_helpers.CACHE_DIR = str(tmp_path / "cache_async_nomethods")

    class NoMethods:
        pass

    caplog.set_level('ERROR')
    with pytest.raises(AttributeError):
        asyncio.run(preview_helpers.async_generate_preview('x.png', is_vid=False, storage=NoMethods()))
    assert any('Failed to generate preview for' in r.getMessage() for r in caplog.records)


def test_generate_preview_alpha_transparency_center_white(tmp_path):
    preview_helpers.CACHE_DIR = str(tmp_path / "cache_alpha")
    # create RGBA image with transparent center
    w, h = 64, 64
    img = Image.new('RGBA', (w, h), (255, 0, 0, 255))
    for x in range(22, 42):
        for y in range(22, 42):
            img.putpixel((x, y), (0, 0, 0, 0))
    with BytesIO() as bio:
        img.save(bio, format='PNG')
        data = bio.getvalue()

    storage = FakeStorage(content=data)
    out = preview_helpers.generate_preview('alpha.png', is_vid=False, storage=storage, size=64)
    res = Image.open(BytesIO(out))
    cx, cy = w // 2, h // 2
    px = res.getpixel((cx, cy))
    # ensure center pixel is close to white after conversion & compression
    assert isinstance(px, tuple) and all(c >= 230 for c in px[:3])


def test_async_generate_preview_video_frame_async_method(tmp_path):
    preview_helpers.CACHE_DIR = str(tmp_path / "cache_async_vid")
    frame = make_png_bytes(mode='RGB')
    storage = AsyncFakeStorage(content=frame)

    out = asyncio.run(preview_helpers.async_generate_preview('v.mp4', is_vid=True, storage=storage))
    assert isinstance(out, (bytes, bytearray))
    assert storage.extract_calls == 1
    cache_path = preview_helpers._cache_path('v.mp4')
    assert os.path.exists(cache_path)


def test_async_generate_preview_video_missing_raises_FileNotFoundError_async(tmp_path):
    preview_helpers.CACHE_DIR = str(tmp_path / "cache_async_vid2")
    storage = AsyncFakeStorage(content=None)

    with pytest.raises(FileNotFoundError):
        asyncio.run(preview_helpers.async_generate_preview('novid.mp4', is_vid=True, storage=storage))


def test_async_generate_preview_video_uses_sync_method(tmp_path):
    preview_helpers.CACHE_DIR = str(tmp_path / "cache_async_vid3")
    frame = make_png_bytes(mode='RGB')
    storage = FakeStorage(content=frame)

    out = asyncio.run(preview_helpers.async_generate_preview('v2.mp4', is_vid=True, storage=storage))
    assert isinstance(out, (bytes, bytearray))
    assert storage.extract_calls == 1


def test_async_paletted_image_handling(tmp_path):
    preview_helpers.CACHE_DIR = str(tmp_path / "cache_async_pal")
    pal = make_paletted_bytes()
    storage = AsyncFakeStorage(content=pal)

    out = asyncio.run(preview_helpers.async_generate_preview('p.png', is_vid=False, storage=storage))
    img = Image.open(BytesIO(out))
    assert img.mode == 'RGB'

def _make_webp_bytes_or_skip(mode='RGB', size=(64, 64), color=(128, 128, 128, 255)):
    img = Image.new(mode, size, color)
    with BytesIO() as bio:
        img.save(bio, format='WEBP')
        return bio.getvalue()


def test_generate_preview_handles_webp_sync(tmp_path):
    preview_helpers.CACHE_DIR = str(tmp_path / "cache_webp")
    webp = _make_webp_bytes_or_skip(mode='RGB')
    storage = FakeStorage(content=webp)

    out = preview_helpers.generate_preview('img.webp', is_vid=False, storage=storage)
    img = Image.open(BytesIO(out))
    assert img.format == 'JPEG'
    assert img.mode == 'RGB'


def test_generate_preview_handles_webp_with_alpha(tmp_path):
    preview_helpers.CACHE_DIR = str(tmp_path / "cache_webp_alpha")
    webp = _make_webp_bytes_or_skip(mode='RGBA', color=(10, 20, 30, 128))
    storage = FakeStorage(content=webp)

    out = preview_helpers.generate_preview('img_alpha.webp', is_vid=False, storage=storage)
    img = Image.open(BytesIO(out))
    assert img.mode == 'RGB'


def test_async_generate_preview_handles_webp(tmp_path):
    preview_helpers.CACHE_DIR = str(tmp_path / "cache_webp_async")
    webp = _make_webp_bytes_or_skip(mode='RGB')
    storage = AsyncFakeStorage(content=webp)

    out = asyncio.run(preview_helpers.async_generate_preview('a.webp', is_vid=False, storage=storage))
    img = Image.open(BytesIO(out))
    assert img.format == 'JPEG'


def test_generate_preview_large_webp_resized(tmp_path):
    preview_helpers.CACHE_DIR = str(tmp_path / "cache_webp_large")
    webp = _make_webp_bytes_or_skip(mode='RGB', size=(1024, 1024))
    storage = FakeStorage(content=webp)

    out = preview_helpers.generate_preview('big.webp', is_vid=False, storage=storage, size=200)
    img = Image.open(BytesIO(out))
    assert max(img.size) <= 200


def test_async_cache_write_failure_logged(tmp_path, monkeypatch, caplog):
    preview_helpers.CACHE_DIR = str(tmp_path / "cache_async_write_fail")
    storage = AsyncFakeStorage(content=make_png_bytes(mode='RGB'))

    cache_path = preview_helpers._cache_path('write_fail.png')

    import builtins as _builtins
    real_open = _builtins.open

    def fake_open(path, mode='r', *args, **kwargs):
        if path == cache_path and 'w' in mode:
            raise RuntimeError('open failed')
        return real_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(_builtins, 'open', fake_open)
    caplog.set_level('DEBUG')

    try:
        out = asyncio.run(preview_helpers.async_generate_preview('write_fail.png', is_vid=False, storage=storage))
        assert isinstance(out, (bytes, bytearray))
        assert any('Failed to write preview cache for' in r.getMessage() for r in caplog.records)
    finally:
        monkeypatch.setattr(_builtins, 'open', real_open)


def test_generate_preview_raises_on_corrupted_image(tmp_path):
    preview_helpers.CACHE_DIR = str(tmp_path / "cache_corrupt")
    storage = FakeStorage(content=b'not an image')

    with pytest.raises(Exception):
        preview_helpers.generate_preview('corrupt.png', is_vid=False, storage=storage)


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

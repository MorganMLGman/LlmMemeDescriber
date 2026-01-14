"""Tests for preview cache persistence functionality."""

import json

from llm_memedescriber import preview_helpers


def test_save_preview_cache_creates_manifest(tmp_path):
    """Test that save_preview_cache creates a manifest with cache files."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    manifest_dir = tmp_path / "data" / "preview_cache"
    manifest_file = manifest_dir / "cache_manifest.json"
    
    test_files = ["abc123.jpg", "def456.jpg", "ghi789.jpg"]
    for filename in test_files:
        (cache_dir / filename).write_bytes(b"fake image data")
    
    preview_helpers.CACHE_DIR = str(cache_dir)
    preview_helpers.PREVIEW_CACHE_METADATA = str(manifest_file)
    
    count = preview_helpers.save_preview_cache()
    
    assert count == 3
    assert manifest_file.exists()
    
    with open(manifest_file, 'r') as f:
        manifest = json.load(f)
    
    assert manifest['count'] == 3
    assert set(manifest['cached_previews']) == set(test_files)


def test_save_preview_cache_empty_directory(tmp_path):
    """Test that save_preview_cache handles empty cache directory."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    manifest_dir = tmp_path / "data" / "preview_cache"
    manifest_file = manifest_dir / "cache_manifest.json"
    
    preview_helpers.CACHE_DIR = str(cache_dir)
    preview_helpers.PREVIEW_CACHE_METADATA = str(manifest_file)
    
    count = preview_helpers.save_preview_cache()
    
    assert count == 0
    assert manifest_file.exists()
    
    with open(manifest_file, 'r') as f:
        manifest = json.load(f)
    
    assert manifest['count'] == 0
    assert manifest['cached_previews'] == []


def test_save_preview_cache_ignores_non_jpg_files(tmp_path):
    """Test that save_preview_cache only includes .jpg files."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    manifest_dir = tmp_path / "data" / "preview_cache"
    manifest_file = manifest_dir / "cache_manifest.json"
    
    (cache_dir / "img1.jpg").write_bytes(b"data")
    (cache_dir / "img2.jpg").write_bytes(b"data")
    (cache_dir / "other.png").write_bytes(b"data")
    (cache_dir / "readme.txt").write_bytes(b"data")
    
    preview_helpers.CACHE_DIR = str(cache_dir)
    preview_helpers.PREVIEW_CACHE_METADATA = str(manifest_file)
    
    count = preview_helpers.save_preview_cache()
    
    assert count == 2
    
    with open(manifest_file, 'r') as f:
        manifest = json.load(f)
    
    assert all(f.endswith('.jpg') for f in manifest['cached_previews'])


def test_restore_preview_cache_missing_manifest(tmp_path, caplog):
    """Test that restore_preview_cache handles missing manifest gracefully."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    manifest_file = tmp_path / "data" / "preview_cache" / "cache_manifest.json"
    
    preview_helpers.CACHE_DIR = str(cache_dir)
    preview_helpers.PREVIEW_CACHE_METADATA = str(manifest_file)
    
    caplog.set_level('INFO')
    count = preview_helpers.restore_preview_cache()
    
    assert count == 0
    assert "No preview cache manifest found" in caplog.text


def test_restore_preview_cache_restores_files(tmp_path):
    """Test that restore_preview_cache copies files back to cache."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    manifest_dir = tmp_path / "data" / "preview_cache"
    manifest_dir.mkdir(parents=True)
    manifest_file = manifest_dir / "cache_manifest.json"
    
    test_files = ["abc123.jpg", "def456.jpg", "ghi789.jpg"]
    for filename in test_files:
        (manifest_dir / filename).write_bytes(b"fake image data")
    
    manifest = {
        'cached_previews': test_files,
        'count': len(test_files)
    }
    with open(manifest_file, 'w') as f:
        json.dump(manifest, f, indent=2)
    
    preview_helpers.CACHE_DIR = str(cache_dir)
    preview_helpers.PREVIEW_CACHE_METADATA = str(manifest_file)
    
    count = preview_helpers.restore_preview_cache()
    
    assert count == 3
    
    for filename in test_files:
        assert (cache_dir / filename).exists()
        assert (cache_dir / filename).read_bytes() == b"fake image data"


def test_restore_preview_cache_skips_existing_files(tmp_path):
    """Test that restore_preview_cache doesn't overwrite existing files."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    manifest_dir = tmp_path / "data" / "preview_cache"
    manifest_dir.mkdir(parents=True)
    manifest_file = manifest_dir / "cache_manifest.json"
    
    (manifest_dir / "file1.jpg").write_bytes(b"source data")
    (manifest_dir / "file2.jpg").write_bytes(b"source data")
    
    (cache_dir / "file1.jpg").write_bytes(b"existing data")
    
    manifest = {
        'cached_previews': ["file1.jpg", "file2.jpg"],
        'count': 2
    }
    with open(manifest_file, 'w') as f:
        json.dump(manifest, f, indent=2)
    
    preview_helpers.CACHE_DIR = str(cache_dir)
    preview_helpers.PREVIEW_CACHE_METADATA = str(manifest_file)
    
    count = preview_helpers.restore_preview_cache()
    
    assert count == 1
    assert (cache_dir / "file1.jpg").read_bytes() == b"existing data"
    assert (cache_dir / "file2.jpg").read_bytes() == b"source data"


def test_restore_preview_cache_handles_invalid_manifest(tmp_path, caplog):
    """Test that restore_preview_cache handles corrupted manifest."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    manifest_dir = tmp_path / "data" / "preview_cache"
    manifest_dir.mkdir(parents=True)
    manifest_file = manifest_dir / "cache_manifest.json"
    
    manifest_file.write_text("invalid json {")
    
    preview_helpers.CACHE_DIR = str(cache_dir)
    preview_helpers.PREVIEW_CACHE_METADATA = str(manifest_file)
    
    caplog.set_level('ERROR')
    count = preview_helpers.restore_preview_cache()
    
    assert count == 0
    assert "Failed to restore preview cache" in caplog.text


def test_cache_persistence_full_workflow(tmp_path):
    """Test complete save and restore workflow."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    manifest_dir = tmp_path / "data" / "preview_cache"
    manifest_file = manifest_dir / "cache_manifest.json"
    
    test_files = ["preview_00.jpg", "preview_01.jpg", "preview_02.jpg", 
                  "preview_03.jpg", "preview_04.jpg"]
    for filename in test_files:
        (cache_dir / filename).write_bytes(b"image data " + filename.encode())
    
    preview_helpers.CACHE_DIR = str(cache_dir)
    preview_helpers.PREVIEW_CACHE_METADATA = str(manifest_file)
    
    saved_count = preview_helpers.save_preview_cache()
    assert saved_count == 5
    assert manifest_file.exists()
    
    for f in cache_dir.glob("*.jpg"):
        f.unlink()
    assert len(list(cache_dir.glob("*.jpg"))) == 0
    
    restored_count = preview_helpers.restore_preview_cache()
    assert restored_count == 5
    
    assert len(list(cache_dir.glob("*.jpg"))) == 5
    for filename in test_files:
        assert (cache_dir / filename).exists()
        assert (cache_dir / filename).read_bytes() == b"image data " + filename.encode()

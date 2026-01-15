"""Tests for preview cache persistence functionality."""

import json

from llm_memedescriber import preview_helpers


def test_save_preview_cache_creates_manifest(tmp_path):
    """Test that save_preview_cache creates a manifest with cache files."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    
    test_files = ["abc123.jpg", "def456.jpg", "ghi789.jpg"]
    for filename in test_files:
        (cache_dir / filename).write_bytes(b"fake image data")
    
    manifest_file = cache_dir / "cache_manifest.json"
    
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
    manifest_file = cache_dir / "cache_manifest.json"
    
    preview_helpers.CACHE_DIR = str(cache_dir)
    preview_helpers.PREVIEW_CACHE_METADATA = str(manifest_file)
    
    count = preview_helpers.save_preview_cache()
    
    assert count == 0
    assert manifest_file.exists()
    
    with open(manifest_file, 'r') as f:
        manifest = json.load(f)
    
    assert manifest['count'] == 0
    assert manifest['cached_previews'] == []


def test_save_preview_cache_ignores_empty_files(tmp_path):
    """Test that save_preview_cache only includes .jpg files with content."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    manifest_file = cache_dir / "cache_manifest.json"
    
    (cache_dir / "img1.jpg").write_bytes(b"data")
    (cache_dir / "img2.jpg").write_bytes(b"data")
    (cache_dir / "img_empty.jpg").write_bytes(b"")  # Empty file - should be excluded
    (cache_dir / "other.png").write_bytes(b"data")
    (cache_dir / "readme.txt").write_bytes(b"data")
    
    preview_helpers.CACHE_DIR = str(cache_dir)
    preview_helpers.PREVIEW_CACHE_METADATA = str(manifest_file)
    
    count = preview_helpers.save_preview_cache()
    
    # Should only count files with content, not empty files
    assert count == 2
    
    with open(manifest_file, 'r') as f:
        manifest = json.load(f)
    
    assert len(manifest['cached_previews']) == 2
    assert 'img_empty.jpg' not in manifest['cached_previews']
    assert all(f.endswith('.jpg') for f in manifest['cached_previews'])


def test_restore_preview_cache_missing_manifest(tmp_path, caplog):
    """Test that restore_preview_cache handles missing manifest gracefully."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    manifest_file = cache_dir / "cache_manifest.json"
    
    preview_helpers.CACHE_DIR = str(cache_dir)
    preview_helpers.PREVIEW_CACHE_METADATA = str(manifest_file)
    
    caplog.set_level('INFO')
    count = preview_helpers.restore_preview_cache()
    
    assert count == 0
    assert "No preview cache manifest found" in caplog.text


def test_restore_preview_cache_restores_files(tmp_path):
    """Test that restore_preview_cache verifies cached files exist."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    manifest_file = cache_dir / "cache_manifest.json"
    
    test_files = ["abc123.jpg", "def456.jpg", "ghi789.jpg"]
    for filename in test_files:
        (cache_dir / filename).write_bytes(b"fake image data")
    
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
    """Test that restore_preview_cache verifies files that don't exist."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    manifest_file = cache_dir / "cache_manifest.json"
    
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
    assert (cache_dir / "file1.jpg").exists()
    assert not (cache_dir / "file2.jpg").exists()


def test_restore_preview_cache_handles_invalid_manifest(tmp_path, caplog):
    """Test that restore_preview_cache handles corrupted manifest."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    manifest_file = cache_dir / "cache_manifest.json"
    
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
    manifest_file = cache_dir / "cache_manifest.json"
    
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
    assert restored_count == 0


def test_save_preview_cache_stores_actual_files(tmp_path):
    """Test that save_preview_cache creates manifest for files in persistent storage."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    manifest_file = cache_dir / "cache_manifest.json"
    
    test_files = {
        "preview_001.jpg": b"JPEG data for file 001",
        "preview_002.jpg": b"JPEG data for file 002",
        "preview_003.jpg": b"JPEG data for file 003",
    }
    
    for filename, content in test_files.items():
        (cache_dir / filename).write_bytes(content)
    
    preview_helpers.CACHE_DIR = str(cache_dir)
    preview_helpers.PREVIEW_CACHE_METADATA = str(manifest_file)
    
    saved_count = preview_helpers.save_preview_cache()
    assert saved_count == 3
    
    assert manifest_file.exists()
    manifest_data = json.load(open(manifest_file))
    assert manifest_data['count'] == 3
    
    stored_files = list(cache_dir.glob("*.jpg"))
    assert len(stored_files) == 3
    
    for filename, expected_content in test_files.items():
        stored_file = cache_dir / filename
        assert stored_file.exists(), f"File {filename} not found in {cache_dir}"
        assert stored_file.read_bytes() == expected_content, \
            f"Content mismatch for {filename}"


def test_save_and_restore_preserves_file_integrity(tmp_path):
    """Test that files maintain their content through save/restore cycle."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    manifest_file = cache_dir / "cache_manifest.json"
    
    preview_data = {
        "hash_abc123.jpg": b"\xff\xd8\xff\xe0" + b"JPEG" * 100,  # JPEG header + data
        "hash_def456.jpg": b"\xff\xd8\xff\xe1" + b"EXIF" * 50,   # JPEG with EXIF
        "hash_ghi789.jpg": b"\x89PNG\r\n\x1a\n" + b"PNG" * 75,   # PNG header + data
    }
    
    for filename, content in preview_data.items():
        (cache_dir / filename).write_bytes(content)
    
    preview_helpers.CACHE_DIR = str(cache_dir)
    preview_helpers.PREVIEW_CACHE_METADATA = str(manifest_file)
    
    saved_count = preview_helpers.save_preview_cache()
    assert saved_count == 3
    
    for filename in preview_data.keys():
        assert (cache_dir / filename).exists()
    
    restored_count = preview_helpers.restore_preview_cache()
    assert restored_count == 3
    
    for filename, original_content in preview_data.items():
        cached_file = cache_dir / filename
        assert cached_file.exists(), f"File {filename} not found"
        cached_content = cached_file.read_bytes()
        assert cached_content == original_content, \
            f"Content mismatch for {filename}. Expected {len(original_content)} bytes, got {len(cached_content)}"


def test_cleanup_orphaned_cache(tmp_path):
    """Test that cleanup_orphaned_cache removes cache files for removed memes."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    
    preview_helpers.CACHE_DIR = str(cache_dir)
    
    # Create some cache files by hashing filenames
    valid_filenames = {"image1.jpg", "image2.jpg"}
    orphaned_filenames = {"removed1.jpg", "removed2.jpg"}
    
    # Create cache files for both valid and orphaned files
    import hashlib
    for filename in valid_filenames | orphaned_filenames:
        name_hash = hashlib.md5(filename.encode()).hexdigest()
        cache_file = cache_dir / f"{name_hash}.jpg"
        cache_file.write_bytes(b"fake image data")
    
    # Verify all files exist
    assert len(list(cache_dir.glob("*.jpg"))) == 4
    
    # Clean up orphaned cache
    removed_count = preview_helpers.cleanup_orphaned_cache(valid_filenames)
    
    # Verify that 2 files were removed
    assert removed_count == 2
    assert len(list(cache_dir.glob("*.jpg"))) == 2
    
    # Verify that valid files still exist
    for filename in valid_filenames:
        name_hash = hashlib.md5(filename.encode()).hexdigest()
        assert (cache_dir / f"{name_hash}.jpg").exists()
    
    # Verify that orphaned files were removed
    for filename in orphaned_filenames:
        name_hash = hashlib.md5(filename.encode()).hexdigest()
        assert not (cache_dir / f"{name_hash}.jpg").exists()


def test_cleanup_orphaned_cache_empty_valid_set(tmp_path):
    """Test cleanup_orphaned_cache with empty valid filenames set removes all cache."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    
    preview_helpers.CACHE_DIR = str(cache_dir)
    
    # Create some cache files
    for i in range(3):
        (cache_dir / f"cache_{i}.jpg").write_bytes(b"fake image data")
    
    assert len(list(cache_dir.glob("*.jpg"))) == 3
    
    # Clean up with empty valid set should remove all files
    removed_count = preview_helpers.cleanup_orphaned_cache(set())
    
    assert removed_count == 3
    assert len(list(cache_dir.glob("*.jpg"))) == 0


def test_cleanup_orphaned_cache_with_non_jpg_files(tmp_path):
    """Test that cleanup_orphaned_cache ignores non-jpg files."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    
    preview_helpers.CACHE_DIR = str(cache_dir)
    
    # Create some cache files and other files
    (cache_dir / "cache1.jpg").write_bytes(b"fake image data")
    (cache_dir / "cache2.jpg").write_bytes(b"fake image data")
    (cache_dir / "other_file.txt").write_text("text file")
    (cache_dir / "cache_manifest.json").write_text("{}")
    
    assert len(list(cache_dir.glob("*.jpg"))) == 2
    
    # Clean up with empty valid set
    removed_count = preview_helpers.cleanup_orphaned_cache(set())
    
    # Should only remove jpg files
    assert removed_count == 2
    assert (cache_dir / "other_file.txt").exists()
    assert (cache_dir / "cache_manifest.json").exists()
    assert len(list(cache_dir.glob("*.jpg"))) == 0

#!/usr/bin/env python3
"""
Build script for bundling and minifying JavaScript files.
Pure Python solution - no Node.js/npm required!
"""

import gzip
import json
import os
from pathlib import Path


def create_bundle(files, output_path, minify=True, source_map=True):
    """Bundle multiple JS files into one, optionally minify."""
    
    print("🔨 Building JavaScript bundle...")
    
    # Read all source files
    contents = []
    source_files = []
    
    for file_path in files:
        file_path = Path(file_path)
        if not file_path.exists():
            print(f"⚠️  Warning: {file_path} not found, skipping...")
            continue
            
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            contents.append(f'// ===== Source: {file_path.name} =====\n{content}\n')
            source_files.append(file_path.name)
    
    # Concatenate all files
    bundle = '\n'.join(contents)
    
    # Minify if requested
    if minify:
        try:
            import rjsmin
            print("✨ Minifying with rjsmin...")
            bundle = rjsmin.jsmin(bundle, keep_bang_comments=True)
            print(f"📦 Minification complete!")
        except ImportError:
            print("⚠️  rjsmin not installed. Install with: pip install rjsmin")
            print("📦 Saving unminified bundle...")
            minify = False
    
    # Write bundle
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(bundle)
    
    # Create source map
    if source_map:
        create_source_map(source_files, output_path, bundle)
    
    # Create gzip version (for static file serving)
    create_gzip(output_path)
    
    # Stats
    original_size = sum(Path(f).stat().st_size for f in files if Path(f).exists())
    bundle_size = output_path.stat().st_size
    gz_size = (output_path.with_suffix(output_path.suffix + '.gz')).stat().st_size if (output_path.with_suffix(output_path.suffix + '.gz')).exists() else 0
    reduction = ((original_size - bundle_size) / original_size * 100) if original_size > 0 else 0
    gz_reduction = ((bundle_size - gz_size) / bundle_size * 100) if bundle_size > 0 else 0
    
    print(f"\n✅ Bundle created: {output_path}")
    print(f"📊 Original size: {original_size:,} bytes")
    print(f"📊 Bundle size: {bundle_size:,} bytes")
    if minify:
        print(f"📉 Minification reduction: {reduction:.1f}%")
    if gz_size > 0:
        print(f"📦 Gzipped size: {gz_size:,} bytes ({gz_reduction:.1f}% smaller than minified)")
    
    return output_path


def create_source_map(source_files, output_path, content):
    """Create a simple source map for debugging."""
    
    source_map = {
        "version": 3,
        "file": output_path.name,
        "sources": source_files,
        "sourcesContent": [],
        "names": [],
        "mappings": ""
    }
    
    # Read source contents for sourcesContent
    base_dir = Path('llm_memedescriber/static')
    for source_file in source_files:
        source_path = base_dir / source_file
        if source_path.exists():
            with open(source_path, 'r', encoding='utf-8') as f:
                source_map["sourcesContent"].append(f.read())
        else:
            source_map["sourcesContent"].append("")
    
    # Write source map
    map_path = output_path.with_suffix(output_path.suffix + '.map')
    with open(map_path, 'w', encoding='utf-8') as f:
        json.dump(source_map, f, indent=2)
    
    # Add source map reference to bundle
    with open(output_path, 'a', encoding='utf-8') as f:
        f.write(f'\n//# sourceMappingURL={map_path.name}\n')
    
    print(f"🗺️  Source map created: {map_path}")


def create_gzip(output_path):
    """Create gzipped version of bundle for static file serving."""
    
    output_path = Path(output_path)
    gz_path = output_path.with_suffix(output_path.suffix + '.gz')
    
    with open(output_path, 'rb') as f_in:
        with gzip.open(gz_path, 'wb', compresslevel=9) as f_out:
            f_out.writelines(f_in)
    
    print(f"🗜️  Gzipped bundle created: {gz_path}")


def main():
    """Main build function."""
    
    # Define the build order (must match dependency chain)
    static_dir = Path('llm_memedescriber/static')
    
    source_files = [
        static_dir / 'core.js',
        static_dir / 'api.js',
        static_dir / 'auth-search.js',
        static_dir / 'memes-ui.js',
        static_dir / 'meme-actions.js',
        static_dir / 'deduplication.js',
        static_dir / 'sync-download.js',
    ]
    
    # Build production bundle (minified)
    print("=" * 60)
    print("Building PRODUCTION bundle (minified)...")
    print("=" * 60)
    create_bundle(
        files=source_files,
        output_path=static_dir / 'bundle.min.js',
        minify=True,
        source_map=True
    )
    
    print("\n" + "=" * 60)
    print("Building DEVELOPMENT bundle (unminified)...")
    print("=" * 60)
    create_bundle(
        files=source_files,
        output_path=static_dir / 'bundle.js',
        minify=False,
        source_map=True
    )
    
    print("\n" + "=" * 60)
    print("✅ Build complete!")
    print("=" * 60)
    print("\n💡 Usage:")
    print("   Production: Use bundle.min.js (minified)")
    print("   Development: Use bundle.js (readable with source maps)")
    print("\n📝 To install rjsmin for minification:")
    print("   pip install rjsmin")


if __name__ == '__main__':
    main()

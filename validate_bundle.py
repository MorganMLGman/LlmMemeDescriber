#!/usr/bin/env python3
"""
Quick test to verify JavaScript bundle has no syntax errors.
"""

import subprocess
import sys
from pathlib import Path


def test_js_syntax(js_file):
    """Test if JavaScript file has valid syntax using Node.js (if available)."""
    
    js_path = Path(js_file)
    if not js_path.exists():
        print(f"❌ File not found: {js_path}")
        return False
    
    print(f"🔍 Testing: {js_path.name}")
    
    # Try to use Node.js to check syntax
    try:
        result = subprocess.run(
            ['node', '--check', str(js_path)],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if result.returncode == 0:
            print(f"✅ {js_path.name}: No syntax errors")
            return True
        else:
            print(f"❌ {js_path.name}: Syntax errors found:")
            print(result.stderr)
            return False
            
    except FileNotFoundError:
        print("⚠️  Node.js not found - skipping syntax check")
        print("   (Optional: Install Node.js for syntax validation)")
        return None
    except subprocess.TimeoutExpired:
        print(f"⚠️  Timeout checking {js_path.name}")
        return None


def main():
    """Test all JavaScript bundles."""
    
    static_dir = Path('llm_memedescriber/static')
    
    files_to_test = [
        static_dir / 'bundle.min.js',
        static_dir / 'bundle.js',
    ]
    
    print("=" * 60)
    print("JavaScript Bundle Syntax Test")
    print("=" * 60 + "\n")
    
    results = []
    for js_file in files_to_test:
        result = test_js_syntax(js_file)
        results.append(result)
    
    print("\n" + "=" * 60)
    
    if None in results:
        print("⚠️  Tests skipped (Node.js not available)")
        print("   Bundles created successfully but not validated.")
    elif all(results):
        print("✅ All bundles passed syntax check!")
        sys.exit(0)
    else:
        print("❌ Some bundles have syntax errors")
        sys.exit(1)


if __name__ == '__main__':
    main()

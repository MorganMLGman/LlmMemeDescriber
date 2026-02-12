# JavaScript Build Process

## Overview

Frontend JavaScript has been split into modular files and bundled for production use.

## Files Structure

### Source Files (in `llm_memedescriber/static/`)
- `core.js` - State management, utilities, UI helpers
- `api.js` - HTTP communication and CSRF management
- `auth-search.js` - Authentication, search, prompt management
- `memes-ui.js` - Meme rendering, modal, infinite scroll
- `meme-actions.js` - Download, clipboard operations
- `deduplication.js` - Duplicate management
- `sync-download.js` - Sync and video download features

### Generated Files (automatically created)
- `bundle.min.js` - Production bundle (minified, ~51KB)
- `bundle.min.js.map` - Source map for debugging
- `bundle.js` - Development bundle (readable, ~76KB)
- `bundle.js.map` - Development source map

## Building

### Prerequisites
```bash
# Activate virtual environment
source .venv/bin/activate

# Install rjsmin (already in Pipfile dev dependencies)
pip install rjsmin
```

### Build Command
```bash
# Build both production and development bundles
python3 build_js.py
```

This creates:
1. **Production bundle** (`bundle.min.js`) - minified, 32% smaller
2. **Development bundle** (`bundle.js`) - readable with comments
3. **Source maps** for both versions (`.map` files)

## Development Workflow

### Making Changes
1. Edit source files (`core.js`, `api.js`, etc.)
2. Run `python3 build_js.py` to rebuild bundles
3. Refresh browser (cache-busting via `?v=1` query parameter)

### Using Development Bundle
Change HTML templates temporarily to use:
```html
<script src="/static/bundle.js?v=1"></script>
```

### Production Deployment
Ensure `bundle.min.js` is used in templates:
```html
<script src="/static/bundle.min.js?v=1"></script>
```

## Debugging

Source maps are automatically included, allowing you to:
- See original source files in browser DevTools
- Set breakpoints in original code
- Get stack traces with original file names and line numbers

## Statistics

- **Original files:** 77,498 bytes (7 files)
- **Minified bundle:** 52,569 bytes (-32.2%)
- **With gzip:** ~15-20KB (estimated)

## Benefits

✅ **Single HTTP request** instead of 7
✅ **32% size reduction** via minification
✅ **Source maps** for easy debugging
✅ **Pure Python** - no Node.js/npm required
✅ **Fast builds** - under 1 second

## Docker Build Integration

The JavaScript bundle is **automatically built in GitHub Actions CI/CD pipeline** before Docker images are created.

### How it works:

1. **GitHub Actions workflow** (`build-js-bundle.yml`):
   - Installs Python + `rjsmin`
   - Runs `python build_js.py` to generate bundles
   - Uploads `bundle.min.js` and `bundle.min.js.map` as artifacts

2. **Docker build workflow** (`docker-multiarch.yml`):
   - Downloads JS bundle artifact
   - Bundles are present in workspace before `docker build`
   - Dockerfile simply copies them (no build logic needed)

### Local Development:

For local Docker builds, **you must build the bundle first**:
```bash
# 1. Build bundle locally
source .venv/bin/activate
python3 build_js.py

# 2. Build Docker image (uses local bundle)
docker build -t llm-meme-describer .
```

### CI/CD Builds:

In GitHub Actions, bundle is automatically built and downloaded - **no manual steps needed!**

## Benefits of GitHub Actions Approach

✅ **Simpler Dockerfile** - no dev dependencies, no build logic  
✅ **Faster builds** - bundle reused across platforms (amd64/arm64)  
✅ **Cleaner separation** - build tools not in production image  
✅ **Artifact caching** - bundle built once, used multiple times

### Important Notes:
- ✅ Bundle is built **inside Docker** - no need to build locally before Docker build
- ✅ Source JS files are included for source maps to work
- ✅ `.dockerignore` prevents local bundles from being copied to build context
- ✅ Only production `bundle.min.js` is used in templates

### Troubleshooting Docker Build:

If the JS bundle fails to build in Docker:
```bash
# Check builder stage logs
docker build --target builder -t test-builder .

# Inspect built files
docker run --rm test-builder ls -lh /app/llm_memedescriber/static/bundle*
```

### Local Development vs Docker Workflow:

**Local Development:**
```bash
# 1. Edit source files
vim llm_memedescriber/static/core.js

# 2. Rebuild bundle
source .venv/bin/activate
python3 build_js.py

# 3. Test changes
# (refresh browser)
```

**Docker Deployment:**
```bash
# Just rebuild the image - bundle is built automatically!
docker build -t llm-meme-describer .
docker-compose up -d
```

No manual bundle generation needed for Docker! 🐳

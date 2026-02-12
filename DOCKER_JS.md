# JavaScript Bundle in Docker - Quick Reference

## TL;DR

**CI/CD (GitHub Actions)**: Bundle is automatically built before Docker images - **no action needed!**

**Local Development**: You must build the bundle manually before `docker build`.

## How it works in GitHub Actions

When you push code or create a release:

```
1. build-js Job (GitHub Actions)
   ├─ Checkout code
   ├─ Install Python + rjsmin
   ├─ Run: python3 build_js.py
   ├─ Upload bundle.min.js as artifact
   └─ Upload bundle.min.js.map as artifact

2. build Job (GitHub Actions)  
   ├─ Checkout code
   ├─ Download JS bundle artifact → llm_memedescriber/static/
   ├─ Verify bundle exists
   └─ docker build (bundle already present in workspace)

3. Docker Build
   ├─ Builder: Install ONLY production dependencies (NO rjsmin!)
   ├─ Production: Copy everything (including bundle from workspace)
   └─ Result: Optimized image with bundle, NO build tools!
```

## Key Points

✅ **In CI/CD**: Fully automatic, bundle built in GitHub Actions  
✅ **Locally**: Must run `python3 build_js.py` before docker build  
✅ **Simpler Dockerfile**: No dev dependencies, no build logic  
✅ **Faster**: Bundle built once, reused for amd64 and arm64 images  
✅ **Cleaner**: Build tools (rjsmin) NOT in production image

## Local Development Workflow

### Option 1: Rebuild bundle before Docker (recommended)

```bash
# 1. Edit JS files
vim llm_memedescriber/static/core.js

# 2. Build bundle locally
source .venv/bin/activate
python3 build_js.py

# 3. Build Docker image
docker build -t llm-meme-describer .
```

### Option 2: Test without Docker first

```bash
# 1. Edit JS files
# 2. Build bundle and test locally
python3 build_js.py
# Start app locally and test in browser

# 3. When satisfied, build Docker
docker build -t llm-meme-describer .
```

## File Handling

**Bundle files are NOT committed to repository:**
- `.gitignore` excludes `bundle*.js` and `bundle*.js.map`
- `.dockerignore` also excludes them (for safety)
- In CI/CD: Generated as artifacts
- Locally: Must build manually

## Troubleshooting

### Error: "bundle.min.js not found" during Docker build

**Cause**: Bundle wasn't built locally before `docker build`

**Fix**:
```bash
source .venv/bin/activate
python3 build_js.py
docker build -t llm-meme-describer .
```

### Want to skip bundle rebuild locally?

If you already have bundle from previous build:
```bash
# Check if exists
ls -lh llm_memedescriber/static/bundle.min.js

# If exists, just build Docker
docker build -t llm-meme-describer .
```

### Bundle not updating in browser?

Clear cache or increment version in templates:
```html
<script src="/static/bundle.min.js?v=2"></script>
```

## GitHub Actions Workflows

### build-js-bundle.yml
- **Triggers**: Push to main, PR, manual dispatch
- **Paths**: Only runs when JS files or build script change
- **Output**: Artifact `js-bundle-{sha}` (retained 7 days)

### docker-multiarch.yml  
- **Dependencies**: Requires `build-js` job to complete first
- **Downloads**: Artifact from `build-js` before building Docker
- **Platforms**: Uses same bundle for linux/amd64 and linux/arm64

## Comparison: Old vs New Approach

### Old (Dockerfile builds JS):
```
Dockerfile:
- Install pipenv with --dev (rjsmin, pytest, etc.)
- Copy build_js.py and source JS
- Run python build_js.py
- Delete entire venv
- Reinstall production dependencies only
- Copy bundle to production stage
❌ Complex Dockerfile
❌ Dev tools temporarily in build
```

### New (GitHub Actions builds JS):
```
GitHub Actions:
- Install rjsmin in temp CI environment
- Build bundle
- Upload as artifact

Dockerfile:
- Install production dependencies only
- Copy bundle from workspace
✅ Simple Dockerfile  
✅ No dev tools in any stage
✅ Faster multi-platform builds
```

## Questions?

See [BUILD_JS.md](BUILD_JS.md) for complete documentation.

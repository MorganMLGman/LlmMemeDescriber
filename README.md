# Llm Meme Describer

Llm Meme Describer analyzes images (memes) with a generative AI model and stores short, searchable descriptions and metadata for quick previewing and de-duplication.

## Highlights ✨
- Generates concise, human-readable descriptions and extracted text (OCR)
- Detects and groups visually similar images (deduplication)
- Serves thumbnails and a simple web UI for browsing and searching
- Provides a small REST API for programmatic access

## Run & access ▶️
Start the service using Docker:

**Option 1: Docker run**
```sh
docker run --rm -p 8000:8000 \
  -e GOOGLE_GENAI_API_KEY="YOUR_API_KEY" \
  -e WEBDAV_URL="https://example.com/remote.php/dav/files/user" \
  -e WEBDAV_USERNAME="username" \
  -e WEBDAV_PASSWORD="password" \
  -e WEBDAV_PATH="/Path/To/Images" \
  morganmlg/llm-meme-describer
```

**Option 2: Docker Compose** (recommended)
```sh
docker-compose -f docker-compose.example.yml up -d
```
See `docker-compose.example.yml` for full configuration options.

By default the web preview is available at http://localhost:8000/

## Useful endpoints 🔧
- Web UI: GET /
- Search by keywords: GET /memes/search/by-keywords?q=your+query&limit=20&offset=0
- Meme details: GET /memes/{filename}
- Thumbnail preview: GET /memes/{filename}/preview?size=600
- Duplicate list: GET /memes/{filename}/duplicates
- App stats: GET /api/stats

## Behavior & notes 💡
- Previews are cached for fast access; images and videos are supported (first video frame used).
- Search uses full-text indexing (filename, description, keywords, OCR text).
- Metadata updates are supported via the REST API (category, keywords, description).

## Configuration ⚙️
Main runtime options are provided as environment variables:

**Required:**
- `GOOGLE_GENAI_API_KEY` — API key for the generative model
- `WEBDAV_URL`, `WEBDAV_USERNAME`, `WEBDAV_PASSWORD`, `WEBDAV_PATH` — remote storage details

**Optional (with defaults):**
- `GOOGLE_GENAI_MODEL` — model id (default: `gemini-2.5-flash`)
- `RUN_INTERVAL` — sync interval (default: `15min`)
- `TIMEZONE` — IANA timezone (default: `UTC`)
- `LOGGING_LEVEL` — log level (default: `INFO`)

For a complete list of all configuration options and Docker secrets setup, see `docker-compose.example.yml`.


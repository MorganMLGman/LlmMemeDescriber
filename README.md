# Llm Meme Describer

Llm Meme Describer analyzes images (memes) with a generative AI model and stores short, searchable descriptions and metadata for quick previewing and de-duplication.

## Highlights ✨
- Generates concise, human-readable descriptions and extracted text (OCR)
- Detects and groups visually similar images (deduplication)
- Serves thumbnails and a simple web UI for browsing and searching
- Provides a small REST API for programmatic access

## Run & access ▶️
Start the service (example using Docker):

```sh
docker run --rm -p 8000:8000 \
  -e GOOGLE_GENAI_API_KEY="YOUR_API_KEY" \
  -e WEBDAV_URL="https://example.com/remote.php/dav/files/user" \
  -e WEBDAV_USERNAME="username" \
  -e WEBDAV_PASSWORD="password" \
  -e WEBDAV_PATH="/Path/To/Images" \
  llm-meme-describer
```

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
Main runtime options are provided as environment variables (examples):
- `GOOGLE_GENAI_API_KEY` — API key for the generative model
- `GOOGLE_GENAI_MODEL` — model id (e.g., `gemini-3-flash-preview`)
- `WEBDAV_*` — connection details for remote storage
- `RUN_INTERVAL`, `TIMEZONE`, `LOGGING_LEVEL` — runtime behavior and logging

See config files in the repository for full details on available settings.


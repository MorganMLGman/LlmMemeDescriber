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
docker run --rm -p 8443:8443 \
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

By default the web preview is available at **https://localhost:8443/** 

⚠️ **Note:** The app uses HTTPS only. Self-signed certificates are automatically generated if none are provided. This is suitable for development and when deployed behind a reverse proxy (e.g., Cloudflare). For production with custom certificates, see the [SSL/TLS Configuration](#ssltls-configuration) section below.

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

### SSL/TLS Configuration

The application uses **HTTPS exclusively** on port `8443`.

#### Auto-generated Self-Signed Certificates (Default)
If no certificate files are provided, the app automatically generates self-signed certificates on startup and stores them in `/data/certs/`. These are regenerated annually and suitable for:
- Local development
- Testing environments
- Deployments behind a reverse proxy (e.g., Cloudflare, nginx, HAProxy)

No additional configuration needed—just run the container!

#### Using Your Own Certificates
For deployments with proper SSL certificates:

1. **Via Docker Secrets** (recommended for Docker Compose):
   ```yaml
   services:
     llm-meme-describer:
       secrets:
         - ssl_cert_file
         - ssl_key_file
   
   secrets:
     ssl_cert_file:
       file: ./certs/server.crt
     ssl_key_file:
       file: ./certs/server.key
   ```

2. **Via Environment Variables:**
   ```bash
   docker run --rm -p 8443:8443 \
     -e SSL_CERT_FILE="/path/to/cert.pem" \
     -e SSL_KEY_FILE="/path/to/key.pem" \
     morganmlg/llm-meme-describer
   ```

#### Certificate Format
- **Certificate:** PEM-encoded X.509 certificate (`.crt` or `.pem`)
- **Private Key:** PEM-encoded RSA private key (`.key` or `.pem`)

#### Generating Self-Signed Certificates (Manual)
If you want to pre-generate certificates:
```bash
openssl req -x509 -newkey rsa:4096 -keyout server.key -out server.crt \
  -days 365 -nodes -subj "/C=US/ST=State/L=City/O=Org/CN=localhost"
```

#### Behind a Reverse Proxy
If using Cloudflare, nginx, or HAProxy:
- The app listens on `https://0.0.0.0:8443`
- Your reverse proxy handles the public HTTPS with a proper certificate
- The app's self-signed certificate secures the internal connection
- No additional configuration needed in the app


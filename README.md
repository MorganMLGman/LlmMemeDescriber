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
- **Database-first approach**: All meme metadata (descriptions, OCR, perceptual hashes) is stored in a local SQLite database.
- **Automatic sync**: On startup and at regular intervals, the service scans WebDAV for new/removed images and updates the database.
- **Previews cached**: Image and video thumbnails are cached locally for fast access; first video frame is extracted for videos.
- **Full-text search**: Uses Whoosh indexing for fast searches across filename, description, keywords, and OCR text.
- **Deduplication**: Automatically detects visually similar images using perceptual hashing.
- **REST API**: Full REST API for programmatic access to metadata and metadata updates (category, keywords, description).

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

## Authentication 🔐

The application supports three mutually exclusive authentication modes:

### 1. Public Mode (Default)
No authentication required. All endpoints are publicly accessible.
```bash
PUBLIC_MODE=true
```

### 2. OIDC Authentication
OpenID Connect authentication via external provider (e.g., Authelia, Keycloak).
```bash
OIDC_ENABLED=true
OIDC_PROVIDER_URL=https://auth.example.com
OIDC_CLIENT_ID=your-client-id
OIDC_CLIENT_SECRET=your-secret
OIDC_REDIRECT_URI=https://your-app.com/auth/callback
JWT_SECRET=random-secret-key
CSRF_SECRET=another-random-key
```

### 3. Basic Auth (HTTP Basic Authentication)
Simple username/password authentication for self-hosted deployments.

**Enable Basic Auth:**
```bash
BASIC_AUTH=true
JWT_SECRET=random-jwt-secret-key
CSRF_SECRET=random-csrf-secret-key
PUBLIC_MODE=false
OIDC_ENABLED=false
```

**Managing Users via CLI:**

All CLI commands require interactive mode (use `-it` flag):
```bash
# Create user
docker exec -it llm-meme-describer python -m llm_memedescriber.cli create-user

# List all users
docker exec llm-meme-describer python -m llm_memedescriber.cli list-users

# Delete user
docker exec -it llm-meme-describer python -m llm_memedescriber.cli delete-user
```

**Security Features:**
- Passwords hashed with Argon2id (256MB memory, 8 threads, 4 iterations)
- Rate limiting: 3 failed attempts → exponential lockout (30s, 1m, 5m, 15m)
- Session-based authentication with HTTP-only JWT cookies
- Custom login form with automatic session management
- All traffic over HTTPS only

**Testing Authentication:**
```bash
# Test with curl
curl -u username:password https://your-app.com/api/memes

# Using browser (automatic Basic Auth prompt)
# Navigate to https://your-app.com and enter credentials
```

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


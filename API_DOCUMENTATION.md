# LlmMemeDescriber API Documentation

Complete reference for all backend API endpoints, authentication methods, and integration patterns.

## Table of Contents

1. [Overview](#overview)
2. [Base Configuration](#base-configuration)
3. [Authentication Guide](#authentication-guide)
4. [Common Patterns](#common-patterns)
5. [Error Responses](#error-responses)
6. [Endpoint Reference](#endpoint-reference)
   - [UI Endpoints](#ui-endpoints)
   - [Authentication & Authorization](#authentication--authorization)
   - [Meme Management](#meme-management)
   - [Deduplication](#deduplication)
   - [Sync & Storage](#sync--storage)
   - [Video Downloads](#video-downloads)
   - [Health Check](#health-check)
7. [Appendices](#appendices)

---

## Overview

The LlmMemeDescriber API is a FastAPI-based REST API for managing, describing, and deduplicating meme image libraries. It features:

- **Multi-mode authentication:** Public, OIDC (OAuth 2.0), and Basic Auth
- **Comprehensive meme management:** CRUD operations, metadata editing, sharing
- **Intelligent deduplication:** Perceptual hashing for duplicate detection
- **Video integration:** Download videos from URLs using yt-dlp
- **Full-text search:** Whoosh-based search across meme descriptions and keywords
- **Security:** CSRF protection, rate limiting, API tokens with expiration
- **Audit logging:** All state-changing operations logged with user context

**Framework:** FastAPI
**Total Endpoints:** 56
**Database:** SQLite with SQLModel ORM
**Authentication:** Multi-mode (Public/OIDC/Basic Auth)

---

## Base Configuration

### API Base URL

**HTTPS Mode** (default):
```
https://localhost:8443/api
https://example.com:8443/api
```

**HTTP Mode** (when `NO_TLS=true`):
```
http://localhost:8080/api
http://example.com:8080/api
```

### Common Headers

All requests should include:

```
Content-Type: application/json
```

Authentication (when required):

```
Authorization: Bearer <api_token>
```

CSRF Protection (for state-changing operations):

```
X-CSRF-Token: <csrf_token>
```

### Environment Variables

Key configuration variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `PUBLIC_MODE` | `true` | Enable public access without authentication |
| `OIDC_ENABLED` | `false` | Enable OIDC/OAuth 2.0 authentication |
| `BASIC_AUTH` | `false` | Enable Basic Auth (username/password) |
| `CSRF_SECRET` | required if auth enabled | Secret for CSRF token generation |
| `JWT_SECRET` | required if auth enabled | Secret for JWT token signing |
| `REDIS_URL` | optional | Redis URL for session storage (default: in-memory) |
| `NO_TLS` | `false` | Disable TLS/SSL (use HTTP instead) |
| `ENABLE_VIDEO_DOWNLOADS` | `false` | Enable video download feature (requires ffmpeg) |

---

## Authentication Guide

### Authentication Modes

The backend supports three mutually exclusive authentication modes:

#### 1. Public Mode (Default)

All endpoints are accessible without authentication. No session or token required.

**When to use:** Development, internal tools, or when authentication is not needed.

```
# All endpoints accessible directly
GET http://localhost:8080/memes
GET http://localhost:8080/api/stats
```

#### 2. OIDC Authentication

OAuth 2.0 / OpenID Connect integration with external providers (e.g., Authelia, Keycloak, Auth0).

**Features:**
- Session-based authentication via cookies
- PKCE (Proof Key for Code Exchange) support
- Optional group-based authorization
- Automatic token refresh

**Session Cookies:**
- `session_id` (HTTPS, SameSite=Lax)
- `session_id_http` (HTTP, SameSite=Lax)

**Session Storage:**
- In-memory (default)
- Redis (when `REDIS_URL` configured)

**Group Authorization:**
- Configure via `OIDC_ALLOWED_GROUPS` environment variable
- Format: comma-separated group names (e.g., `"admins,editors"`)
- Special values: `"all"` (allow all authenticated users), `""` (deny all)

**Flow:**
```
1. User visits /auth/login
2. Redirected to OIDC provider
3. User authenticates
4. Provider redirects to /auth/callback with authorization code
5. Backend exchanges code for ID token
6. Session cookie set
7. User redirected to home page
```

#### 3. Basic Auth

HTTP Basic Authentication with username/password. JWT tokens stored in cookies.

**Features:**
- Argon2id password hashing (strong parameters)
- Rate limiting: 3 failed attempts, exponential backoff lockout
- JWT tokens valid for 24 hours by default

**Session Cookies:**
- `auth_token` (HTTPS, SameSite=Strict)
- `auth_token_http` (HTTP, SameSite=Strict)

**Lockout Progression:**
- Attempt 1: Lockout 30 seconds
- Attempt 2: Lockout 1 minute
- Attempt 3+: Lockout 5 minutes, then 15 minutes

**Flow:**
```
1. User visits /login
2. Enters username/password in form
3. POST /auth/basic-login
4. JWT token set in cookie
5. User redirected to home page
```

### API Token Management

Generate long-lived API tokens for programmatic access.

**Requirements:**
- Must be authenticated (via web UI, session cookie)
- Token shown only once (cannot be retrieved later)
- Can set optional expiration date

**Generation:**
```
POST /api/tokens
Content-Type: application/json
X-CSRF-Token: <csrf_token>

{
  "name": "Production API Token",
  "expires_at": "2025-12-31T23:59:59Z"
}
```

**Response:**
```json
{
  "id": 1,
  "name": "Production API Token",
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "created_at": "2024-01-01T00:00:00Z"
}
```

**Usage in Requests:**
```
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

**Token Properties:**
- Claims: `sub` (user ID), `iat` (issued at), `exp` (expiry), `jti` (token ID)
- Signature verification: HS256
- Revocation: Checked on every request (can be revoked anytime)
- Expiration: Timestamp-based, checked on every request

### CSRF Protection

CSRF tokens are **required for all state-changing operations** (POST, PATCH, DELETE).

**How to use:**

1. Get CSRF token:
```
GET /api/csrf-token
```

2. Response:
```json
{
  "csrf_token": "abc123def456..."
}
```

3. Send token in header for state-changing requests:
```
POST /memes
X-CSRF-Token: abc123def456...
Content-Type: application/json

{
  "filename": "meme.jpg"
}
```

**Cookies:** Automatically managed via `csrftoken` cookie

### Current User Info

Get information about the currently authenticated user:

```
GET /auth/user
```

Response:
```json
{
  "user_id": "john_doe",
  "name": "John Doe",
  "email": "john@example.com",
  "picture": "https://example.com/avatar.jpg"
}
```

---

## Common Patterns

### Pagination

All list endpoints support pagination via query parameters:

**Parameters:**
- `limit` (integer, default: 100, max: 500) - Number of results to return
- `offset` (integer, default: 0) - Number of results to skip

**Example:**
```
GET /memes?limit=50&offset=100
```

Returns items 100-149 (50 items starting from position 100)

### Sorting

List endpoints support sorting via the `sort` parameter:

**Format:** `sort=field_name` (ascending) or `sort=-field_name` (descending)

**Allowed fields:**
- `created_at` - Creation timestamp
- `updated_at` - Last update timestamp
- `status` - Meme processing status
- `filename` - Meme filename
- `category` - User-assigned category
- `description` - Meme description

**Example:**
```
GET /memes?sort=-created_at          # Newest first
GET /memes?sort=filename             # Alphabetical order
```

### Filtering

List endpoints support filtering via query parameters:

**Status filter** (applies to all meme lists):
```
GET /memes?status=filled             # Only processed memes
GET /memes?status=pending            # Only unprocessed memes
```

**Status values:**
- `pending` - Waiting for LLM description generation
- `filled` - Description successfully generated
- `failed` - Description generation failed
- `removed` - Meme deleted

### Response Format

Responses follow consistent patterns:

**Single Object Response:**
```json
{
  "id": 1,
  "filename": "meme.jpg",
  "category": "humor",
  "description": "Funny cat meme"
}
```

**List Response:**
```json
[
  {"id": 1, "filename": "meme1.jpg"},
  {"id": 2, "filename": "meme2.jpg"}
]
```

**Status/Action Response:**
```json
{
  "status": "success",
  "message": "Operation completed",
  "result": {}
}
```

**Timestamp Format:**
All timestamps use ISO 8601 format (UTC):
```
2024-01-01T12:30:45Z
```

### Filename Requirements

Filenames are sanitized for security (prevent path traversal):

**Rules:**
- Max length: 255 characters
- Allowed characters: UTF-8, alphanumeric, space, dash, underscore, dot
- Forbidden characters: `< > : " | ? * \x00`
- Path separators (`/`, `\`) removed
- Leading dots removed

**Invalid examples:**
```
../../../etc/passwd     # Path traversal
meme\file.jpg          # Backslash
file|pipe.jpg          # Pipe character
```

---

## Error Responses

All errors follow a consistent format:

**Response Format:**
```json
{
  "detail": "Error message describing what went wrong"
}
```

**HTTP Status Codes:**

| Code | Reason | When Used |
|------|--------|-----------|
| 400 | Bad Request | Invalid input (validation error, bad filename, invalid sort field) |
| 401 | Unauthorized | Authentication required but not provided/invalid |
| 403 | Forbidden | Valid authentication but insufficient permissions (invalid share token) |
| 404 | Not Found | Resource not found (meme doesn't exist, feature not enabled) |
| 409 | Conflict | Resource conflict (duplicate download job for same URL) |
| 422 | Unprocessable Entity | Validation error in request body or parameters |
| 429 | Too Many Requests | Rate limit exceeded |
| 500 | Internal Server Error | Unexpected server error |
| 503 | Service Unavailable | Application not initialized or storage unavailable |

**Common Error Scenarios:**

Invalid meme filename:
```json
{
  "detail": "Invalid filename: exceeds maximum length"
}
```

Authentication required:
```json
{
  "detail": "Not authenticated"
}
```

Rate limited:
```json
{
  "detail": "Rate limit exceeded"
}
```

Invalid JSON:
```json
{
  "detail": [
    {
      "loc": ["body", "keywords"],
      "msg": "field required",
      "type": "value_error.missing"
    }
  ]
}
```

---

## Endpoint Reference

### UI Endpoints

HTML page serving endpoints. Return HTML content, not JSON.

#### GET /

**Serve main meme browser gallery page.**

**Authentication:** Required (except in public mode)
**Rate Limit:** None
**CSRF:** Not required
**Response:** HTML page with interactive meme gallery

**Description:** Main application interface for browsing, searching, and managing memes. Shows paginated list with filters and sorting. In non-public mode, redirects to `/login` if user not authenticated.

**Redirect Behavior:**
- Public mode: Shows gallery without login
- OIDC/Basic Auth: Redirects to `/login` if no session
- After logout: Redirects to `/login`

---

#### GET /login

**Serve login page.**

**Authentication:** Public
**Rate Limit:** None
**CSRF:** Not required
**Response:** HTML login page

**Description:** Login interface with authentication options based on configured mode:
- OIDC enabled: "Login with [Provider]" button
- Basic Auth enabled: Username/password form
- Public mode: Redirects directly to `/`

---

#### GET /duplicates

**Serve duplicate management interface page.**

**Authentication:** Required (except in public mode)
**Rate Limit:** None
**CSRF:** Not required
**Response:** HTML page with duplicate management UI

**Description:** Interface for viewing duplicate groups, merging duplicates, and managing false positives. Shows perceptual hash similarity scores.

---

#### GET /pending

**Serve pending memes page.**

**Authentication:** Required (except in public mode)
**Rate Limit:** None
**CSRF:** Not required
**Response:** HTML page with pending memes list

**Description:** Shows memes waiting for LLM description generation. Allows force-regeneration and reprocessing.

---

#### GET /tokens

**Serve API token management page.**

**Authentication:** Required (except in public mode)
**Rate Limit:** None
**CSRF:** Not required
**Response:** HTML page with token management UI

**Description:** Interface for generating, viewing, and revoking API tokens. Shows token creation date, last used, and expiration.

---

### Authentication & Authorization

#### GET /auth/login

**Initiate OIDC authentication flow.**

**Authentication:** Public
**Rate Limit:** 10/minute
**CSRF:** Not required
**Query Parameters:** None
**Response:** Redirect to OIDC provider authorization URL

**Description:** Starts OAuth 2.0 authorization flow with PKCE. Generates random state and code challenge, stores them, and redirects user to configured OIDC provider.

**Status Codes:**
- 302 Redirect - OIDC provider URL
- 503 Service Unavailable - OIDC not configured or not initialized

---

#### GET /auth/callback

**Handle OIDC provider callback.**

**Authentication:** Public (callback from provider)
**Rate Limit:** 10/minute
**CSRF:** Not required

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `code` | string | Yes | Authorization code from OIDC provider |
| `state` | string | Yes | CSRF state token (must match stored value) |
| `error` | string | No | Error code if authentication failed |
| `error_description` | string | No | Error description from provider |

**Response:** Redirect to `/` with session cookie set

**Description:** Exchanges authorization code for ID token. Validates PKCE challenge and state parameter. Creates session on success.

**Error Handling:**
- Invalid state: Redirects to `/login?error=invalid_state`
- Token exchange failure: Redirects to `/login?error=token_error`
- Group authorization failure: Redirects to `/login?error=not_in_allowed_groups`

**Session Creation:**
- Sets `session_id` (HTTPS) or `session_id_http` (HTTP) cookie
- Default expiry: 24 hours
- Optionally stores in Redis if configured

---

#### POST /auth/basic-login

**Authenticate with Basic Auth credentials.**

**Authentication:** Public
**Rate Limit:** 10/minute
**CSRF:** Not required

**Request Body:**
```json
{
  "username": "john_doe",
  "password": "secure_password"
}
```

**Response (200):**
```json
{
  "message": "Login successful"
}
```

Sets `auth_token` (HTTPS) or `auth_token_http` (HTTP) cookie with JWT token.

**Error Responses:**
```
400 Bad Request - Missing username or password
401 Unauthorized - Invalid credentials
429 Too Many Requests - Account locked (excessive failed attempts)
```

**Description:** Standard HTTP Basic Auth login. Validates credentials against database (Argon2id hashing). Enforces rate limiting with exponential backoff lockout (30s → 1m → 5m → 15m).

---

#### GET /api/auth/lockout-status

**Check Basic Auth lockout status for a user.**

**Authentication:** Public
**Rate Limit:** None
**CSRF:** Not required

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `username` | string | Yes | Username to check |

**Response (200):**
```json
{
  "locked": false,
  "locked_until": null,
  "failed_attempts": 0
}
```

**Response (200) - User locked:**
```json
{
  "locked": true,
  "locked_until": "2024-01-01T12:05:00Z",
  "failed_attempts": 3
}
```

**Description:** Queries lockout status without requiring authentication. Allows UI to show lockout message before form submission.

---

#### POST /auth/logout

**Logout current user (revoke session).**

**Authentication:** Optional
**Rate Limit:** 10/minute
**CSRF:** Not required
**Response (302):** Redirect to `/login` with cookies cleared

**Description:** Invalidates session/JWT token. Clears authentication cookies. Logs audit event.

---

#### GET /auth/user

**Get current authenticated user information.**

**Authentication:** Required
**Rate Limit:** None
**CSRF:** Not required

**Response (200):**
```json
{
  "user_id": "john_doe",
  "name": "John Doe",
  "email": "john@example.com",
  "picture": "https://example.com/avatar.jpg"
}
```

**Description:** Returns user profile from ID token or database. Useful for displaying username and avatar in UI.

**Note:** In public mode with `user_id = "public-user"`, other fields are null.

---

#### GET /api/csrf-token

**Get CSRF token for authenticated requests.**

**Authentication:** Required
**Rate Limit:** None
**CSRF:** Not required

**Response (200):**
```json
{
  "csrf_token": "abc123def456ghi789..."
}
```

**Description:** Returns CSRF token. Token is also set in `csrftoken` cookie. Must include both cookie and header in subsequent state-changing requests.

---

#### POST /api/tokens

**Generate new API token for current user.**

**Authentication:** Required (session/cookie only, not via existing API token)
**Rate Limit:** 10/hour
**CSRF:** Required

**Request Body:**
```json
{
  "name": "Production API Token",
  "expires_at": "2025-12-31T23:59:59Z"
}
```

**Response (200):**
```json
{
  "id": 1,
  "name": "Production API Token",
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJqb2huX2RvZSIsImlhdCI6MTcwNDExMDAwMCwiZXhwIjoxNzM2NjQ2MDAwLCJqdGkiOiJhYmMxMjNkZWY0NTYifQ...",
  "created_at": "2024-01-01T00:00:00Z"
}
```

**Error Responses:**
```
400 Bad Request - Invalid expiration date format
401 Unauthorized - Not authenticated
429 Too Many Requests - Rate limit (10/hour)
```

**Token Details:**
- **Shown only once** in creation response
- Cannot be retrieved after creation (save it immediately)
- Must be included in `Authorization: Bearer <token>` header
- Validated on every request (signature + revocation check)
- Can be revoked anytime via DELETE endpoint

**Field Descriptions:**
- `name` (string, required) - User-friendly token name
- `expires_at` (string, optional) - ISO datetime when token expires (e.g., "2025-12-31T23:59:59Z")

---

#### GET /api/tokens

**List all API tokens for current user.**

**Authentication:** Required
**Rate Limit:** None
**CSRF:** Not required

**Response (200):**
```json
[
  {
    "id": 1,
    "name": "Production API Token",
    "created_at": "2024-01-01T00:00:00Z",
    "last_used_at": "2024-01-15T12:30:45Z",
    "expires_at": "2025-12-31T23:59:59Z",
    "revoked": false,
    "expired": false
  },
  {
    "id": 2,
    "name": "Development Token",
    "created_at": "2024-01-10T00:00:00Z",
    "last_used_at": null,
    "expires_at": null,
    "revoked": true,
    "expired": false
  }
]
```

**Description:** Lists all tokens for authenticated user. Note: Actual token values are NOT returned (only hash is stored). Shows metadata only.

---

#### POST /api/tokens/{token_id}/revoke

**Revoke an API token (soft delete).**

**Authentication:** Required
**Rate Limit:** None
**CSRF:** Required

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `token_id` | integer | Token ID to revoke |

**Response (200):**
```json
{
  "status": "revoked",
  "token_id": 1
}
```

**Error Responses:**
```
404 Not Found - Token doesn't exist or belongs to different user
```

**Description:** Marks token as revoked. Token becomes invalid immediately (checked on every request). Cannot be un-revoked.

---

#### DELETE /api/tokens/{token_id}

**Delete an API token (hard delete).**

**Authentication:** Required
**Rate Limit:** None
**CSRF:** Required

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `token_id` | integer | Token ID to delete |

**Response (200):**
```json
{
  "status": "deleted",
  "token_id": 1
}
```

**Error Responses:**
```
404 Not Found - Token doesn't exist or belongs to different user
```

**Description:** Permanently deletes token record. Cannot be recovered after deletion.

**Difference from revoke:**
- Revoke: Token marked as inactive, record kept (can still see in history)
- Delete: Token record removed from database completely

---

### Meme Management

#### GET /memes

**List memes with filtering and pagination.**

**Authentication:** Required (except in public mode)
**Rate Limit:** None
**CSRF:** Not required

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | integer | 100 | Max results (max 500) |
| `offset` | integer | 0 | Skip N results |
| `status` | string | null | Filter by status (pending/filled/failed/removed) |
| `sort` | string | "-created_at" | Sort field (±created_at, ±updated_at, ±status, ±filename, ±category, ±description) |

**Example Requests:**
```
GET /memes?limit=50&offset=0
GET /memes?status=filled&sort=-updated_at
GET /memes?sort=filename&limit=20
```

**Response (200):**
```json
[
  {
    "id": 1,
    "filename": "funny_cat.jpg",
    "category": "animals",
    "description": "A cat making a funny face",
    "keywords": "cat,funny,animal,cute",
    "text_in_image": "I CAN HAZ CHEEZBURGER",
    "source_url": null,
    "status": "filled",
    "attempts": 1,
    "last_error": null,
    "last_attempt_at": "2024-01-01T12:00:00Z",
    "created_at": "2024-01-01T00:00:00Z",
    "updated_at": "2024-01-01T12:00:00Z",
    "phash": "a1b2c3d4e5f6...",
    "is_false_positive": false,
    "processed": true
  }
]
```

**Field Descriptions:**
- `processed` - Computed field: `status === "filled"`
- `phash` - Perceptual hash for duplicate detection (64-bit hex string)
- `is_false_positive` - Marked as non-duplicate pair
- `status` - pending (unprocessed), filled (LLM description generated), failed (generation failed), removed (deleted)

**Sorting Examples:**
- `sort=-created_at` - Newest memes first
- `sort=filename` - Alphabetical by filename
- `sort=-updated_at` - Recently modified first

---

#### GET /memes/{filename}

**Get detailed information about a specific meme.**

**Authentication:** Required (except in public mode)
**Rate Limit:** None
**CSRF:** Not required

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `filename` | string | Meme filename (URL-encoded) |

**Example Requests:**
```
GET /memes/funny_cat.jpg
GET /memes/my%20meme%20with%20spaces.png
```

**Response (200):**
```json
{
  "id": 1,
  "filename": "funny_cat.jpg",
  "category": "animals",
  "description": "A cat making a funny face",
  "keywords": "cat,funny,animal,cute",
  "text_in_image": "I CAN HAZ CHEEZBURGER",
  "source_url": null,
  "status": "filled",
  "attempts": 1,
  "last_error": null,
  "last_attempt_at": "2024-01-01T12:00:00Z",
  "created_at": "2024-01-01T00:00:00Z",
  "updated_at": "2024-01-01T12:00:00Z",
  "phash": "a1b2c3d4e5f6...",
  "is_false_positive": false,
  "processed": true
}
```

**Error Responses:**
```
400 Bad Request - Invalid filename
404 Not Found - Meme not found
```

---

#### GET /memes/{filename}/preview

**Get thumbnail preview of a meme.**

**Authentication:** Required (except in public mode)
**Rate Limit:** None
**CSRF:** Not required

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `filename` | string | Meme filename |

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `size` | integer | 300 | Preview size in pixels (generates square image) |

**Example Requests:**
```
GET /memes/funny_cat.jpg/preview
GET /memes/funny_cat.jpg/preview?size=200
GET /memes/video.mp4/preview?size=400
```

**Response (200):** JPEG image (binary stream)

**Headers:**
```
Content-Type: image/jpeg
Content-Length: <size_in_bytes>
```

**Description:** Returns JPEG thumbnail. For videos, extracts first frame. Size parameter controls both width and height (creates square image).

**Error Responses:**
```
404 Not Found - Meme not found
503 Service Unavailable - Image processing failed
```

---

#### GET /memes/{filename}/download

**Download original meme file.**

**Authentication:** Required (except in public mode) OR valid share token
**Rate Limit:** None
**CSRF:** Not required

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `filename` | string | Meme filename |

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `share_token` | string | Optional: Share token for public access (bypass authentication) |

**Example Requests:**
```
GET /memes/funny_cat.jpg/download                           # Requires authentication
GET /memes/funny_cat.jpg/download?share_token=abc123       # Public via share token
```

**Response (200):** File binary stream

**Headers:**
```
Content-Type: image/jpeg (or appropriate MIME type)
Content-Disposition: inline; filename="funny_cat.jpg"
Content-Length: <file_size>
```

**Error Responses:**
```
403 Forbidden - Invalid or expired share token
404 Not Found - Meme not found
503 Service Unavailable - File read error
```

**Description:** Returns original file content. Sets Content-Disposition to inline for browser preview. Share tokens bypass authentication (public download).

---

#### GET /memes/{filename}/share-link

**Generate temporary share link for a meme.**

**Authentication:** Required
**Rate Limit:** 10/minute
**CSRF:** Not required

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `filename` | string | Meme filename |

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `expires_in_hours` | integer | 24 | Link expiry time in hours |

**Example Requests:**
```
GET /memes/funny_cat.jpg/share-link                    # Default 24 hour expiry
GET /memes/funny_cat.jpg/share-link?expires_in_hours=1 # 1 hour expiry
GET /memes/funny_cat.jpg/share-link?expires_in_hours=72 # 3 day expiry
```

**Response (200):**
```json
{
  "share_url": "https://example.com/memes/funny_cat.jpg/shared?token=abc123def456",
  "expires_at": "2024-01-02T12:30:00Z"
}
```

**Description:** Generates temporary download link with expiring share token. Token is Argon2-hashed and stored in database. Share links allow public file download without authentication.

---

#### GET /memes/{filename}/shared

**Access meme via temporary share token (public).**

**Authentication:** Public (requires valid token)
**Rate Limit:** None
**CSRF:** Not required

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `filename` | string | Meme filename |

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `token` | string | Yes | Share token |

**Example Request:**
```
GET /memes/funny_cat.jpg/shared?token=abc123def456
```

**Response (200):** File binary stream (same as download endpoint)

**Headers:**
```
Content-Type: image/jpeg
Content-Disposition: inline; filename="funny_cat.jpg"
```

**Error Responses:**
```
403 Forbidden - Invalid, expired, or revoked share token
404 Not Found - Meme not found
```

**Description:** Public endpoint for accessing shared files. Token validity and expiration checked on every request. Tracks usage count in database.

---

#### GET /api/share-tokens

**List all active share tokens for current user.**

**Authentication:** Required
**Rate Limit:** None
**CSRF:** Not required

**Response (200):**
```json
[
  {
    "id": 1,
    "filename": "funny_cat.jpg",
    "created_at": "2024-01-01T12:00:00Z",
    "expires_at": "2024-01-02T12:00:00Z",
    "used_count": 5
  },
  {
    "id": 2,
    "filename": "dog_video.mp4",
    "created_at": "2024-01-01T14:00:00Z",
    "expires_at": "2024-01-04T14:00:00Z",
    "used_count": 0
  }
]
```

**Description:** Lists all share tokens created by current user. Shows usage count and expiration time.

---

#### DELETE /api/share-tokens/{token_id}

**Revoke/delete a share token.**

**Authentication:** Required
**Rate Limit:** None
**CSRF:** Required

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `token_id` | integer | Share token ID |

**Response (200):**
```json
{
  "status": "deleted",
  "token_id": 1
}
```

**Error Responses:**
```
404 Not Found - Token doesn't exist or belongs to different user
```

**Description:** Revokes share token. Link becomes invalid immediately. Cannot be un-revoked.

---

#### POST /memes/{filename}/force-description

**Force regeneration of meme description.**

**Authentication:** Required (except in public mode)
**Rate Limit:** None
**CSRF:** Required

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `filename` | string | Meme filename |

**Request Body:** None (or empty JSON)

**Response (200):** Updated meme object

**Description:** Triggers LLM description generation, bypassing attempt count limit. Useful for retrying failed descriptions or updating with new prompt. Sets status back to "pending" to queue for processing.

**Error Responses:**
```
404 Not Found - Meme not found
503 Service Unavailable - LLM service unavailable
```

---

#### POST /memes/{filename}/reprocess

**Clear all metadata and regenerate from scratch.**

**Authentication:** Required (except in public mode)
**Rate Limit:** None
**CSRF:** Required

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `filename` | string | Meme filename |

**Request Body:** None (or empty JSON)

**Response (200):** Updated meme object

**Description:** Clears all metadata (description, keywords, category, phash, text_in_image). Sets attempt count to 0 and status to "pending". Meme will be fully reprocessed.

**Field Changes:**
- `description` → null
- `keywords` → null
- `category` → null
- `phash` → null
- `text_in_image` → null
- `attempts` → 0
- `status` → "pending"
- `last_error` → null

---

#### PATCH /memes/{filename}

**Update meme metadata (partial update).**

**Authentication:** Required (except in public mode)
**Rate Limit:** None
**CSRF:** Required

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `filename` | string | Meme filename |

**Request Body:**
```json
{
  "category": "animals",
  "keywords": "cat,funny,cute",
  "description": "A funny cat meme"
}
```

**Field Constraints:**
| Field | Type | Constraints |
|-------|------|-------------|
| `category` | string | Optional, max 100 characters |
| `keywords` | string | Optional, max 500 characters |
| `description` | string | Optional, max 2000 characters |

**Response (200):** Updated meme object

**Example:**
```json
{
  "category": "animals",
  "description": "Updated description"
}
```

**Description:** Updates only provided fields. Other fields unchanged. Updates `updated_at` timestamp. Logs audit event.

**Error Responses:**
```
400 Bad Request - Field exceeds max length
404 Not Found - Meme not found
422 Unprocessable Entity - Invalid field types
```

---

#### DELETE /memes/{filename}

**Delete meme from database and storage.**

**Authentication:** Required (except in public mode)
**Rate Limit:** 10/hour
**CSRF:** Required

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `filename` | string | Meme filename |

**Request Body:** None

**Response (200):**
```json
{
  "status": "deleted",
  "filename": "funny_cat.jpg"
}
```

**Description:** Removes meme from database and WebDAV storage. Permanently deletes file. Cannot be undone. All associated share tokens invalidated. Logs audit event.

**Deletion Process:**
1. Verify meme exists
2. Delete from WebDAV storage
3. Delete database record
4. Invalidate all share tokens for this file
5. Update statistics

**Error Responses:**
```
404 Not Found - Meme not found
503 Service Unavailable - Storage deletion failed
```

---

#### GET /memes/search/by-keywords

**Search memes by keywords using Whoosh full-text search.**

**Authentication:** Required (except in public mode)
**Rate Limit:** None
**CSRF:** Not required

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `q` | string | "" | Search query (2+ characters) |
| `limit` | integer | 20 | Max results |
| `offset` | integer | 0 | Skip N results |

**Example Requests:**
```
GET /memes/search/by-keywords?q=funny+cat
GET /memes/search/by-keywords?q=dog&limit=50
GET /memes/search/by-keywords?q=meme&offset=10&limit=20
```

**Response (200):**
```json
[
  {
    "filename": "funny_cat.jpg",
    "category": "animals",
    "description": "A very funny cat making a silly face",
    "keywords": "cat,funny,animal,cute",
    "score": 0.95
  },
  {
    "filename": "cat_video.mp4",
    "category": "animals",
    "description": "Cat video compilation",
    "keywords": "cat,video,cute",
    "score": 0.87
  }
]
```

**Description:** Full-text search across keywords, description, and category fields. Returns results ranked by relevance score. Searches minimum 2 characters.

**Search Scope:**
- Keywords field (high relevance)
- Description text (medium relevance)
- Category field (lower relevance)

**Error Responses:**
```
400 Bad Request - Query too short (< 2 characters)
```

---

#### GET /memes/phash-status

**Get perceptual hash initialization status.**

**Authentication:** Required (except in public mode)
**Rate Limit:** None
**CSRF:** Not required

**Response (200):**
```json
{
  "total_memes": 1000,
  "with_phash": 950,
  "without_phash": 50,
  "success_rate": 95.0,
  "status": "ok"
}
```

**Description:** Returns overview of perceptual hash calculation progress. Useful for monitoring deduplication readiness.

**Field Descriptions:**
- `total_memes` - Total memes in database
- `with_phash` - Memes with calculated perceptual hash
- `without_phash` - Memes without hash (pending calculation)
- `success_rate` - Percentage of successful hash calculations
- `status` - "ok" or "error"

---

#### GET /api/stats

**Get application statistics.**

**Authentication:** Required (except in public mode)
**Rate Limit:** None
**CSRF:** Not required

**Response (200):**
```json
{
  "total": 1000,
  "filled": 800,
  "pending": 150,
  "failed": 50,
  "max_generation_attempts": 3,
  "status": "ok"
}
```

**Field Descriptions:**
- `total` - Total memes in database
- `filled` - Memes with descriptions (status="filled")
- `pending` - Awaiting description generation
- `failed` - Failed description generation
- `max_generation_attempts` - Attempt limit before marking failed

---

#### GET /api/pending-memes

**Get list of pending memes (unfilled).**

**Authentication:** Required (except in public mode)
**Rate Limit:** None
**CSRF:** Not required

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | integer | 100 | Max results |
| `offset` | integer | 0 | Skip N results |

**Response (200):** Array of meme objects (same structure as GET /memes)

**Description:** Returns memes with `status="pending"` (awaiting LLM description generation). Ordered by creation date.

---

#### GET /api/prompt

**Get current LLM prompt template.**

**Authentication:** Required (except in public mode)
**Rate Limit:** None
**CSRF:** Not required

**Response (200):**
```json
{
  "prompt": "Describe this meme in one sentence. Include the main subject, action, and any text visible in the image."
}
```

**Description:** Returns the prompt template used for LLM description generation. Used in system message for LLM calls.

---

#### POST /api/prompt

**Update LLM prompt template.**

**Authentication:** Required (except in public mode)
**Rate Limit:** 10/minute
**CSRF:** Required

**Request Body:**
```json
{
  "prompt": "New prompt template: Describe this meme..."
}
```

**Response (200):**
```json
{
  "status": "updated",
  "prompt": "New prompt template: Describe this meme..."
}
```

**Description:** Updates system prompt used for LLM description generation. Affects only future description generations, not existing ones.

**Error Responses:**
```
400 Bad Request - Prompt empty or too long
```

---

### Deduplication

#### POST /memes/deduplication/analyze

**Analyze all memes for duplicates using perceptual hashing.**

**Authentication:** Required (except in public mode)
**Rate Limit:** 10/minute
**CSRF:** Required

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `threshold` | integer | 10 | Hamming distance threshold (0-64) |

**Example Requests:**
```
POST /memes/deduplication/analyze                    # Default threshold=10
POST /memes/deduplication/analyze?threshold=5        # Stricter (fewer duplicates)
POST /memes/deduplication/analyze?threshold=20       # Looser (more duplicates)
```

**Response (200):**
```json
{
  "status": "completed",
  "duplicate_groups": 15,
  "total_duplicates": 45,
  "threshold": 10
}
```

**Description:** Scans all memes with perceptual hashes and groups duplicates. Hamming distance measures hash similarity (0=identical, 64=completely different).

**Threshold Guide:**
- Threshold 0-3: Only pixel-nearly-identical images
- Threshold 5-10: Very similar images (recommended: 10)
- Threshold 15-20: Similar images with variations
- Threshold 25+: Loosely related images

**Performance:** May take minutes on large libraries.

**Error Responses:**
```
400 Bad Request - Threshold outside range [0, 64]
503 Service Unavailable - Phash calculation not ready
```

---

#### GET /memes/duplicates-by-group

**Get all duplicate groups.**

**Authentication:** Required (except in public mode)
**Rate Limit:** None
**CSRF:** Not required

**Response (200):**
```json
[
  {
    "group_id": 1,
    "members": [
      {
        "filename": "meme1.jpg",
        "phash": "a1b2c3d4e5f6...",
        "category": "humor",
        "similarity": 0
      },
      {
        "filename": "meme2.jpg",
        "phash": "a1b2c3d4e5f7...",
        "category": "humor",
        "similarity": 3
      },
      {
        "filename": "meme3.jpg",
        "phash": "a1b2c3d4e5f8...",
        "category": "humor",
        "similarity": 7
      }
    ],
    "representative": "meme1.jpg"
  }
]
```

**Description:** Returns all duplicate groups organized hierarchically. Primary meme listed first. Similarity shows hamming distance from primary meme's phash.

---

#### GET /memes/{filename}/duplicates

**Get duplicates for a specific meme.**

**Authentication:** Required (except in public mode)
**Rate Limit:** None
**CSRF:** Not required

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `filename` | string | Meme filename |

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `threshold` | integer | 10 | Hamming distance threshold |

**Response (200):**
```json
{
  "primary": {
    "filename": "meme1.jpg",
    "similarity": 0,
    "preview_url": "/memes/meme1.jpg/preview"
  },
  "duplicates": [
    {
      "filename": "meme2.jpg",
      "similarity": 5,
      "preview_url": "/memes/meme2.jpg/preview"
    },
    {
      "filename": "meme3.jpg",
      "similarity": 8,
      "preview_url": "/memes/meme3.jpg/preview"
    }
  ]
}
```

**Error Responses:**
```
404 Not Found - Meme not found or has no phash
```

---

#### POST /memes/{filename}/recalculate-phash

**Recalculate perceptual hash for a meme.**

**Authentication:** Required (except in public mode)
**Rate Limit:** 20/minute
**CSRF:** Required

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `filename` | string | Meme filename |

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `timestamp` | float | 1.0 | Video timestamp in seconds (for video frames) |

**Example Requests:**
```
POST /memes/image.jpg/recalculate-phash              # Image
POST /memes/video.mp4/recalculate-phash?timestamp=5  # 5 seconds into video
```

**Response (200):**
```json
{
  "filename": "meme.jpg",
  "phash": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
  "status": "updated"
}
```

**Description:** Recalculates perceptual hash (for videos, extracts frame at specified timestamp). Useful after image edits or for testing different video frames.

**Error Responses:**
```
404 Not Found - Meme not found
503 Service Unavailable - Phash calculation failed
```

---

#### POST /memes/{filename}/mark-not-duplicate

**Mark a duplicate pair as false positive.**

**Authentication:** Required (except in public mode)
**Rate Limit:** None
**CSRF:** Required

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `filename` | string | First meme filename |

**Request Body:**
```json
{
  "other_filename": "meme2.jpg"
}
```

**Response (200):**
```json
{
  "status": "marked",
  "filename_a": "meme1.jpg",
  "filename_b": "meme2.jpg"
}
```

**Description:** Adds pair exception to prevent treating these memes as duplicates. Useful for similar-looking but distinct memes. Prevents them from being grouped in future duplicate analyses.

---

#### POST /memes/merge-duplicates

**Merge duplicate memes (keep primary, delete others).**

**Authentication:** Required (except in public mode)
**Rate Limit:** 30/minute
**CSRF:** Required

**Request Body:**
```json
{
  "primary_filename": "meme1.jpg",
  "duplicate_filenames": ["meme2.jpg", "meme3.jpg"],
  "merge_metadata": true,
  "metadata_sources": ["meme2.jpg"]
}
```

**Field Descriptions:**
- `primary_filename` (string, required) - Meme to keep
- `duplicate_filenames` (array, required) - Memes to delete
- `merge_metadata` (boolean, optional, default: true) - Merge metadata from duplicates
- `metadata_sources` (array, optional) - Specific files to extract metadata from

**Response (200):**
```json
{
  "status": "merged",
  "primary_filename": "meme1.jpg",
  "deleted_count": 2,
  "deleted_files": ["meme2.jpg", "meme3.jpg"]
}
```

**Description:** Merges multiple duplicate memes into one. Primary meme keeps (or inherits) metadata. Duplicate files deleted from storage. All share tokens for deleted memes invalidated.

**Metadata Merge Logic:**
- If `merge_metadata=true`:
  - Takes metadata from first file in `metadata_sources` if specified
  - Otherwise takes from primary file
  - Non-empty fields preserved in primary

**Error Responses:**
```
400 Bad Request - Primary filename in duplicate list
404 Not Found - Primary or duplicate file not found
409 Conflict - File deletion failed
```

---

#### POST /duplicates/pairs

**Add pair exception (always ignore as duplicate).**

**Authentication:** Required (except in public mode)
**Rate Limit:** 10/minute
**CSRF:** Required

**Request Body:**
```json
{
  "filename_a": "meme1.jpg",
  "filename_b": "meme2.jpg"
}
```

**Response (200):**
```json
{
  "status": "added",
  "filename_a": "meme1.jpg",
  "filename_b": "meme2.jpg"
}
```

**Description:** Creates bidirectional pair exception. These files will never be grouped as duplicates even if similar. Useful for images that look alike but are intentionally different.

---

#### GET /duplicates/pairs

**List all pair exceptions.**

**Authentication:** Required (except in public mode)
**Rate Limit:** None
**CSRF:** Not required

**Response (200):**
```json
[
  {
    "id": 1,
    "filename_a": "meme1.jpg",
    "filename_b": "meme2.jpg",
    "created_at": "2024-01-01T00:00:00Z"
  },
  {
    "id": 2,
    "filename_a": "image_a.png",
    "filename_b": "image_b.png",
    "created_at": "2024-01-02T00:00:00Z"
  }
]
```

---

#### DELETE /duplicates/pairs

**Remove pair exception.**

**Authentication:** Required (except in public mode)
**Rate Limit:** 10/minute
**CSRF:** Required

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `filename_a` | string | Yes | First filename |
| `filename_b` | string | Yes | Second filename |

**Example Request:**
```
DELETE /duplicates/pairs?filename_a=meme1.jpg&filename_b=meme2.jpg
```

**Response (200):**
```json
{
  "status": "removed",
  "filename_a": "meme1.jpg",
  "filename_b": "meme2.jpg"
}
```

**Error Responses:**
```
404 Not Found - Pair exception not found
```

---

#### POST /memes/duplicates/delete-group

**Delete all memes in a duplicate group except primary.**

**Authentication:** Required (except in public mode)
**Rate Limit:** 10/minute
**CSRF:** Required

**Request Body:**
```json
{
  "group_id": 1,
  "keep_filename": "meme1.jpg"
}
```

**Response (200):**
```json
{
  "status": "deleted",
  "group_id": 1,
  "deleted_count": 5,
  "kept_filename": "meme1.jpg"
}
```

**Description:** Removes all duplicates from a group except the specified file. Deletes from storage and database. All share tokens for deleted files invalidated.

**Error Responses:**
```
404 Not Found - Group not found
400 Bad Request - Keep file not in group
```

---

### Sync & Storage

#### POST /sync

**Trigger manual WebDAV synchronization.**

**Authentication:** Required (except in public mode)
**Rate Limit:** 5/minute
**CSRF:** Required

**Request Body:** None (empty JSON)

**Response (200):**
```json
{
  "status": "completed",
  "message": "Sync completed; Transcoded 5/10 MKV files",
  "result": {
    "added": 10,
    "removed": 2,
    "unfilled": 5,
    "saved": 8,
    "failed": 0,
    "unsupported": 1,
    "mkv_transcoding": {
      "total_found": 10,
      "transcoded": 5,
      "failed": 0
    }
  }
}
```

**Field Descriptions:**
- `added` - New files discovered from WebDAV
- `removed` - Files deleted from WebDAV
- `unfilled` - Files awaiting LLM processing
- `saved` - Files successfully saved/updated
- `failed` - Files that failed to sync
- `unsupported` - Files with unsupported format
- `mkv_transcoding` - MKV video transcoding stats

**Description:** Synchronizes local database with WebDAV storage. Discovers new files, detects deleted files, transcodes MKV videos. Operation is asynchronous (may take minutes).

---

#### GET /sync/status

**Get current sync/transcoding operation status.**

**Authentication:** Required (except in public mode)
**Rate Limit:** 20/minute
**CSRF:** Not required

**Response (200) - Idle:**
```json
{
  "operation": null,
  "progress": null
}
```

**Response (200) - Active Sync:**
```json
{
  "operation": "sync",
  "progress": {
    "current": 5,
    "total": 10,
    "filename": "current_file.jpg"
  }
}
```

**Response (200) - Transcoding Video:**
```json
{
  "operation": "transcoding_mkv",
  "progress": {
    "current": 3,
    "total": 8,
    "filename": "video.mkv"
  }
}
```

**Description:** Real-time status of background operations. Updates every few seconds.

---

### Video Downloads

#### POST /api/download-video

**Download video from URL using yt-dlp.**

**Authentication:** Required (except in public mode)
**Rate Limit:** 5/minute
**CSRF:** Required

**Request Body:**
```json
{
  "url": "https://youtube.com/watch?v=dQw4w9WgXcQ",
  "extract_if_failed": true
}
```

**Field Descriptions:**
- `url` (string, required) - Video URL (supports YouTube, Vimeo, etc.)
- `extract_if_failed` (boolean, optional, default: true) - If direct download fails, extract URLs from page HTML

**Response (200) - Direct Download:**
```json
{
  "id": 1,
  "url": "https://youtube.com/watch?v=dQw4w9WgXcQ",
  "status": "pending",
  "progress_percent": 0.0,
  "filename": null,
  "error_message": null,
  "created_at": "2024-01-01T12:00:00Z",
  "started_at": null,
  "completed_at": null,
  "video_title": null,
  "video_duration": null,
  "file_size_bytes": null
}
```

**Response (200) - Multiple Extracted URLs:**
```json
{
  "page_url": "https://example.com/page",
  "extracted_urls": [
    {
      "url": "https://youtube.com/watch?v=1",
      "source": "Found in iframe"
    },
    {
      "url": "https://vimeo.com/123",
      "source": "Found in video tag"
    }
  ],
  "message": "Multiple video URLs found. Please select one to download."
}
```

**Status Values:**
- `pending` - Queued, waiting to start
- `downloading` - Download in progress
- `processing` - Converting format
- `completed` - Download complete, file added to gallery
- `failed` - Download or processing failed

**Error Responses:**
```
404 Not Found - Feature not enabled (ENABLE_VIDEO_DOWNLOADS=false)
409 Conflict - Download job already exists for this URL
503 Service Unavailable - FFmpeg not available
```

**Description:** Starts asynchronous video download. Returns job ID to monitor progress. Downloaded videos added to meme gallery automatically.

---

#### GET /api/download-jobs/{job_id}

**Get download job status.**

**Authentication:** Required (except in public mode)
**Rate Limit:** 60/minute
**CSRF:** Not required

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `job_id` | integer | Download job ID |

**Response (200):**
```json
{
  "id": 1,
  "url": "https://youtube.com/watch?v=dQw4w9WgXcQ",
  "status": "downloading",
  "progress_percent": 45.5,
  "filename": null,
  "error_message": null,
  "created_at": "2024-01-01T12:00:00Z",
  "started_at": "2024-01-01T12:01:00Z",
  "completed_at": null,
  "video_title": "Rick Astley - Never Gonna Give You Up",
  "video_duration": 213,
  "file_size_bytes": null
}
```

**Error Responses:**
```
404 Not Found - Job not found
```

**Description:** Real-time status of a specific download job. Poll this endpoint to monitor progress.

---

#### GET /api/download-jobs

**List all download jobs for current user.**

**Authentication:** Required (except in public mode)
**Rate Limit:** 30/minute
**CSRF:** Not required

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `status` | string | null | Filter by status (pending/downloading/completed/failed) |
| `limit` | integer | 50 | Max results |
| `offset` | integer | 0 | Skip N results |

**Example Requests:**
```
GET /api/download-jobs
GET /api/download-jobs?status=completed
GET /api/download-jobs?status=downloading&limit=10
```

**Response (200):** Array of download job objects

---

#### DELETE /api/download-jobs/{job_id}

**Cancel/delete a download job.**

**Authentication:** Required (except in public mode)
**Rate Limit:** None
**CSRF:** Required

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `job_id` | integer | Download job ID |

**Response (200):**
```json
{
  "status": "cancelled",
  "job_id": 1
}
```

**Error Responses:**
```
404 Not Found - Job not found
400 Bad Request - Job already completed (cannot cancel)
```

**Description:** Cancels pending/downloading jobs. Completed or failed jobs cannot be cancelled.

---

### Health Check

#### GET /health

**Service health check.**

**Authentication:** Public
**Rate Limit:** None
**CSRF:** Not required

**Response (200):**
```json
{
  "status": "ok",
  "message": "App is running"
}
```

**Error Responses:**
```
503 Service Unavailable - Application not fully initialized
```

**Description:** Simple health check for load balancers and monitoring. Returns 200 when service is operational, 503 when initializing or experiencing issues.

---

## Appendices

### A. Meme Object Schema

```typescript
interface Meme {
  // Identifiers
  id: number;
  filename: string;

  // Metadata
  category: string | null;        // Max 100 characters
  description: string | null;     // Max 2000 characters
  keywords: string | null;        // Comma-separated, max 500 characters
  text_in_image: string | null;   // Text visible in image
  source_url: string | null;      // Original source URL

  // Processing Status
  status: "pending" | "filled" | "failed" | "removed";
  attempts: number;               // LLM generation attempts
  last_error: string | null;      // Last generation error message
  last_attempt_at: string | null; // ISO datetime

  // Timestamps
  created_at: string;  // ISO datetime (UTC)
  updated_at: string;  // ISO datetime (UTC)

  // Deduplication
  phash: string | null;           // Perceptual hash (64-bit hex)
  is_false_positive: boolean;     // Marked as non-duplicate

  // Computed Fields
  processed: boolean;             // True if status === "filled"
}
```

### B. Rate Limits Summary

| Endpoint | Limit |
|----------|-------|
| GET /auth/login | 10/minute |
| GET /auth/callback | 10/minute |
| POST /auth/basic-login | 10/minute |
| POST /auth/logout | 10/minute |
| POST /api/tokens | 10/hour |
| GET /memes/{filename}/share-link | 10/minute |
| POST /sync | 5/minute |
| GET /sync/status | 20/minute |
| POST /api/download-video | 5/minute |
| GET /api/download-jobs/{job_id} | 60/minute |
| GET /api/download-jobs | 30/minute |
| POST /memes/deduplication/analyze | 10/minute |
| DELETE /memes/{filename} | 10/hour |
| POST /api/prompt | 10/minute |
| POST /memes/{filename}/recalculate-phash | 20/minute |
| POST /memes/merge-duplicates | 30/minute |
| POST /duplicates/pairs | 10/minute |
| DELETE /duplicates/pairs | 10/minute |
| POST /memes/duplicates/delete-group | 10/minute |

### C. CSRF-Protected Endpoints

All state-changing operations require CSRF token in `X-CSRF-Token` header:

**Meme Operations:**
- POST /memes/{filename}/force-description
- POST /memes/{filename}/reprocess
- PATCH /memes/{filename}
- DELETE /memes/{filename}

**Deduplication:**
- POST /memes/deduplication/analyze
- POST /memes/{filename}/recalculate-phash
- POST /memes/{filename}/mark-not-duplicate
- POST /memes/merge-duplicates
- POST /duplicates/pairs
- DELETE /duplicates/pairs
- POST /memes/duplicates/delete-group

**Sync & Storage:**
- POST /sync

**Video Downloads:**
- POST /api/download-video
- DELETE /api/download-jobs/{job_id}

**Authentication & Tokens:**
- POST /api/tokens
- POST /api/tokens/{token_id}/revoke
- DELETE /api/tokens/{token_id}

**Sharing:**
- DELETE /api/share-tokens/{token_id}

**Configuration:**
- POST /api/prompt

### D. Supported File Types

**Images:**
- JPEG (.jpg, .jpeg)
- PNG (.png)
- WebP (.webp)
- GIF (.gif)
- BMP (.bmp)
- TIFF (.tiff)

**Videos:**
- MP4 (.mp4)
- WebM (.webm)
- MOV (.mov)
- Matroska (.mkv) - Transcoded to MP4 during sync
- AVI (.avi)
- FLV (.flv)

### E. Configuration Reference

**Authentication Configuration:**
```bash
# Enable public access (no authentication)
PUBLIC_MODE=true

# Enable OIDC authentication
OIDC_ENABLED=true
OIDC_CLIENT_ID=client_id
OIDC_CLIENT_SECRET=secret
OIDC_PROVIDER_URL=https://auth.example.com
OIDC_ALLOWED_GROUPS=admins,editors

# Enable Basic Auth
BASIC_AUTH=true

# Required for authentication
CSRF_SECRET=your_csrf_secret_here
JWT_SECRET=your_jwt_secret_here

# Optional: Redis session storage
REDIS_URL=redis://localhost:6379
```

**Feature Configuration:**
```bash
# Enable video downloads
ENABLE_VIDEO_DOWNLOADS=true

# TLS/SSL Configuration
NO_TLS=false              # Use HTTPS (default)
NO_TLS=true               # Use HTTP only

# Server Configuration
HOST=0.0.0.0
PORT=8443                 # HTTPS port
HTTP_PORT=8080            # HTTP port
```

**Storage Configuration:**
```bash
# WebDAV Storage
WEBDAV_URL=https://webdav.example.com/
WEBDAV_USERNAME=user
WEBDAV_PASSWORD=pass
```

### F. Timestamp Format

All timestamps use ISO 8601 format (UTC timezone):

```
2024-01-01T12:30:45Z
2024-12-31T23:59:59Z
```

### G. Pagination Tips

**Getting total count:**
Query with `limit=1&offset=0` and use `total_count` from response headers (if available).

**Efficient pagination:**
```
# Get first page
GET /memes?limit=100&offset=0

# Get next page
GET /memes?limit=100&offset=100

# Get last page (estimate, adjust as needed)
GET /memes?limit=100&offset=900
```

**Default pagination:**
```
GET /memes              # Returns first 100 (default limit)
GET /memes?limit=500    # Max 500 per request
```

---

**Last Updated:** 2024-01-01
**API Version:** 1.0

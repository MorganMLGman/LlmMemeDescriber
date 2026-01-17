"""
Authorization and authentication utilities.

This module provides helpers for endpoint authorization.
"""

# Authorization strategies:

# 1. Public endpoints (no auth required):
#    - GET / (index)
#    - GET /health
#    - GET /memes (public API - publicly searchable)
#    - GET /memes/{id} (public API)

# 2. User-required endpoints (require session or bearer token):
#    - POST /api/tokens (generate new token)
#    - GET /api/tokens (list user's tokens)
#    - DELETE /api/tokens/{id} (revoke token)
#    - PATCH /memes/{id} (update metadata - optional, can require auth)
#    - DELETE /memes/{id} (delete meme - requires auth)
#    - POST /sync (sync with storage)
#    - POST /memes/merge-duplicates (merge duplicates)

# Usage in endpoints:
#
# from fastapi import Depends
#
# @app.get("/api/protected")
# def protected_endpoint(user_info: Dict = Depends(require_auth)):
#     user_id = user_info.get('sub')
#     # endpoint logic
#     return {"user_id": user_id}
#
# @app.get("/api/optional-auth")
# def optional_endpoint(user_info: Optional[Dict] = Depends(optional_auth)):
#     if user_info:
#         # User authenticated
#         user_id = user_info.get('sub')
#     else:
#         # Anonymous access
#         pass
#     return {"result": "data"}

"""
Authentication caching utilities for optimizing auth performance.

Provides Redis-based caching for token validation results when Redis is configured.
Falls back to no caching if Redis is not available.
"""
import logging
import hashlib
import json
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

# Global Redis client (lazy initialized)
_redis_client: Optional[Any] = None
_redis_enabled: bool = False


def init_redis_cache(redis_url: Optional[str], redis_password: Optional[str]) -> None:
    """Initialize Redis connection for auth caching (optional).
    
    Args:
        redis_url: Redis connection URL (e.g., "redis://redis:6379/0")
        redis_password: Redis password (if required)
    """
    global _redis_client, _redis_enabled
    
    if not redis_url:
        logger.info("Redis not configured - auth caching disabled")
        _redis_enabled = False
        return
    
    try:
        import redis
        
        # Parse password from URL or use separate parameter
        if redis_password:
            _redis_client = redis.from_url(redis_url, password=redis_password, decode_responses=True)
        else:
            _redis_client = redis.from_url(redis_url, decode_responses=True)
        
        # Test connection
        _redis_client.ping()
        _redis_enabled = True
        logger.info("Redis auth caching enabled: %s", redis_url)
    except ImportError:
        logger.warning("Redis library not installed - auth caching disabled")
        _redis_enabled = False
    except Exception as e:
        logger.warning("Failed to connect to Redis: %s - auth caching disabled", e)
        _redis_enabled = False


def get_cached_token_validation(token: str) -> Optional[Dict[str, Any]]:
    """Get cached token validation result from Redis.
    
    Args:
        token: Bearer token string
        
    Returns:
        Cached user info dict if found, None otherwise
    """
    if not _redis_enabled or not _redis_client:
        return None
    
    try:
        # Use SHA256 hash of token as cache key (don't store raw tokens)
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        cache_key = f"auth:token:{token_hash}"
        
        cached_data = _redis_client.get(cache_key)
        if cached_data:
            logger.debug("Token validation cache HIT")
            return json.loads(cached_data)
        
        logger.debug("Token validation cache MISS")
        return None
    except Exception as e:
        logger.warning("Redis cache read failed: %s", e)
        return None


def cache_token_validation(token: str, user_info: Dict[str, Any], ttl_seconds: int = 60) -> None:
    """Cache token validation result in Redis.
    
    Args:
        token: Bearer token string
        user_info: User information dict to cache
        ttl_seconds: Cache TTL in seconds (default: 1 minute)
    """
    if not _redis_enabled or not _redis_client:
        return
    
    try:
        # Use SHA256 hash of token as cache key
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        cache_key = f"auth:token:{token_hash}"
        
        _redis_client.setex(
            cache_key,
            ttl_seconds,
            json.dumps(user_info)
        )
        logger.debug("Cached token validation (TTL: %d seconds)", ttl_seconds)
    except Exception as e:
        logger.warning("Redis cache write failed: %s", e)


def invalidate_token_cache(token: str) -> None:
    """Invalidate cached token validation result.
    
    Call this when token is revoked or expires.
    
    Args:
        token: Bearer token string to invalidate
    """
    if not _redis_enabled or not _redis_client:
        return
    
    try:
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        cache_key = f"auth:token:{token_hash}"
        
        deleted = _redis_client.delete(cache_key)
        if deleted:
            logger.debug("Invalidated token cache")
    except Exception as e:
        logger.warning("Redis cache invalidation failed: %s", e)


def invalidate_token_cache_by_user_id(user_id: str) -> None:
    """Invalidate all cached tokens for a specific user.
    
    Less precise than invalidating specific tokens, but useful when
    we don't have the token string (e.g., bulk revocation).
    
    WARNING: This uses SCAN which can be slow on large Redis instances.
    Use sparingly.
    
    Args:
        user_id: User ID to invalidate tokens for
    """
    if not _redis_enabled or not _redis_client:
        return
    
    try:
        # Find all token cache keys and check if they match the user
        cursor = 0
        pattern = "auth:token:*"
        deleted_count = 0
        
        while True:
            cursor, keys = _redis_client.scan(cursor, match=pattern, count=100)
            
            for key in keys:
                try:
                    cached_data = _redis_client.get(key)
                    if cached_data:
                        user_info = json.loads(cached_data)
                        if user_info.get('sub') == user_id:
                            _redis_client.delete(key)
                            deleted_count += 1
                except Exception:
                    continue
            
            if cursor == 0:
                break
        
        if deleted_count > 0:
            logger.info("Invalidated %d cached tokens for user %s", deleted_count, user_id)
    except Exception as e:
        logger.warning("Bulk token cache invalidation failed: %s", e)

"""
OIDC authentication and JWT token management.

Supports:
- OIDC login/logout flow with Authelia
- JWT token generation for API access
- Session cookie management
- Token validation for both session and bearer tokens
"""

import hashlib
import logging
import secrets
import json
import base64
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any
from urllib.parse import urlencode

from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError
from authlib.integrations.httpx_client import AsyncOAuth2Client
from jose import JWTError, jwt
from pydantic import SecretStr
from sqlmodel import Session, select

from .config import load_settings

logger = logging.getLogger(__name__)

# Argon2id parameters for password/token hashing (strong production config)
ARGON2_TIME_COST = 8          # 8 iterations
ARGON2_MEMORY_COST = 262144   # 256 MB (4x default)
ARGON2_PARALLELISM = 8        # 8 threads (2x default)
ARGON2_HASH_LEN = 32          # 32 bytes (2x default)
ARGON2_SALT_LEN = 16          # 16 bytes (2x default)

# Basic Auth rate limiting (exponential backoff)
BASIC_AUTH_MAX_ATTEMPTS = 3
BASIC_AUTH_LOCKOUT_DELAYS = [30, 60, 300, 900]  # 30s, 1min, 5min, 15min


def get_password_hasher() -> PasswordHasher:
    """Return configured Argon2id PasswordHasher with strong parameters."""
    return PasswordHasher(
        time_cost=ARGON2_TIME_COST,
        memory_cost=ARGON2_MEMORY_COST,
        parallelism=ARGON2_PARALLELISM,
        hash_len=ARGON2_HASH_LEN,
        salt_len=ARGON2_SALT_LEN
    )


class OIDCClient:
    """Manages OIDC authentication flow with Authelia.
    
    Supports PKCE (Proof Key for Code Exchange) for enhanced security.
    """
    
    def __init__(self, settings, session_manager=None):
        self.settings = settings
        self.provider_url = settings.oidc_provider_url
        self.client_id = settings.oidc_client_id
        self.client_secret = settings.oidc_client_secret.get_secret_value() if settings.oidc_client_secret else None
        self.redirect_uri = settings.oidc_redirect_uri
        self.scopes = settings.oidc_scopes

        # SSL verification settings
        self.verify_ssl = settings.oidc_verify_ssl
        self.ca_bundle = settings.oidc_ca_bundle_path if settings.oidc_ca_bundle_path else True
        # If ca_bundle_path not set, use default True (system CA bundle)

        # PKCE state storage (uses session_manager if provided, otherwise fallback to in-memory)
        self.session_manager = session_manager
        self.pkce_states = {} if not session_manager else None
    
    def _generate_pkce_pair(self) -> Dict[str, str]:
        """Generate PKCE code_verifier and code_challenge for S256."""
        # Generate random code_verifier (43-128 characters, unreserved characters only)
        code_verifier = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode('utf-8').rstrip('=')
        
        # Create code_challenge as SHA256(code_verifier)
        code_challenge = base64.urlsafe_b64encode(
            hashlib.sha256(code_verifier.encode('utf-8')).digest()
        ).decode('utf-8').rstrip('=')
        
        return {
            'code_verifier': code_verifier,
            'code_challenge': code_challenge
        }
        
    def get_authorization_url(self, state: str) -> str:
        """Generate authorization URL for OIDC provider with PKCE.

        Generates PKCE code_challenge and stores code_verifier for later token exchange.
        """
        # Generate PKCE pair
        pkce = self._generate_pkce_pair()

        # Store PKCE state (use session_manager if available, otherwise in-memory)
        if self.session_manager:
            self.session_manager.store_pkce_state(state, pkce)
        else:
            self.pkce_states[state] = pkce

        params = {
            'client_id': self.client_id,
            'redirect_uri': self.redirect_uri,
            'response_type': 'code',
            'scope': self.scopes,
            'state': state,
            'code_challenge': pkce['code_challenge'],
            'code_challenge_method': 'S256',  # Use SHA256 for code challenge
        }
        return f"{self.provider_url}/api/oidc/authorization?{urlencode(params)}"
    
    async def exchange_code_for_token(self, code: str, state: str) -> Dict[str, Any]:
        """Exchange authorization code for tokens (backend call).

        Verifies OIDC provider SSL certificate to prevent MITM attacks.
        Uses PKCE code_verifier to prevent authorization code interception.
        """
        # Get the stored code_verifier for this state
        if self.session_manager:
            pkce_data = self.session_manager.get_pkce_state(state)
            if not pkce_data:
                raise ValueError("Invalid state - PKCE verifier not found")
            code_verifier = pkce_data['code_verifier']
        else:
            if state not in self.pkce_states:
                raise ValueError("Invalid state - PKCE verifier not found")
            code_verifier = self.pkce_states[state]['code_verifier']
            del self.pkce_states[state]  # Clean up
        
        # Determine SSL verification parameter
        verify = self.ca_bundle if self.verify_ssl else False
        
        async with AsyncOAuth2Client(
            client_id=self.client_id,
            client_secret=self.client_secret,
            verify=verify  # SSL certificate verification
        ) as client:
            token = await client.fetch_token(
                f"{self.provider_url}/api/oidc/token",
                code=code,
                redirect_uri=self.redirect_uri,
                code_verifier=code_verifier,  # Send PKCE verifier
            )
            return token
    
    async def get_userinfo(self, access_token: str) -> Dict[str, Any]:
        """Get user info from OIDC provider.
        
        Verifies OIDC provider SSL certificate to prevent MITM attacks.
        """
        # Determine SSL verification parameter
        verify = self.ca_bundle if self.verify_ssl else False
        
        async with AsyncOAuth2Client(
            client_id=self.client_id,
            client_secret=self.client_secret,
            token={'access_token': access_token, 'token_type': 'Bearer'},
            verify=verify  # SSL certificate verification
        ) as client:
            userinfo = await client.get(f"{self.provider_url}/api/oidc/userinfo")
            return userinfo.json()


class JWTManager:
    """Manages JWT token generation and validation for API access."""

    def __init__(self, secret: str | SecretStr | None, expiry_days: int = 30):
        # Handle both str and SecretStr - always convert to string
        if isinstance(secret, SecretStr):
            self.secret: str = secret.get_secret_value()
        else:
            self.secret: str = secret if secret else ""
        self.expiry_days = expiry_days
        self.algorithm = "HS256"
    
    def create_token(self, user_id: str, token_jti: Optional[str] = None) -> str:
        """Generate a new JWT token for API access."""
        payload = {
            'sub': user_id,  # Subject (user ID)
            'iat': datetime.now(timezone.utc),
            'exp': datetime.now(timezone.utc) + timedelta(days=self.expiry_days),
        }
        if token_jti:
            payload['jti'] = token_jti  # JWT ID for token revocation tracking
        
        return jwt.encode(payload, self.secret, algorithm=self.algorithm)
    
    def verify_token(self, token: str) -> Optional[Dict[str, Any]]:
        """Verify and decode JWT token."""
        try:
            payload = jwt.decode(token, self.secret, algorithms=[self.algorithm])
            return payload
        except JWTError as e:
            logger.debug(f"JWT verification failed: {e}")
            return None


def verify_share_token_db(token: str, filename: str, engine) -> bool:
    """Verify a share token against the database using Argon2."""
    from sqlmodel import select
    from .models import FileShareToken
    from .db_helpers import session_scope
    
    ph = PasswordHasher()
    try:
        with session_scope(engine) as session:
            # Find tokens for this filename that are not expired
            now = datetime.now(timezone.utc)
            stmt = select(FileShareToken).where(
                FileShareToken.filename == filename,
                FileShareToken.expires_at > now
            )
            candidates = session.exec(stmt).all()
            
            for candidate in candidates:
                try:
                    ph.verify(candidate.token_hash, token)
                    # Valid token found
                    candidate.used_count += 1
                    session.add(candidate)
                    session.commit()
                    return True
                except VerifyMismatchError:
                    continue
            
            return False
    except Exception as e:
        logger.exception(f"Error verifying share token: {e}")
        return False


class SessionManager:
    """Manages session state (stored in application memory or cache)."""
    
    def __init__(self, expiry_seconds: int = 86400):
        self.expiry_seconds = expiry_seconds
        self._sessions: Dict[str, Dict[str, Any]] = {}
    
    def create_session(self, user_id: str, user_info: Dict[str, Any]) -> str:
        """Create a new session, return session ID."""
        session_id = secrets.token_urlsafe(32)
        self._sessions[session_id] = {
            'user_id': user_id,
            'user_info': user_info,
            'created_at': datetime.now(timezone.utc),
            'last_activity': datetime.now(timezone.utc),
        }
        logger.debug(f"Session created: {session_id} for user {user_id}")
        return session_id
    
    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get session data, return None if expired."""
        session = self._sessions.get(session_id)
        if not session:
            return None
        
        created = session['created_at']
        if datetime.now(timezone.utc) - created > timedelta(seconds=self.expiry_seconds):
            del self._sessions[session_id]
            logger.debug(f"Session expired: {session_id}")
            return None
        
        session['last_activity'] = datetime.now(timezone.utc)
        return session
    
    def revoke_session(self, session_id: str) -> bool:
        """Revoke (delete) a session."""
        if session_id in self._sessions:
            del self._sessions[session_id]
            logger.debug(f"Session revoked: {session_id}")
            return True
        return False
    
    def cleanup_expired(self):
        """Remove all expired sessions (call periodically)."""
        now = datetime.now(timezone.utc)
        expired = [
            sid for sid, session in self._sessions.items()
            if now - session['created_at'] > timedelta(seconds=self.expiry_seconds)
        ]
        for sid in expired:
            del self._sessions[sid]
        if expired:
            logger.debug(f"Cleaned up {len(expired)} expired sessions")

    # OAuth/PKCE state storage methods
    def store_oauth_state(self, state: str, ttl_seconds: int = 300) -> bool:
        """Store OAuth state token with expiry (default 5 minutes)."""
        if not hasattr(self, '_oauth_states'):
            self._oauth_states = {}
        self._oauth_states[state] = datetime.now(timezone.utc)
        return True

    def verify_oauth_state(self, state: str) -> bool:
        """Verify and consume OAuth state token."""
        if not hasattr(self, '_oauth_states'):
            return False
        if state not in self._oauth_states:
            return False
        state_time = self._oauth_states[state]
        if datetime.now(timezone.utc) - state_time > timedelta(seconds=300):
            del self._oauth_states[state]
            return False
        del self._oauth_states[state]
        return True

    def store_pkce_state(self, state: str, pkce_data: Dict[str, str], ttl_seconds: int = 300) -> bool:
        """Store PKCE verifier with expiry (default 5 minutes)."""
        if not hasattr(self, '_pkce_states'):
            self._pkce_states = {}
        self._pkce_states[state] = {
            'data': pkce_data,
            'created_at': datetime.now(timezone.utc)
        }
        return True

    def get_pkce_state(self, state: str) -> Optional[Dict[str, str]]:
        """Get and consume PKCE verifier."""
        if not hasattr(self, '_pkce_states'):
            return None
        if state not in self._pkce_states:
            return None
        entry = self._pkce_states[state]
        if datetime.now(timezone.utc) - entry['created_at'] > timedelta(seconds=300):
            del self._pkce_states[state]
            return None
        pkce_data = entry['data']
        del self._pkce_states[state]
        return pkce_data


class RedisSessionManager:
    """Manages session state using Redis as backend."""
    
    def __init__(self, redis_url: str, redis_password: str, expiry_seconds: int = 86400):
        import redis
        
        self.expiry_seconds = expiry_seconds
        self.redis_client = redis.from_url(
            redis_url,
            password=redis_password,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_keepalive=True
        )
        
        try:
            self.redis_client.ping()
            logger.info(f"Connected to Redis at {redis_url}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    def create_session(self, user_id: str, user_info: Dict[str, Any]) -> str:
        """Create a new session, return session ID."""
        session_id = secrets.token_urlsafe(32)
        
        session_data = {
            'user_id': user_id,
            'user_info': user_info,  # Store as dict, will serialize to JSON
            'created_at': datetime.now(timezone.utc).isoformat(),
            'last_activity': datetime.now(timezone.utc).isoformat(),
        }
        
        # Use SETEX to set with automatic expiry
        self.redis_client.setex(
            f"session:{session_id}",
            self.expiry_seconds,
            json.dumps(session_data)
        )
        
        logger.debug(f"Session created in Redis: {session_id} for user {user_id}")
        return session_id
    
    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get session data, return None if expired or not found."""
        try:
            session_data_str = self.redis_client.get(f"session:{session_id}")
            if not session_data_str:
                return None
            
            # Update TTL on access
            self.redis_client.expire(f"session:{session_id}", self.expiry_seconds)
            
            # Parse from JSON
            return json.loads(session_data_str)
        except Exception as e:
            logger.debug(f"Error retrieving session {session_id}: {e}")
            return None
    
    def revoke_session(self, session_id: str) -> bool:
        """Revoke (delete) a session."""
        try:
            result = self.redis_client.delete(f"session:{session_id}")
            if result:
                logger.debug(f"Session revoked in Redis: {session_id}")
            return result > 0
        except Exception as e:
            logger.debug(f"Error revoking session {session_id}: {e}")
            return False
    
    def cache_stats(self, stats: Dict[str, Any], ttl_seconds: int = 60) -> bool:
        """Cache application statistics with TTL (default 60 seconds).
        
        Args:
            stats: Statistics dictionary to cache
            ttl_seconds: Time-to-live in seconds (default 60)
            
        Returns:
            True if cached successfully, False otherwise
        """
        try:
            self.redis_client.setex(
                "cache:stats",
                ttl_seconds,
                json.dumps(stats)
            )
            logger.debug(f"Stats cached in Redis with TTL {ttl_seconds}s")
            return True
        except Exception as e:
            logger.exception(f"Failed to cache stats: {e}")
            return False
    
    def get_cached_stats(self) -> Optional[Dict[str, Any]]:
        """Retrieve cached statistics if available.
        
        Returns:
            Stats dictionary if cache hit, None if cache miss or expired
        """
        try:
            cached_data = self.redis_client.get("cache:stats")
            if cached_data:
                logger.debug("Stats cache hit")
                return json.loads(cached_data)
            logger.debug("Stats cache miss")
            return None
        except Exception as e:
            logger.exception(f"Error retrieving cached stats: {e}")
            return None
    

    def cleanup_expired(self):
        """Redis handles TTL automatically, but we can log for monitoring."""
        try:
            # Get count of active sessions
            pattern = "session:*"
            cursor = 0
            count = 0

            while True:
                cursor, keys = self.redis_client.scan(cursor, match=pattern, count=100)
                count += len(keys)
                if cursor == 0:
                    break

            if count > 0:
                logger.debug(f"Active sessions in Redis: {count}")
        except Exception as e:
            logger.debug(f"Error checking active sessions: {e}")

    # OAuth/PKCE state storage methods
    def store_oauth_state(self, state: str, ttl_seconds: int = 300) -> bool:
        """Store OAuth state token with expiry (default 5 minutes) in Redis."""
        try:
            self.redis_client.setex(
                f"oauth_state:{state}",
                ttl_seconds,
                datetime.now(timezone.utc).isoformat()
            )
            logger.debug(f"OAuth state stored in Redis: {state}")
            return True
        except Exception as e:
            logger.exception(f"Failed to store OAuth state: {e}")
            return False

    def verify_oauth_state(self, state: str) -> bool:
        """Verify and consume OAuth state token from Redis."""
        try:
            result = self.redis_client.get(f"oauth_state:{state}")
            if result:
                self.redis_client.delete(f"oauth_state:{state}")
                logger.debug(f"OAuth state verified and consumed: {state}")
                return True
            return False
        except Exception as e:
            logger.exception(f"Error verifying OAuth state: {e}")
            return False

    def store_pkce_state(self, state: str, pkce_data: Dict[str, str], ttl_seconds: int = 300) -> bool:
        """Store PKCE verifier with expiry (default 5 minutes) in Redis."""
        try:
            self.redis_client.setex(
                f"pkce_state:{state}",
                ttl_seconds,
                json.dumps(pkce_data)
            )
            logger.debug(f"PKCE state stored in Redis: {state}")
            return True
        except Exception as e:
            logger.exception(f"Failed to store PKCE state: {e}")
            return False

    def get_pkce_state(self, state: str) -> Optional[Dict[str, str]]:
        """Get and consume PKCE verifier from Redis."""
        try:
            pkce_data_str = self.redis_client.get(f"pkce_state:{state}")
            if not pkce_data_str:
                return None
            self.redis_client.delete(f"pkce_state:{state}")
            logger.debug(f"PKCE state retrieved and consumed: {state}")
            # Parse from JSON (decode_responses=True ensures string type)
            return json.loads(str(pkce_data_str))
        except Exception as e:
            logger.exception(f"Error retrieving PKCE state: {e}")
            return None


class OIDCAuthContext:
    """Singleton context for OIDC and JWT handling."""

    _instance: Optional['OIDCAuthContext'] = None
    enabled: bool
    oidc_client: Optional[OIDCClient]
    jwt_manager: Optional['JWTManager']
    session_manager: 'SessionManager | RedisSessionManager'

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            settings = load_settings()

            cls._instance.enabled = settings.oidc_enabled
            cls._instance.oidc_client = None
            cls._instance.jwt_manager = None
            
            # Choose session manager based on Redis configuration
            if settings.redis_url:
                try:
                    redis_password = settings.redis_password.get_secret_value()
                    cls._instance.session_manager = RedisSessionManager(
                        settings.redis_url,
                        redis_password,
                        settings.session_expiry_seconds
                    )
                    logger.info("Using Redis for session storage")
                except Exception as e:
                    logger.warning(f"Failed to initialize Redis session manager: {e}. Falling back to in-memory sessions.")
                    cls._instance.session_manager = SessionManager(settings.session_expiry_seconds)
            else:
                cls._instance.session_manager = SessionManager(settings.session_expiry_seconds)
                logger.info("Using in-memory session storage (redis_url not configured)")
            
            # Initialize JWT manager for OIDC or Basic Auth
            if settings.oidc_enabled or settings.basic_auth:
                if not settings.jwt_secret:
                    logger.error("JWT authentication enabled but jwt_secret is not set")
                else:
                    cls._instance.jwt_manager = JWTManager(
                        settings.jwt_secret,
                        settings.jwt_expiry_days
                    )
                    if settings.basic_auth:
                        logger.info("Basic Auth authentication enabled")

            # Initialize OIDC client if OIDC is enabled
            if settings.oidc_enabled:
                if not all([settings.oidc_provider_url, settings.oidc_client_id,
                           settings.oidc_client_secret, settings.oidc_redirect_uri]):
                    logger.error("OIDC enabled but missing required settings")
                else:
                    cls._instance.oidc_client = OIDCClient(settings, cls._instance.session_manager)
                    logger.info("OIDC authentication enabled")

        return cls._instance


def hash_token(token: str) -> str:
    """Hash a token for storage in database using Argon2 with strong parameters."""
    ph = get_password_hasher()
    return ph.hash(token)


def verify_basic_auth_user(username: str, password: str, engine) -> Optional[Dict[str, Any]]:
    """Verify Basic Auth credentials against database with rate limiting.

    Rate limit: 3 attempts, then lockout for 30s → 1m → 5m → 15m (repeating).

    Returns user info dict if valid, None otherwise.
    """
    from .models import BasicAuthUser

    with Session(engine) as session:
        stmt = select(BasicAuthUser).where(
            BasicAuthUser.username == username,
            BasicAuthUser.enabled == True
        )
        user = session.exec(stmt).first()

        if not user:
            return None

        now = datetime.now(timezone.utc)

        # Check if user is locked out
        # Make locked_until timezone-aware if it's naive (for comparison with now)
        if user.locked_until:
            locked_until = user.locked_until if user.locked_until.tzinfo else user.locked_until.replace(tzinfo=timezone.utc)
            if now < locked_until:
                return None
            user.locked_until = None

        try:
            ph = get_password_hasher()
            ph.verify(user.password_hash, password)

            # Successful login - reset counters
            user.last_used_at = now
            user.failed_attempts = 0
            user.locked_until = None
            session.add(user)
            session.commit()

            return {
                'sub': user.username,
                'name': user.username,
                'basic_auth': True
            }
        except Exception:
            # Failed attempt - increment counter
            user.failed_attempts += 1

            # Apply lockout if max attempts exceeded
            if user.failed_attempts >= BASIC_AUTH_MAX_ATTEMPTS:
                # Calculate lockout delay: each failed attempt after initial lockout escalates the delay
                # attempt 3 → 30s, attempt 4 → 1min, attempt 5 → 5min, attempt 6+ → 15min
                lockout_index = min(
                    user.failed_attempts - BASIC_AUTH_MAX_ATTEMPTS,
                    len(BASIC_AUTH_LOCKOUT_DELAYS) - 1
                )
                delay_seconds = BASIC_AUTH_LOCKOUT_DELAYS[lockout_index]
                user.locked_until = now + timedelta(seconds=delay_seconds)

            session.add(user)
            session.commit()
            return None


def generate_state_token() -> str:
    """Generate CSRF state token for OIDC flow."""
    return secrets.token_urlsafe(32)


def verify_api_token_not_revoked(token: str, engine) -> Optional[Dict[str, Any]]:
    """
    Verify token exists in DB, is not revoked, and has not expired.
    
    Args:
        token: Plain text API token from bearer header
        engine: SQLAlchemy engine for DB access
        
    Returns:
        Dict with user info if valid, None otherwise
    """
    from sqlmodel import select, Session
    from .models import UserToken
    from .db_helpers import session_scope
    
    ph = PasswordHasher()
    try:
        with session_scope(engine) as session:
            # Fetch all non-revoked tokens for this user to verify against
            stmt = select(UserToken).where(UserToken.revoked == False)
            tokens = session.exec(stmt).all()
            
            user_token = None
            for candidate in tokens:
                try:
                    ph.verify(candidate.token_hash, token)
                    user_token = candidate
                    break
                except VerifyMismatchError:
                    continue
            
            if not user_token:
                logger.debug("Token not found or revoked")
                return None
            
            if user_token.expires_at:
                now = datetime.now(timezone.utc)
                expires_at = user_token.expires_at
                
                if expires_at.tzinfo is None:
                    expires_at = expires_at.replace(tzinfo=timezone.utc)
                
                if now > expires_at:
                    logger.warning(f"Token rejected - EXPIRED: (expired at {user_token.expires_at}, current time: {now}, user: {user_token.user_id})")
                    return None
            user_token.last_used_at = datetime.now(timezone.utc)
            session.add(user_token)
            session.commit()
            
            logger.debug(f"API token validated on use: (user: {user_token.user_id}, name: {user_token.name})")
            return {'sub': user_token.user_id, 'token_id': str(user_token.id)}
    except Exception as e:
        logger.exception(f"Error verifying API token on use: {e}")
        return None


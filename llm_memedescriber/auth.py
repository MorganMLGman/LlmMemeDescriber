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
import base64
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any
from urllib.parse import urlencode

from authlib.integrations.httpx_client import AsyncOAuth2Client
from jose import JWTError, jwt
from pydantic import BaseModel

from .config import load_settings

logger = logging.getLogger(__name__)


class OIDCClient:
    """Manages OIDC authentication flow with Authelia.
    
    Supports PKCE (Proof Key for Code Exchange) for enhanced security.
    """
    
    def __init__(self, settings):
        self.settings = settings
        self.provider_url = settings.oidc_provider_url
        self.client_id = settings.oidc_client_id
        self.client_secret = settings.oidc_client_secret
        self.redirect_uri = settings.oidc_redirect_uri
        self.scopes = settings.oidc_scopes
        
        # SSL verification settings
        self.verify_ssl = settings.oidc_verify_ssl
        self.ca_bundle = settings.oidc_ca_bundle_path if settings.oidc_ca_bundle_path else True
        # If ca_bundle_path not set, use default True (system CA bundle)
        
        # PKCE state storage (in production, use Redis or similar)
        self.pkce_states = {}
    
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
    
    def __init__(self, secret: str, expiry_days: int = 30):
        self.secret = secret
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
        
        # Check expiry
        created = session['created_at']
        if datetime.now(timezone.utc) - created > timedelta(seconds=self.expiry_seconds):
            del self._sessions[session_id]
            logger.debug(f"Session expired: {session_id}")
            return None
        
        # Update last activity
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


class OIDCAuthContext:
    """Singleton context for OIDC and JWT handling."""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            settings = load_settings()
            
            cls._instance.enabled = settings.oidc_enabled
            cls._instance.oidc_client = None
            cls._instance.jwt_manager = None
            cls._instance.session_manager = SessionManager(settings.session_expiry_seconds)
            
            if settings.oidc_enabled:
                if not all([settings.oidc_provider_url, settings.oidc_client_id, 
                           settings.oidc_client_secret, settings.oidc_redirect_uri, 
                           settings.jwt_secret]):
                    logger.error("OIDC enabled but missing required settings")
                else:
                    cls._instance.oidc_client = OIDCClient(settings)
                    cls._instance.jwt_manager = JWTManager(
                        settings.jwt_secret,
                        settings.jwt_expiry_days
                    )
                    logger.info("OIDC authentication enabled")
        
        return cls._instance


def hash_token(token: str) -> str:
    """Hash a token for storage in database."""
    return hashlib.sha256(token.encode()).hexdigest()


def generate_state_token() -> str:
    """Generate CSRF state token for OIDC flow."""
    return secrets.token_urlsafe(32)


def verify_api_token_not_revoked(token: str, engine) -> Optional[Dict[str, Any]]:
    """
    Verify token exists in DB and is not revoked.
    
    Args:
        token: Plain text API token from bearer header
        engine: SQLAlchemy engine for DB access
        
    Returns:
        Dict with user info if valid, None otherwise
    """
    from sqlmodel import select, Session
    from .models import UserToken
    from .db_helpers import session_scope
    
    token_hash = hash_token(token)
    try:
        with session_scope(engine) as session:
            # Query using SQLModel
            stmt = select(UserToken).where(
                UserToken.token_hash == token_hash,
                UserToken.revoked == False
            )
            user_token = session.exec(stmt).first()
            
            if not user_token:
                logger.debug(f"Token not found or revoked: {token_hash[:8]}...")
                return None
            
            # Token is valid - update last_used_at
            user_token.last_used_at = datetime.now(timezone.utc)
            session.add(user_token)
            session.commit()
            
            logger.debug(f"API token validated for user: {user_token.user_id}")
            return {'sub': user_token.user_id, 'token_id': str(user_token.id)}
    except Exception as e:
        logger.error(f"Error verifying API token: {e}")
        return None

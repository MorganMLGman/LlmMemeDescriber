"""Tests for Redis session management."""
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone
from llm_memedescriber.auth import RedisSessionManager, SessionManager


class TestSessionManager:
    """Test in-memory session manager."""
    
    def test_create_session(self):
        manager = SessionManager(expiry_seconds=3600)
        user_id = "user123"
        user_info = {"email": "user@example.com", "name": "Test User"}
        
        session_id = manager.create_session(user_id, user_info)
        
        assert session_id is not None
        assert len(session_id) > 0
    
    def test_get_session(self):
        manager = SessionManager(expiry_seconds=3600)
        user_id = "user123"
        user_info = {"email": "user@example.com", "name": "Test User"}
        
        session_id = manager.create_session(user_id, user_info)
        session = manager.get_session(session_id)
        
        assert session is not None
        assert session['user_id'] == user_id
        assert session['user_info'] == user_info
    
    def test_get_session_not_found(self):
        manager = SessionManager(expiry_seconds=3600)
        session = manager.get_session("nonexistent")
        
        assert session is None
    
    def test_revoke_session(self):
        manager = SessionManager(expiry_seconds=3600)
        user_id = "user123"
        user_info = {"email": "user@example.com"}
        
        session_id = manager.create_session(user_id, user_info)
        assert manager.revoke_session(session_id) is True
        assert manager.get_session(session_id) is None
    
    def test_revoke_nonexistent_session(self):
        manager = SessionManager(expiry_seconds=3600)
        assert manager.revoke_session("nonexistent") is False
    
    def test_session_expiry(self):
        """Test that expired sessions are removed by cleanup."""
        manager = SessionManager(expiry_seconds=1)  # 1 second expiry
        user_id = "user123"
        user_info = {"email": "user@example.com"}
        
        session_id = manager.create_session(user_id, user_info)
        assert manager.get_session(session_id) is not None
        
        # Simulate time passing by directly manipulating the session
        manager._sessions[session_id]['created_at'] = datetime.now(timezone.utc) - __import__('datetime').timedelta(seconds=10)
        
        # Cleanup should remove it
        manager.cleanup_expired()
        assert manager.get_session(session_id) is None
    
    def test_cleanup_expired(self):
        manager = SessionManager(expiry_seconds=1)
        
        # Create multiple sessions
        session_id_1 = manager.create_session("user1", {"email": "user1@example.com"})
        session_id_2 = manager.create_session("user2", {"email": "user2@example.com"})
        
        assert len(manager._sessions) == 2
        
        # Expire first session
        import datetime as dt
        manager._sessions[session_id_1]['created_at'] = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=10)
        
        manager.cleanup_expired()
        
        # First should be gone, second should exist
        assert manager.get_session(session_id_1) is None
        assert manager.get_session(session_id_2) is not None


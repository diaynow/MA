from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from src.utils.auth import verify_token
from src.config import settings
from typing import Optional
from uuid import UUID

security = HTTPBearer()


def get_current_user_email(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> str:
    """Get current authenticated user email from JWT token"""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    email = verify_token(credentials.credentials, credentials_exception)
    return email


def get_current_user_id(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> Optional[UUID]:
    """Get current authenticated user ID from JWT token
    
    Note: In microservices architecture, we don't have direct DB access to users.
    We only validate JWT token. User ID should be stored in token payload if needed.
    For now, we return None and rely on email for identification.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    # For now, we only validate token and return email
    # In production, you might want to include user_id in JWT payload
    email = verify_token(credentials.credentials, credentials_exception)
    # Return None as we don't have user_id in token yet
    # This can be extended to decode user_id from token if added to JWT payload
    return None

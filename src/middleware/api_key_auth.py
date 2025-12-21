"""Middleware for internal API key authentication"""
from fastapi import Depends, HTTPException, status, Header
from sqlalchemy.orm import Session
from typing import Optional
from src.models.database import get_db, ApiKey
from datetime import datetime


def verify_api_key(
    authorization: Optional[str] = Header(None),
    db: Session = Depends(get_db)
) -> ApiKey:
    """
    Verify internal API key from Authorization header.
    Supports both 'Token <key>' and 'Bearer <key>' formats for internal keys.
    """
    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authorization header",
            headers={"WWW-Authenticate": "Token"},
        )
    
    # Parse authorization header: "Token <key>" or "Bearer <key>"
    parts = authorization.split(" ", 1)
    if len(parts) != 2:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authorization header format. Expected: 'Token <key>' or 'Bearer <key>'",
            headers={"WWW-Authenticate": "Token"},
        )
    
    auth_type, api_key = parts
    
    # Accept both "Token" and "Bearer" for internal API keys
    # (to distinguish from user JWT which uses "Bearer")
    if auth_type.lower() not in ["token", "bearer"]:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authorization type. Use 'Token' or 'Bearer' for API keys",
            headers={"WWW-Authenticate": "Token"},
        )
    
    # Look up API key in database
    db_api_key = db.query(ApiKey).filter(
        ApiKey.key == api_key,
        ApiKey.is_active == "active"
    ).first()
    
    if not db_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or inactive API key",
            headers={"WWW-Authenticate": "Token"},
        )
    
    # Check expiration
    if db_api_key.expires_at and db_api_key.expires_at < datetime.utcnow():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key has expired",
            headers={"WWW-Authenticate": "Token"},
        )
    
    return db_api_key


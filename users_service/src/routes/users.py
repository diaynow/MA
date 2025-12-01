from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy.orm import Session
from datetime import timedelta
from src.models.database import User, get_db
from src.models.schemas import (
    SubscribeRequest,
    SubscriptionKeyUpdate,
    SuccessResponse,
    UserCreate,
    UserLogin,
    UserResponse,
)
from src.controllers.crud import SubscriptionCRUD, UserCRUD
from src.middleware.auth import get_current_active_user
from src.utils.auth import create_access_token
from src.config import settings

router = APIRouter(prefix="/api/users", tags=["users"])


@router.get("/", response_model=dict)
def get_users_api_info():
    """Get Users API information"""
    return {
        "success": True,
        "message": "Users API is running",
        "service": "users-api",
        "endpoints": {
            "POST /api/users": "Register a new user",
            "POST /api/users/login": "Login user and get access token",
            "PUT /api/users/me/subscription-key": "Save push subscription key",
            "POST /api/users/subscribe": "Subscribe to another user",
        }
    }


@router.post("/", response_model=SuccessResponse, status_code=status.HTTP_201_CREATED)
def register_user(
    user_data: UserCreate,
    db: Session = Depends(get_db)
):
    """Register a new user"""
    try:
        crud = UserCRUD(db)
        db_user = crud.create_user(user_data)
        
        return SuccessResponse(
            message="User registered successfully",
            data={
                "user": UserResponse.model_validate(db_user).model_dump()
            }
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@router.put("/me/subscription-key", response_model=SuccessResponse)
def save_subscription_key(
    payload: SubscriptionKeyUpdate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Persist push subscription key for the current user."""
    crud = SubscriptionCRUD(db)
    updated_user = crud.update_subscription_key(current_user.id, payload.subscription_key)
    if not updated_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )
    return SuccessResponse(
        message="Subscription key saved",
        data={"user": UserResponse.model_validate(updated_user).model_dump()},
    )


@router.post("/subscribe", status_code=status.HTTP_204_NO_CONTENT)
def subscribe_user(
    payload: SubscribeRequest,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Subscribe current user to another author."""
    crud = SubscriptionCRUD(db)
    try:
        crud.subscribe(current_user.id, payload.target_user_id)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/login", response_model=SuccessResponse)
def login_user(
    user_credentials: UserLogin,
    db: Session = Depends(get_db)
):
    """Authenticate user and return access token"""
    try:
        crud = UserCRUD(db)
        user = crud.authenticate_user(user_credentials.email, user_credentials.password)
        
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect email or password",
                headers={"WWW-Authenticate": "Bearer"},
            )
        
        access_token_expires = timedelta(minutes=settings.access_token_expire_minutes)
        access_token = create_access_token(
            data={"sub": user.email, "user_id": str(user.id)}, expires_delta=access_token_expires
        )
        
        return SuccessResponse(
            message="Login successful",
            data={
                "user": UserResponse.model_validate(user).model_dump(),
                "access_token": access_token,
                "token_type": "bearer"
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


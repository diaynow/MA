from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from datetime import timedelta
from src.models.database import get_db
from src.models.schemas import (
    UserCreate, UserLogin, UserResponse,
    SuccessResponse
)
from src.controllers.crud import UserCRUD
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
            "POST /api/users/login": "Login user and get access token"
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
                "user": UserResponse.from_orm(db_user).dict()
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
                "user": UserResponse.from_orm(user).dict(),
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


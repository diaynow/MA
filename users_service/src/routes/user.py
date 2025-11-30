from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from src.models.database import get_db, User
from src.models.schemas import (
    UserUpdate, UserResponse, SuccessResponse
)
from src.controllers.crud import UserCRUD
from src.middleware.auth import get_current_active_user

router = APIRouter(prefix="/api/user", tags=["user"])


@router.get("/", response_model=SuccessResponse)
def get_current_user_info(
    current_user: User = Depends(get_current_active_user)
):
    """Get current user information"""
    return SuccessResponse(
        message="User information retrieved successfully",
        data={
            "user": UserResponse.from_orm(current_user).dict()
        }
    )


@router.put("/", response_model=SuccessResponse)
def update_current_user(
    user_data: UserUpdate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Update current user information"""
    try:
        crud = UserCRUD(db)
        updated_user = crud.update_user(current_user.id, user_data)
        
        if not updated_user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )
        
        return SuccessResponse(
            message="User updated successfully",
            data={
                "user": UserResponse.from_orm(updated_user).dict()
            }
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(e)
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


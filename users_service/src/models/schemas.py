from pydantic import BaseModel, Field, EmailStr, validator
from typing import Optional
from datetime import datetime
from uuid import UUID


# User schemas
class UserBase(BaseModel):
    email: EmailStr = Field(..., description="User email")
    username: str = Field(..., min_length=3, max_length=50, description="Username")
    bio: Optional[str] = Field(None, max_length=500, description="User bio")
    image_url: Optional[str] = Field(None, max_length=500, description="User profile image URL")


class UserCreate(UserBase):
    password: str = Field(..., min_length=6, max_length=100, description="User password")

    @validator('username')
    def validate_username(cls, v):
        if not v.isalnum():
            raise ValueError('Username must contain only alphanumeric characters')
        return v


class UserLogin(BaseModel):
    email: EmailStr = Field(..., description="User email")
    password: str = Field(..., min_length=6, description="User password")


class UserUpdate(BaseModel):
    email: Optional[EmailStr] = Field(None, description="User email")
    username: Optional[str] = Field(None, min_length=3, max_length=50, description="Username")
    bio: Optional[str] = Field(None, max_length=500, description="User bio")
    image_url: Optional[str] = Field(None, max_length=500, description="User profile image URL")
    password: Optional[str] = Field(None, min_length=6, max_length=100, description="New password")

    @validator('username')
    def validate_username(cls, v):
        if v is not None and not v.isalnum():
            raise ValueError('Username must contain only alphanumeric characters')
        return v


class UserResponse(UserBase):
    id: UUID
    is_active: bool
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# Auth schemas
class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    email: Optional[str] = None


# Response schemas
class ErrorResponse(BaseModel):
    success: bool = False
    message: str
    errors: Optional[list] = None


class SuccessResponse(BaseModel):
    success: bool = True
    message: str
    data: Optional[dict] = None


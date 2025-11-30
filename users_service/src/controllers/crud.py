from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from src.models.database import User
from src.models.schemas import UserCreate, UserUpdate
from typing import Optional
import uuid


class UserCRUD:
    def __init__(self, db: Session):
        self.db = db

    def create_user(self, user_data: UserCreate) -> User:
        """Create a new user"""
        # Check if user already exists
        if self.db.query(User).filter(User.email == user_data.email).first():
            raise ValueError("Email already registered")
        
        if self.db.query(User).filter(User.username == user_data.username).first():
            raise ValueError("Username already taken")
        
        db_user = User(
            email=user_data.email,
            username=user_data.username,
            bio=user_data.bio,
            image_url=user_data.image_url
        )
        db_user.set_password(user_data.password)
        
        try:
            self.db.add(db_user)
            self.db.commit()
            self.db.refresh(db_user)
            return db_user
        except IntegrityError:
            self.db.rollback()
            raise ValueError("User with this email or username already exists")

    def get_user_by_email(self, email: str) -> Optional[User]:
        """Get user by email"""
        return self.db.query(User).filter(User.email == email).first()

    def get_user_by_username(self, username: str) -> Optional[User]:
        """Get user by username"""
        return self.db.query(User).filter(User.username == username).first()

    def get_user_by_id(self, user_id: uuid.UUID) -> Optional[User]:
        """Get user by ID"""
        return self.db.query(User).filter(User.id == user_id).first()

    def update_user(self, user_id: uuid.UUID, user_data: UserUpdate) -> Optional[User]:
        """Update user by ID"""
        db_user = self.get_user_by_id(user_id)
        if not db_user:
            return None
        
        # Check for email conflicts
        if user_data.email and user_data.email != db_user.email:
            if self.db.query(User).filter(User.email == user_data.email).first():
                raise ValueError("Email already registered")
        
        # Check for username conflicts
        if user_data.username and user_data.username != db_user.username:
            if self.db.query(User).filter(User.username == user_data.username).first():
                raise ValueError("Username already taken")
        
        # Update fields
        if user_data.email is not None:
            db_user.email = user_data.email
        if user_data.username is not None:
            db_user.username = user_data.username
        if user_data.bio is not None:
            db_user.bio = user_data.bio
        if user_data.image_url is not None:
            db_user.image_url = user_data.image_url
        if user_data.password is not None:
            db_user.set_password(user_data.password)
        
        try:
            self.db.commit()
            self.db.refresh(db_user)
            return db_user
        except IntegrityError:
            self.db.rollback()
            raise ValueError("User with this email or username already exists")

    def authenticate_user(self, email: str, password: str) -> Optional[User]:
        """Authenticate user with email and password"""
        user = self.get_user_by_email(email)
        if not user:
            return None
        if not user.verify_password(password):
            return None
        return user


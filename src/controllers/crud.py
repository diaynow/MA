from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from src.models.database import User, Article, Comment
from src.models.schemas import UserCreate, UserUpdate, ArticleCreate, ArticleUpdate, CommentCreate
from src.utils.auth import get_password_hash, verify_password
from src.utils.slug import generate_slug
from typing import List, Optional
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


class ArticleCRUD:
    def __init__(self, db: Session):
        self.db = db

    def create_article(self, article_data: ArticleCreate, author_id: uuid.UUID) -> Article:
        """Create a new article"""
        slug = generate_slug(article_data.title)
        
        # Ensure slug is unique
        original_slug = slug
        counter = 1
        while self.db.query(Article).filter(Article.slug == slug).first():
            slug = f"{original_slug}-{counter}"
            counter += 1
        
        db_article = Article(
            title=article_data.title,
            description=article_data.description,
            body=article_data.body,
            tag_list=article_data.tag_list or [],
            slug=slug,
            author_id=author_id
        )
        
        try:
            self.db.add(db_article)
            self.db.commit()
            self.db.refresh(db_article)
            return db_article
        except IntegrityError:
            self.db.rollback()
            raise ValueError("Article with this slug already exists")

    def get_article_by_slug(self, slug: str) -> Optional[Article]:
        """Get article by slug"""
        return self.db.query(Article).filter(Article.slug == slug).first()

    def get_articles(self, skip: int = 0, limit: int = 100) -> List[Article]:
        """Get all articles with pagination"""
        return self.db.query(Article).offset(skip).limit(limit).all()

    def get_articles_count(self) -> int:
        """Get total count of articles"""
        return self.db.query(Article).count()

    def get_user_articles(self, user_id: uuid.UUID, skip: int = 0, limit: int = 100) -> List[Article]:
        """Get articles by specific user"""
        return self.db.query(Article).filter(Article.author_id == user_id).offset(skip).limit(limit).all()

    def update_article(self, slug: str, article_data: ArticleUpdate, user_id: uuid.UUID) -> Optional[Article]:
        """Update article by slug (only by author)"""
        db_article = self.get_article_by_slug(slug)
        if not db_article:
            return None
        
        # Check if user is the author
        if db_article.author_id != user_id:
            raise ValueError("You can only update your own articles")
        
        # Update fields if provided
        if article_data.title is not None:
            db_article.title = article_data.title
            # Generate new slug if title changed
            new_slug = generate_slug(article_data.title)
            if new_slug != slug:
                # Check if new slug is unique
                original_slug = new_slug
                counter = 1
                while self.db.query(Article).filter(Article.slug == new_slug).first():
                    new_slug = f"{original_slug}-{counter}"
                    counter += 1
                db_article.slug = new_slug
        
        if article_data.description is not None:
            db_article.description = article_data.description
        
        if article_data.body is not None:
            db_article.body = article_data.body
        
        if article_data.tag_list is not None:
            db_article.tag_list = article_data.tag_list
        
        try:
            self.db.commit()
            self.db.refresh(db_article)
            return db_article
        except IntegrityError:
            self.db.rollback()
            raise ValueError("Article with this slug already exists")

    def delete_article(self, slug: str, user_id: uuid.UUID) -> bool:
        """Delete article by slug (only by author)"""
        db_article = self.get_article_by_slug(slug)
        if not db_article:
            return False
        
        # Check if user is the author
        if db_article.author_id != user_id:
            raise ValueError("You can only delete your own articles")
        
        self.db.delete(db_article)
        self.db.commit()
        return True


class CommentCRUD:
    def __init__(self, db: Session):
        self.db = db

    def create_comment(self, comment_data: CommentCreate, article_id: uuid.UUID, author_id: uuid.UUID) -> Comment:
        """Create a new comment"""
        db_comment = Comment(
            body=comment_data.body,
            article_id=article_id,
            author_id=author_id
        )
        
        try:
            self.db.add(db_comment)
            self.db.commit()
            self.db.refresh(db_comment)
            return db_comment
        except IntegrityError:
            self.db.rollback()
            raise ValueError("Failed to create comment")

    def get_comments_by_article(self, article_id: uuid.UUID, skip: int = 0, limit: int = 100) -> List[Comment]:
        """Get all comments for a specific article"""
        return self.db.query(Comment).filter(Comment.article_id == article_id).offset(skip).limit(limit).all()

    def get_comments_count_by_article(self, article_id: uuid.UUID) -> int:
        """Get total count of comments for a specific article"""
        return self.db.query(Comment).filter(Comment.article_id == article_id).count()

    def get_comment_by_id(self, comment_id: uuid.UUID) -> Optional[Comment]:
        """Get comment by ID"""
        return self.db.query(Comment).filter(Comment.id == comment_id).first()

    def delete_comment(self, comment_id: uuid.UUID, user_id: uuid.UUID) -> bool:
        """Delete comment by ID (only by author)"""
        db_comment = self.get_comment_by_id(comment_id)
        if not db_comment:
            return False
        
        # Check if user is the author
        if db_comment.author_id != user_id:
            raise ValueError("You can only delete your own comments")
        
        self.db.delete(db_comment)
        self.db.commit()
        return True
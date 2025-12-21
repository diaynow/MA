import logging

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from pydantic import PositiveInt
from typing import List
from uuid import UUID
from src.models.database import get_db
from src.models.schemas import (
    ArticleCreate, 
    ArticleUpdate, 
    ArticleResponse, 
    ArticleListResponse,
    SuccessResponse,
    ErrorResponse
)
from src.controllers.crud import ArticleCRUD
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi import HTTPException, status
from jose import JWTError
from src.config import settings
from src.tasks.saga import enqueue_moderation_task

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/articles", tags=["articles"])

security = HTTPBearer()


def get_user_id_from_token(credentials: HTTPAuthorizationCredentials = Depends(security)) -> UUID:
    """Extract user_id from JWT token"""
    
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    try:
        from jose import jwt
        payload = jwt.decode(credentials.credentials, settings.secret_key, algorithms=[settings.algorithm])
        user_id_str = payload.get("user_id")
        if user_id_str is None:
            raise credentials_exception
        return UUID(user_id_str)
    except (JWTError, ValueError, TypeError):
        raise credentials_exception


@router.post("/", response_model=SuccessResponse, status_code=status.HTTP_201_CREATED)
def create_article(
    article_data: ArticleCreate,
    user_id: UUID = Depends(get_user_id_from_token),
    db: Session = Depends(get_db)
):
    """Create a new article (status: DRAFT)"""
    try:
        crud = ArticleCRUD(db)
        db_article = crud.create_article(article_data, user_id)
        # Note: No notification on creation, only after publication
        
        return SuccessResponse(
            message="Article created successfully",
            data={
                "article": ArticleResponse.from_orm(db_article).dict(),
                "slug": db_article.slug
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


@router.get("/", response_model=SuccessResponse)
def get_articles(
    skip: PositiveInt = 0,
    limit: PositiveInt = 100,
    db: Session = Depends(get_db)
):
    """Get all articles with pagination"""
    try:
        crud = ArticleCRUD(db)
        articles = crud.get_articles(skip=skip, limit=limit)
        count = crud.get_articles_count()
        
        articles_data = [ArticleResponse.from_orm(article).dict() for article in articles]
        
        return SuccessResponse(
            message="Articles retrieved successfully",
            data={
                "articles": articles_data,
                "count": count
            }
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@router.get("/{slug}", response_model=SuccessResponse)
def get_article_by_slug(
    slug: str,
    db: Session = Depends(get_db)
):
    """Get article by slug"""
    try:
        crud = ArticleCRUD(db)
        article = crud.get_article_by_slug(slug)
        
        if not article:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Article not found"
            )
        
        return SuccessResponse(
            message="Article retrieved successfully",
            data={
                "article": ArticleResponse.from_orm(article).dict()
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@router.put("/{slug}", response_model=SuccessResponse)
def update_article(
    slug: str,
    article_data: ArticleUpdate,
    user_id: UUID = Depends(get_user_id_from_token),
    db: Session = Depends(get_db)
):
    """Update article by slug (only by author)"""
    try:
        crud = ArticleCRUD(db)
        updated_article = crud.update_article(slug, article_data, user_id)
        
        if not updated_article:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Article not found"
            )
        
        return SuccessResponse(
            message="Article updated successfully",
            data={
                "article": ArticleResponse.from_orm(updated_article).dict()
            }
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(e)
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@router.delete("/{slug}", response_model=SuccessResponse)
def delete_article(
    slug: str,
    user_id: UUID = Depends(get_user_id_from_token),
    db: Session = Depends(get_db)
):
    """Delete article by slug (only by author)"""
    try:
        crud = ArticleCRUD(db)
        deleted = crud.delete_article(slug, user_id)
        
        if not deleted:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Article not found"
            )
        
        return SuccessResponse(
            message="Article deleted successfully"
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(e)
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@router.post("/{slug}/publish", response_model=SuccessResponse)
def publish_article(
    slug: str,
    user_id: UUID = Depends(get_user_id_from_token),
    db: Session = Depends(get_db)
):
    """Request publication of an article (changes status from DRAFT to PENDING_PUBLISH)"""
    try:
        crud = ArticleCRUD(db)
        db_article = crud.request_publication(slug, user_id)
        
        if not db_article:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Article not found"
            )
        
        # Enqueue moderation task
        try:
            enqueue_moderation_task(
                post_id=str(db_article.id),
                author_id=str(db_article.author_id),
                title=db_article.title,
                body=db_article.body,
                requested_by=str(user_id)
            )
            logger.info("Moderation task enqueued for article %s", db_article.id)
        except Exception as task_exc:
            logger.error("Failed to enqueue moderation task: %s", task_exc)
            # Rollback status change if task enqueue fails
            db_article.status = "DRAFT"
            db.commit()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to start publication process"
            )
        
        return SuccessResponse(
            message="Article publication requested successfully",
            data={
                "article": ArticleResponse.from_orm(db_article).dict(),
                "status": db_article.status
            }
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error publishing article: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )
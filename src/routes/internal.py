"""Internal API endpoints for service-to-service communication"""
import logging
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from pydantic import BaseModel
from uuid import UUID
from src.models.database import get_db
from src.controllers.crud import ArticleCRUD
from src.middleware.api_key_auth import verify_api_key
from src.models.database import ApiKey
from src.models.schemas import SuccessResponse, ErrorResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/internal", tags=["internal"])


class RejectRequest(BaseModel):
    reason: str = "Moderation rejected"


class PreviewRequest(BaseModel):
    preview_url: str


class PublishRequest(BaseModel):
    pass  # No additional data needed


@router.post("/articles/{article_id}/reject", response_model=SuccessResponse)
def reject_article(
    article_id: UUID,
    request: RejectRequest,
    api_key: ApiKey = Depends(verify_api_key),
    db: Session = Depends(get_db)
):
    """Reject article (internal endpoint, requires API key)"""
    try:
        crud = ArticleCRUD(db)
        db_article = crud.update_article_status(article_id, "REJECTED")
        
        if not db_article:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Article not found"
            )
        
        logger.info("Article %s rejected by %s. Reason: %s", article_id, api_key.description, request.reason)
        
        return SuccessResponse(
            message="Article rejected successfully",
            data={
                "article_id": str(article_id),
                "status": "REJECTED",
                "reason": request.reason
            }
        )
    except Exception as e:
        logger.error("Error rejecting article %s: %s", article_id, e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@router.put("/articles/{article_id}/preview", response_model=SuccessResponse)
def set_article_preview(
    article_id: UUID,
    request: PreviewRequest,
    api_key: ApiKey = Depends(verify_api_key),
    db: Session = Depends(get_db)
):
    """Set article preview URL (internal endpoint, requires API key)"""
    try:
        crud = ArticleCRUD(db)
        db_article = crud.update_article_preview(article_id, request.preview_url)
        
        if not db_article:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Article not found"
            )
        
        logger.info("Preview set for article %s by %s", article_id, api_key.description)
        
        return SuccessResponse(
            message="Preview set successfully",
            data={
                "article_id": str(article_id),
                "preview_url": request.preview_url
            }
        )
    except Exception as e:
        logger.error("Error setting preview for article %s: %s", article_id, e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@router.post("/articles/{article_id}/publish", response_model=SuccessResponse)
def publish_article_internal(
    article_id: UUID,
    request: PublishRequest,
    api_key: ApiKey = Depends(verify_api_key),
    db: Session = Depends(get_db)
):
    """Publish article (internal endpoint, requires API key)"""
    try:
        crud = ArticleCRUD(db)
        db_article = crud.update_article_status(article_id, "PUBLISHED")
        
        if not db_article:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Article not found"
            )
        
        logger.info("Article %s published by %s", article_id, api_key.description)
        
        return SuccessResponse(
            message="Article published successfully",
            data={
                "article_id": str(article_id),
                "status": "PUBLISHED"
            }
        )
    except Exception as e:
        logger.error("Error publishing article %s: %s", article_id, e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@router.get("/articles/{article_id}", response_model=SuccessResponse)
def get_article_internal(
    article_id: UUID,
    api_key: ApiKey = Depends(verify_api_key),
    db: Session = Depends(get_db)
):
    """Get article by ID (internal endpoint, requires API key)"""
    try:
        crud = ArticleCRUD(db)
        db_article = crud.get_article_by_id(article_id)
        
        if not db_article:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Article not found"
            )
        
        from src.models.schemas import ArticleResponse
        return SuccessResponse(
            message="Article retrieved successfully",
            data={
                "article": ArticleResponse.from_orm(db_article).dict()
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error getting article %s: %s", article_id, e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


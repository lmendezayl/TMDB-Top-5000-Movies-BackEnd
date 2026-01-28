"""REST API Routes for Search with Typesense."""

from typing import Optional, List
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlmodel import Session

from core.auth import get_current_user
from infrastructure.database import get_session
from infrastructure.typesense_indexer import TypesenseIndexer
from domain.DomainEntities import MovieDomainEntity

router = APIRouter(prefix="/api/v1/search", tags=["search"])


@router.get(
    "/movies",
    response_model=List[dict],
    summary="Search movies",
    description="Full-text search on movies with optional filters",
)
async def search_movies(
    q: str = Query(..., min_length=1, description="Search query"),
    limit: int = Query(10, ge=1, le=100, description="Max results"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    min_popularity: Optional[float] = Query(
        None, ge=0, description="Minimum popularity"
    ),
    max_popularity: Optional[float] = Query(
        None, ge=0, description="Maximum popularity"
    ),
    min_vote: Optional[float] = Query(0, ge=0, le=10, description="Minimum vote average"),
    max_vote: Optional[float] = Query(
        10, ge=0, le=10, description="Maximum vote average"
    ),
    current_user: str = Depends(get_current_user),
):
    """
    Search for movies using Typesense vector database.

    **Requires Authentication:** Basic Auth

    **Query Parameters:**
    - `q`: Search query (required)
    - `limit`: Number of results (1-100, default: 10)
    - `offset`: Pagination offset (default: 0)
    - `min_popularity`: Minimum popularity score
    - `max_popularity`: Maximum popularity score
    - `min_vote`: Minimum vote average (0-10)
    - `max_vote`: Maximum vote average (0-10)

    **Returns:**
    - List of matching movies with scores and snippets

    **Example:**
    ```
    GET /api/v1/search/movies?q=avatar&limit=5&min_popularity=5
    ```
    """
    try:
        indexer = TypesenseIndexer()

        results = indexer.search_movies(
            query=q,
            limit=limit,
            offset=offset,
            min_popularity=min_popularity,
            max_popularity=max_popularity,
            min_vote_average=min_vote,
            max_vote_average=max_vote,
        )

        return results.get("hits", [])

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")


@router.get(
    "/stats",
    response_model=dict,
    summary="Search index statistics",
    description="Get information about the search index",
)
async def search_stats(current_user: str = Depends(get_current_user)):
    """
    Get search index statistics.

    **Requires Authentication:** Basic Auth

    **Returns:**
    - Document count in index
    - Collection name
    - Status information

    **Example:**
    ```
    GET /api/v1/search/stats
    ```
    """
    try:
        indexer = TypesenseIndexer()
        stats = indexer.get_stats()
        return stats

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stats retrieval failed: {str(e)}")

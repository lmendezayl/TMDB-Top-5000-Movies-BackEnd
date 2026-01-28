"""API Routes - Endpoints para CRUD de películas."""

from typing import List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlmodel import Session
from infrastructure.database import get_session
from infrastructure.repositories import MovieRepository
from domain import MovieDomainEntity

router = APIRouter(prefix="/api/v1/movies", tags=["movies"])


@router.get("/", response_model=List[MovieDomainEntity])
async def list_movies(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    session: Session = Depends(get_session)
) -> List[MovieDomainEntity]:
    """
    Obtiene lista de películas con paginación.
    
    - **skip**: Número de registros a saltar (default: 0)
    - **limit**: Número máximo de registros (default: 100, máx: 1000)
    """
    repository = MovieRepository(session)
    movies = repository.get_movies_with_details(skip=skip, limit=limit)
    return movies


@router.get("/{movie_id}", response_model=MovieDomainEntity)
async def get_movie(
    movie_id: int,
    session: Session = Depends(get_session)
) -> MovieDomainEntity:
    """Obtiene los detalles de una película por ID."""
    repository = MovieRepository(session)
    movie = repository.get_movie_with_details(movie_id)
    
    if not movie:
        raise HTTPException(status_code=404, detail="Película no encontrada")
    
    return movie


@router.get("/search/by-title", response_model=List[MovieDomainEntity])
async def search_movies_by_title(
    title: str = Query(..., min_length=1),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    session: Session = Depends(get_session)
) -> List[MovieDomainEntity]:
    """
    Busca películas por título (búsqueda parcial).
    
    - **title**: Palabra clave para búsqueda (required)
    - **skip**: Número de registros a saltar
    - **limit**: Número máximo de registros
    """
    repository = MovieRepository(session)
    movies = repository.search_movies_by_title(title, skip=skip, limit=limit)
    
    if not movies:
        raise HTTPException(status_code=404, detail="No se encontraron películas")
    
    return [repository.get_movie_with_details(m.id) for m in movies if m.id]


@router.get("/filter/by-genre", response_model=List[MovieDomainEntity])
async def filter_by_genre(
    genre_id: int = Query(..., gt=0),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    session: Session = Depends(get_session)
) -> List[MovieDomainEntity]:
    """
    Filtra películas por género.
    
    - **genre_id**: ID del género (required)
    - **skip**: Número de registros a saltar
    - **limit**: Número máximo de registros
    """
    repository = MovieRepository(session)
    movies = repository.filter_by_genre(genre_id, skip=skip, limit=limit)
    
    if not movies:
        raise HTTPException(status_code=404, detail="No se encontraron películas con ese género")
    
    return movies


@router.get("/filter/by-popularity", response_model=List[MovieDomainEntity])
async def filter_by_popularity(
    min_popularity: float = Query(0.0, ge=0.0),
    max_popularity: float = Query(1000.0, ge=0.0),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    session: Session = Depends(get_session)
) -> List[MovieDomainEntity]:
    """
    Filtra películas por rango de popularidad.
    
    - **min_popularity**: Popularidad mínima (default: 0.0)
    - **max_popularity**: Popularidad máxima (default: 1000.0)
    - **skip**: Número de registros a saltar
    - **limit**: Número máximo de registros
    """
    if min_popularity > max_popularity:
        raise HTTPException(
            status_code=400,
            detail="min_popularity debe ser menor o igual a max_popularity"
        )
    
    repository = MovieRepository(session)
    movies = repository.filter_by_popularity(
        min_popularity=min_popularity,
        max_popularity=max_popularity,
        skip=skip,
        limit=limit
    )
    
    if not movies:
        raise HTTPException(status_code=404, detail="No se encontraron películas en ese rango")
    
    return movies


@router.get("/filter/by-vote-average", response_model=List[MovieDomainEntity])
async def filter_by_vote_average(
    min_vote: float = Query(0.0, ge=0.0, le=10.0),
    max_vote: float = Query(10.0, ge=0.0, le=10.0),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    session: Session = Depends(get_session)
) -> List[MovieDomainEntity]:
    """
    Filtra películas por rango de calificación promedio (0-10).
    
    - **min_vote**: Calificación mínima (default: 0.0)
    - **max_vote**: Calificación máxima (default: 10.0)
    - **skip**: Número de registros a saltar
    - **limit**: Número máximo de registros
    """
    if min_vote > max_vote:
        raise HTTPException(
            status_code=400,
            detail="min_vote debe ser menor o igual a max_vote"
        )
    
    repository = MovieRepository(session)
    movies = repository.filter_by_vote_average(
        min_vote=min_vote,
        max_vote=max_vote,
        skip=skip,
        limit=limit
    )
    
    if not movies:
        raise HTTPException(status_code=404, detail="No se encontraron películas en ese rango de calificación")
    
    return movies

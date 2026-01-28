"""Movie Repository - Acceso a datos de películas y hechos relacionados."""

from typing import List, Optional
from sqlmodel import Session, select
from infrastructure.models import DimMovie, FactMovieRelease, BridgeMovieGenre, DimGenre
from infrastructure.repositories.base_repository import BaseRepository
from infrastructure.mappers import DomainMapper
from domain import MovieDomainEntity, GenreDomainEntity


class MovieRepository(BaseRepository[DimMovie]):
    """Repositorio para operaciones con películas."""
    
    def __init__(self, session: Session) -> None:
        super().__init__(session, DimMovie)
    
    def get_movie_by_title(self, title: str) -> Optional[DimMovie]:
        """Busca una película por título."""
        statement = select(DimMovie).where(DimMovie.title == title)
        return self.session.exec(statement).first()
    
    def search_movies_by_title(self, title: str, skip: int = 0, limit: int = 100) -> List[DimMovie]:
        """Busca películas por título parcial (LIKE)."""
        search_term = f"%{title}%"
        statement = select(DimMovie).where(DimMovie.title.ilike(search_term)).offset(skip).limit(limit)
        return self.session.exec(statement).all()
    
    def get_genres_for_movie(self, movie_id: int) -> List[GenreDomainEntity]:
        """Obtiene los géneros de una película."""
        statement = (
            select(DimGenre)
            .join(BridgeMovieGenre)
            .where(BridgeMovieGenre.movie_id == movie_id)
        )
        genres = self.session.exec(statement).all()
        return [DomainMapper.to_genre_domain(g) for g in genres]
    
    def get_movie_fact(self, movie_id: int) -> Optional[FactMovieRelease]:
        """Obtiene el hecho (FactMovieRelease) de una película."""
        statement = select(FactMovieRelease).where(FactMovieRelease.movie_info_id == movie_id)
        return self.session.exec(statement).first()
    
    def get_movie_with_details(self, movie_id: int) -> Optional[MovieDomainEntity]:
        """
        Obtiene una película con todos sus detalles completos.
        Combina DimMovie + FactMovieRelease + Géneros.
        """
        movie = self.get_by_id(movie_id)
        if not movie:
            return None
        
        fact = self.get_movie_fact(movie_id)
        if not fact:
            return None
        
        genres = self.get_genres_for_movie(movie_id)
        
        return DomainMapper.to_movie_domain(
            movie=movie,
            fact=fact,
            genres=genres
        )
    
    def get_movies_with_details(self, skip: int = 0, limit: int = 100) -> List[MovieDomainEntity]:
        """Obtiene múltiples películas con todos sus detalles."""
        movies = self.get_all(skip=skip, limit=limit)
        return [self.get_movie_with_details(m.id) for m in movies if m.id]
    
    def filter_by_genre(self, genre_id: int, skip: int = 0, limit: int = 100) -> List[MovieDomainEntity]:
        """Filtra películas por género."""
        statement = (
            select(DimMovie)
            .join(BridgeMovieGenre)
            .where(BridgeMovieGenre.genre_id == genre_id)
            .offset(skip)
            .limit(limit)
        )
        movies = self.session.exec(statement).all()
        return [self.get_movie_with_details(m.id) for m in movies if m.id]
    
    def filter_by_popularity(
        self,
        min_popularity: float,
        max_popularity: float,
        skip: int = 0,
        limit: int = 100
    ) -> List[MovieDomainEntity]:
        """Filtra películas por rango de popularidad."""
        statement = (
            select(DimMovie)
            .join(FactMovieRelease)
            .where(
                (FactMovieRelease.popularity >= min_popularity) &
                (FactMovieRelease.popularity <= max_popularity)
            )
            .offset(skip)
            .limit(limit)
        )
        movies = self.session.exec(statement).all()
        return [self.get_movie_with_details(m.id) for m in movies if m.id]
    
    def filter_by_vote_average(
        self,
        min_vote: float,
        max_vote: float,
        skip: int = 0,
        limit: int = 100
    ) -> List[MovieDomainEntity]:
        """Filtra películas por calificación promedio."""
        statement = (
            select(DimMovie)
            .join(FactMovieRelease)
            .where(
                (FactMovieRelease.vote_average >= min_vote) &
                (FactMovieRelease.vote_average <= max_vote)
            )
            .offset(skip)
            .limit(limit)
        )
        movies = self.session.exec(statement).all()
        return [self.get_movie_with_details(m.id) for m in movies if m.id]

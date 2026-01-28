"""Repositories - Capa de acceso a datos con patrón Repository."""

from .base_repository import BaseRepository
from .movie_repository import MovieRepository

__all__ = [
    "BaseRepository",
    "MovieRepository",
]

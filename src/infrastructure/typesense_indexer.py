"""Typesense Indexer - Indexación de datos en Typesense."""

import logging
import math
from typing import List, Optional
import pandas as pd
from sqlmodel import Session, select

from infrastructure.typesense_client import TypesenseClient
from infrastructure.models import (
    DimMovie,
    DimGenre,
    FactMovieRelease,
    BridgeMovieGenre,
    DimDate,
)

logger = logging.getLogger(__name__)


class TypesenseIndexer:
    """Indexador de datos para Typesense."""

    MOVIES_COLLECTION = "movies"
    MOVIES_SCHEMA = {
        "name": "movies",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "title", "type": "string", "infix": True},
            {"name": "original_title", "type": "string"},
            {"name": "overview", "type": "string"},
            {"name": "release_date", "type": "string"},
            {"name": "runtime", "type": "int32"},
            {"name": "popularity", "type": "float"},
            {"name": "vote_average", "type": "float"},
            {"name": "vote_count", "type": "int32"},
            {"name": "budget", "type": "int64"},
            {"name": "revenue", "type": "int64"},
            {"name": "original_language", "type": "string"},
            {"name": "genres", "type": "string[]"},
            {"name": "directors", "type": "string[]"},
            {"name": "production_companies", "type": "string[]"},
        ],
        "default_sorting_field": "popularity",
    }

    def __init__(self, client: Optional[TypesenseClient] = None):
        """
        Inicializa el indexador.

        Args:
            client: Cliente Typesense (default: crear uno nuevo)
        """
        self.client = client or TypesenseClient()

    def ensure_collection_exists(self) -> bool:
        """
        Verifica que la colección de películas existe, si no, la crea.

        Returns:
            True si la colección existe o fue creada
        """
        try:
            if self.client.collection_exists(self.MOVIES_COLLECTION):
                logger.info(f"Collection '{self.MOVIES_COLLECTION}' already exists")
                return True

            logger.info(f"Creating collection '{self.MOVIES_COLLECTION}'...")
            self.client.create_collection(self.MOVIES_COLLECTION, self.MOVIES_SCHEMA)
            return True
        except Exception as e:
            logger.error(f"Error ensuring collection exists: {e}")
            return False

    def recreate_collection(self) -> bool:
        """
        Elimina y recrea la colección (para reseteo completo).

        Returns:
            True si se recreó exitosamente
        """
        try:
            logger.info(f"Recreating collection '{self.MOVIES_COLLECTION}'...")
            self.client.delete_collection(self.MOVIES_COLLECTION)
            logger.info(f"Deleted collection '{self.MOVIES_COLLECTION}'")

            self.client.create_collection(self.MOVIES_COLLECTION, self.MOVIES_SCHEMA)
            logger.info(f"Recreated collection '{self.MOVIES_COLLECTION}'")
            return True
        except Exception as e:
            logger.error(f"Error recreating collection: {e}")
            return False

    def index_movies(self, session: Session) -> int:
        """
        Indexa todas las películas de la base de datos en Typesense.

        Args:
            session: Sesión SQLModel

        Returns:
            Número de películas indexadas
        """
        try:
            logger.info("Starting indexing of movies...")
            self.ensure_collection_exists()
            results = session.exec(
                select(DimMovie, FactMovieRelease, DimDate)
                .join(FactMovieRelease, DimMovie.id == FactMovieRelease.movie_info_id)
                .join(DimDate, FactMovieRelease.release_date_id == DimDate.id, isouter=True)
            ).all()
            
            logger.info(f"Found {len(results)} movies to index")

            documents = []
            for movie, fact, date in results:
                genres = self._get_movie_genres(session, movie.id)
                release_date_str = ""
                if date and date.date:
                    try:
                        release_date_str = date.date.strftime("%Y-%m-%d")
                    except Exception:
                        release_date_str = str(date.date)

                popularity = float(fact.popularity) if fact.popularity else 0.0
                if math.isnan(popularity):
                    popularity = 0.0
                
                vote_average = float(fact.vote_average) if fact.vote_average else 0.0
                if math.isnan(vote_average):
                    vote_average = 0.0

                doc = {
                    "id": str(movie.id),
                    "title": str(movie.title) if movie.title else "Untitled",
                    "original_title": str(movie.title),
                    "overview": str(movie.overview) if movie.overview else "",
                    "release_date": release_date_str,
                    "runtime": int(fact.runtime) if fact.runtime else (int(movie.runtime) if movie.runtime else 0),
                    "popularity": popularity,
                    "vote_average": vote_average,
                    "vote_count": 0,
                    "budget": int(fact.budget) if fact.budget else 0,
                    "revenue": int(fact.revenue) if fact.revenue else 0,
                    "original_language": str(movie.original_language) if movie.original_language else "en",
                    "genres": [str(g) for g in genres],
                    "directors": [],
                    "production_companies": [],
                }
                documents.append(doc)

            batch_size = 500
            indexed_count = 0
            for i in range(0, len(documents), batch_size):
                batch = documents[i : i + batch_size]
                self.client.upsert_documents(self.MOVIES_COLLECTION, batch)
                indexed_count += len(batch)
                logger.info(f"Indexed {indexed_count}/{len(documents)} movies")

            logger.info(f"Finished indexing {indexed_count} movies")
            return indexed_count

        except Exception as e:
            logger.error(f"Error indexing movies: {e}")
            raise

    def index_movie(self, session: Session, movie_id: int) -> bool:
        """
        Indexa una película individual.

        Args:
            session: Sesión SQLModel
            movie_id: ID de la película

        Returns:
            True si se indexó exitosamente
        """
        try:
            result = session.exec(
                select(DimMovie, FactMovieRelease, DimDate)
                .join(FactMovieRelease, DimMovie.id == FactMovieRelease.movie_info_id)
                .join(DimDate, FactMovieRelease.release_date_id == DimDate.id, isouter=True)
                .where(DimMovie.id == movie_id)
            ).first()

            if not result:
                logger.warning(f"Movie {movie_id} not found in Fact table")
                return False
            
            movie, fact, date = result

            genres = self._get_movie_genres(session, movie.id)
            
            release_date_str = ""
            if date and date.date:
                try:
                    release_date_str = date.date.strftime("%Y-%m-%d")
                except Exception:
                    release_date_str = str(date.date)

            popularity = float(fact.popularity) if fact.popularity else 0.0
            if math.isnan(popularity):
                popularity = 0.0
            
            vote_average = float(fact.vote_average) if fact.vote_average else 0.0
            if math.isnan(vote_average):
                vote_average = 0.0

            doc = {
                "id": str(movie.id),
                "title": str(movie.title) if movie.title else "Untitled",
                "original_title": str(movie.title),
                "overview": str(movie.overview) if movie.overview else "",
                "release_date": release_date_str,
                "runtime": int(fact.runtime) if fact.runtime else (int(movie.runtime) if movie.runtime else 0),
                "popularity": popularity,
                "vote_average": vote_average,
                "vote_count": 0,
                "budget": int(fact.budget) if fact.budget else 0,
                "revenue": int(fact.revenue) if fact.revenue else 0,
                "original_language": str(movie.original_language) if movie.original_language else "en",
                "genres": [str(g) for g in genres],
                "directors": [],
                "production_companies": [],
            }

            self.client.upsert_documents(self.MOVIES_COLLECTION, [doc])
            logger.info(f"Indexed movie {movie_id}")
            return True

        except Exception as e:
            logger.error(f"Error indexing movie {movie_id}: {e}")
            return False

    def _get_movie_genres(self, session: Session, movie_id: int) -> List[str]:
        """
        Obtiene los géneros de una película.

        Args:
            session: Sesión SQLModel
            movie_id: ID de la película

        Returns:
            Lista de nombres de géneros
        """
        try:
            # Join: Bridge -> DimGenre
            results = session.exec(
                select(DimGenre)
                .join(BridgeMovieGenre, DimGenre.id == BridgeMovieGenre.genre_id)
                .where(BridgeMovieGenre.movie_id == movie_id)
            ).all()

            return [genre.name for genre in results]
        except Exception as e:
            logger.error(f"Error getting genres for movie {movie_id}: {e}")
            return []

    def search_movies(
        self,
        query: str,
        limit: int = 10,
        offset: int = 0,
        min_popularity: Optional[float] = None,
        max_popularity: Optional[float] = None,
        min_vote_average: Optional[float] = None,
        max_vote_average: Optional[float] = None,
    ) -> dict:
        """
        Busca películas en Typesense.

        Args:
            query: Texto a buscar
            limit: Número máximo de resultados
            offset: Desplazamiento para paginación
            min_popularity: Popularidad mínima (opcional)
            max_popularity: Popularidad máxima (opcional)
            min_vote_average: Votación mínima (opcional)
            max_vote_average: Votación máxima (opcional)

        Returns:
            Resultados de búsqueda
        """
        try:
            filters = []

            if min_popularity is not None or max_popularity is not None:
                min_pop = min_popularity or 0
                max_pop = max_popularity or float("inf")
                filters.append(f"popularity:[{min_pop}..{max_pop}]")

            if min_vote_average is not None or max_vote_average is not None:
                min_vote = min_vote_average or 0
                max_vote = max_vote_average or 10
                filters.append(f"vote_average:[{min_vote}..{max_vote}]")

            filter_by = " && ".join(filters) if filters else None

            results = self.client.search(
                collection=self.MOVIES_COLLECTION,
                query_text=query,
                search_field="title",
                limit=limit,
                offset=offset,
                filter_by=filter_by,
            )

            return results

        except Exception as e:
            logger.error(f"Error searching movies: {e}")
            raise

    def get_stats(self) -> dict:
        """
        Obtiene estadísticas de la colección.

        Returns:
            Diccionario con estadísticas
        """
        try:
            count = self.client.count_documents(self.MOVIES_COLLECTION)
            return {
                "collection": self.MOVIES_COLLECTION,
                "document_count": count,
            }
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {}

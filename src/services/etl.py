"""ETL Pipeline - Transformación de datos: Bronze → Silver → Gold."""

import json
import logging
from pathlib import Path
from typing import Tuple
from datetime import datetime, timedelta

import pandas as pd
from sqlmodel import Session, func, select

from infrastructure.database import create_db_and_tables, engine
from infrastructure.models import (
    DimMovie,
    DimDate,
    DimGenre,
    DimDirector,
    DimProductionCompany,
    DimLanguage,
    DimCountry,
    BridgeMovieGenre,
    FactMovieRelease,
)

logger = logging.getLogger(__name__)


class ETLPipeline:
    def __init__(
        self,
        bronze_path: str = "data/bronze",
        silver_path: str = "data/silver",
        gold_path: str = "data/gold"
    ):
        base_dir = Path(__file__).resolve().parent.parent.parent
        
        self.bronze_path = base_dir / bronze_path
        self.silver_path = base_dir / silver_path
        self.gold_path = base_dir / gold_path
        
        self.silver_path.mkdir(parents=True, exist_ok=True)
        self.gold_path.mkdir(parents=True, exist_ok=True)
    
    # ==================== BRONZE → SILVER ====================
    
    def read_bronze(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Lee archivos CSV de la capa Bronze."""
        logger.info("Leyendo datos Bronze...")
        
        movies_df = pd.read_csv(self.bronze_path / "tmdb_5000_movies.csv")
        credits_df = pd.read_csv(self.bronze_path / "tmdb_5000_credits.csv")
        
        logger.info(f"Películas leídas: {len(movies_df)} registros")
        logger.info(f"Créditos leídos: {len(credits_df)} registros")
        
        return movies_df, credits_df
    
    def clean_movies(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpia y estandariza datos de películas."""
        logger.info("Limpiando datos de películas...")
        
        df = df.copy()
        df = df.drop_duplicates(subset=['id'])
        logger.info(f"Duplicados eliminados. Registros: {len(df)}")
        
        df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
        df['popularity'] = pd.to_numeric(df['popularity'], errors='coerce').fillna(0.0)
        df['vote_average'] = pd.to_numeric(df['vote_average'], errors='coerce').fillna(0.0)
        df['vote_count'] = pd.to_numeric(df['vote_count'], errors='coerce').fillna(0).astype(int)
        df['budget'] = pd.to_numeric(df['budget'], errors='coerce').fillna(0).astype(int)
        df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce').fillna(0).astype(int)
        df['runtime'] = pd.to_numeric(df['runtime'], errors='coerce').fillna(0).astype(int)
        
        df['overview'] = df['overview'].fillna('')
        df['tagline'] = df['tagline'].fillna('')
        df['original_language'] = df['original_language'].fillna('en')
        
        logger.info("Conversiones de tipos completadas")
        
        return df
    
    def clean_credits(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpia datos de créditos."""
        logger.info("Limpiando datos de créditos...")
        
        df = df.copy()
        df = df.drop_duplicates(subset=['movie_id'])
        
        logger.info(f"Duplicados eliminados. Registros: {len(df)}")
        
        return df
    
    def save_silver(self, movies_df: pd.DataFrame, credits_df: pd.DataFrame) -> None:
        """Guarda DataFrames limpios en Parquet (Silver Layer)."""
        logger.info("Guardando Silver Layer (Parquet)...")
        
        movies_parquet = self.silver_path / "movies_silver.parquet"
        credits_parquet = self.silver_path / "credits_silver.parquet"
        
        movies_df.to_parquet(movies_parquet, index=False, compression='snappy')
        logger.info(f"  ✓ {movies_parquet} ({len(movies_df)} registros)")
        
        credits_df.to_parquet(credits_parquet, index=False, compression='snappy')
        logger.info(f"  ✓ {credits_parquet} ({len(credits_df)} registros)")
    
    # ==================== SILVER → GOLD ====================
    
    # Optional Feature #1
    def build_dim_date(self, movies_df: pd.DataFrame) -> pd.DataFrame:
        """Construye dimensión Date a partir de fechas de lanzamiento."""
        logger.info("Construyendo Dim Date...")

        min_date = movies_df['release_date'].min()
        max_date = movies_df['release_date'].max()

        if pd.isna(min_date):
            min_date = pd.Timestamp('1900-01-01')
        if pd.isna(max_date):
            max_date = pd.Timestamp('2026-01-25')
        
        date_range = pd.date_range(start=min_date, end=max_date, freq='D')
        
        dim_date = pd.DataFrame({
            'id': range(1, len(date_range) + 1),
            'date': date_range,
            'year': date_range.year,
            'month': date_range.month,
            'month_name': date_range.strftime('%B'),
            'day': date_range.day,
            'day_of_week': date_range.dayofweek,
            'day_of_week_name': date_range.day_name(),
            'quarter': date_range.quarter,
            'week': date_range.isocalendar().week,
            'is_weekend': date_range.dayofweek.isin([5, 6]),
        })
        
        logger.info(f"  ✓ Dim Date creada: {len(dim_date)} registros")
        
        return dim_date
    
    def build_dim_movie(self, movies_df: pd.DataFrame) -> pd.DataFrame:
        """Construye dimensión Movie."""
        logger.info("Construyendo Dim Movie...")
        
        dim_movie = movies_df[[
            'id', 'title', 'original_language', 'overview', 'runtime',
            'tagline', 'status'
        ]].copy()
        
        dim_movie = dim_movie.reset_index(drop=True)
        
        dim_movie = dim_movie.rename(columns={'id': 'original_id'})
        dim_movie.insert(0, 'id', range(1, len(dim_movie) + 1))
        
        logger.info(f"  ✓ Dim Movie creada: {len(dim_movie)} registros")
        
        return dim_movie
    
    def build_dim_genre(self, movies_df: pd.DataFrame) -> pd.DataFrame:
        """Construye dimensión Genre."""
        logger.info("Construyendo Dim Genre...")
        
        genres_list = []
        genre_ids_seen = set()
        
        for genres_json in movies_df['genres']:
            try:
                if isinstance(genres_json, str):
                    genres = json.loads(genres_json.replace("'", '"'))
                else:
                    genres = genres_json
                
                for genre in genres:
                    genre_id = genre.get('id')
                    if genre_id and genre_id not in genre_ids_seen:
                        genres_list.append({
                            'id': genre_id,
                            'name': genre.get('name', '')
                        })
                        genre_ids_seen.add(genre_id)
            except Exception as e:
                logger.warning(f"  ⚠ Error procesando género: {e}")
                continue
        
        dim_genre = pd.DataFrame(genres_list)
        
        logger.info(f"  ✓ Dim Genre creada: {len(dim_genre)} registros")
        
        return dim_genre
    
    def build_dim_director(self, credits_df: pd.DataFrame) -> pd.DataFrame:
        """Construye dimensión Director."""
        logger.info("Construyendo Dim Director...")
        
        directors_list = []
        director_ids_seen = set()
        
        for crew_json in credits_df['crew']:
            try:
                if isinstance(crew_json, str):
                    crew = json.loads(crew_json.replace("'", '"'))
                else:
                    crew = crew_json
                
                for person in crew:
                    if person.get('job') == 'Director':
                        director_id = person.get('id')
                        if director_id and director_id not in director_ids_seen:
                            directors_list.append({
                                'id': director_id,
                                'name': person.get('name', ''),
                                'profile_path': person.get('profile_path')
                            })
                            director_ids_seen.add(director_id)
            except Exception:
                continue
        
        dim_director = pd.DataFrame(directors_list)
        
        logger.info(f"  ✓ Dim Director creada: {len(dim_director)} registros")
        
        return dim_director
    
    def build_dim_production_company(self, movies_df: pd.DataFrame) -> pd.DataFrame:
        """Construye dimensión Production Company."""
        logger.info("Construyendo Dim Production Company...")
        
        companies_list = []
        company_ids_seen = set()
        
        for comp_json in movies_df['production_companies']:
            try:
                if isinstance(comp_json, str):
                    companies = json.loads(comp_json.replace("'", '"'))
                else:
                    companies = comp_json
                
                for company in companies:
                    company_id = company.get('id')
                    if company_id and company_id not in company_ids_seen:
                        companies_list.append({
                            'id': company_id,
                            'name': company.get('name', ''),
                            'origin_country': company.get('origin_country')
                        })
                        company_ids_seen.add(company_id)
            except Exception as e:
                continue
        
        dim_company = pd.DataFrame(companies_list)
        
        logger.info(f"  ✓ Dim Production Company creada: {len(dim_company)} registros")
        
        return dim_company
    
    def build_dim_language(self, movies_df: pd.DataFrame) -> pd.DataFrame:
        """Construye dimensión Language."""
        logger.info("Construyendo Dim Language...")
        
        languages_list = []
        language_codes_seen = set()
        
        for lang_json in movies_df['spoken_languages']:
            try:
                if isinstance(lang_json, str):
                    languages = json.loads(lang_json.replace("'", '"'))
                else:
                    languages = lang_json
                
                for lang in languages:
                    iso_code = lang.get('iso_639_1')
                    if iso_code and iso_code not in language_codes_seen:
                        languages_list.append({
                            'iso_639_1': iso_code,
                            'language_name': lang.get('name', '')
                        })
                        language_codes_seen.add(iso_code)
            except Exception :
                logger.warning(f"  ⚠ Error procesando idioma: {e}")
                continue
        
        dim_language = pd.DataFrame(languages_list)
        dim_language.insert(0, 'id', range(1, len(dim_language) + 1))
        
        logger.info(f"  ✓ Dim Language creada: {len(dim_language)} registros")
        
        return dim_language
    
    def build_dim_country(self, movies_df: pd.DataFrame) -> pd.DataFrame:
        """Construye dimensión Country."""
        logger.info("Construyendo Dim Country...")
        
        countries_list = []
        country_codes_seen = set()
        
        for country_json in movies_df['production_countries']:
            try:
                if isinstance(country_json, str):
                    countries = json.loads(country_json.replace("'", '"'))
                else:
                    countries = country_json
                
                for country in countries:
                    iso_code = country.get('iso_3166_1')
                    if iso_code and iso_code not in country_codes_seen:
                        countries_list.append({
                            'iso_3166_1': iso_code,
                            'country_name': country.get('name', '')
                        })
                        country_codes_seen.add(iso_code)
            except Exception as e:
                logger.warning(f"  ⚠ Error procesando país: {e}")
                continue
        
        dim_country = pd.DataFrame(countries_list)
        dim_country.insert(0, 'id', range(1, len(dim_country) + 1))
        
        logger.info(f"  ✓ Dim Country creada: {len(dim_country)} registros")
        
        return dim_country
    
    def build_bridge_movie_genre(
        self,
        movies_df: pd.DataFrame,
        dim_movie: pd.DataFrame,
        dim_genre: pd.DataFrame
    ) -> pd.DataFrame:
        """Construye tabla puente Movie-Genre."""
        logger.info("Construyendo Bridge Movie-Genre...")
        
        original_to_new_id = dict(zip(dim_movie['original_id'], dim_movie['id']))
        genre_name_to_id = dict(zip(dim_genre['name'], dim_genre['id']))
        bridge_list = []
        
        for idx, genres_json in enumerate(movies_df['genres']):
            try:
                original_id = movies_df.iloc[idx]['id']
                new_movie_id = original_to_new_id.get(original_id)
                
                if isinstance(genres_json, str):
                    genres = json.loads(genres_json.replace("'", '"'))
                else:
                    genres = genres_json
                
                for genre in genres:
                    genre_id = genre.get('id')
                    if genre_id in dim_genre['id'].values:
                        bridge_list.append({
                            'movie_id': new_movie_id,
                            'genre_id': genre_id
                        })
            except Exception as e:
                logger.warning(f"  ⚠ Error en bridge: {e}")
                continue
        
        bridge_df = pd.DataFrame(bridge_list)
        bridge_df.insert(0, 'id', range(1, len(bridge_df) + 1))
        logger.info(f"  ✓ Bridge Movie-Genre creada: {len(bridge_df)} registros")
        
        return bridge_df
    
    def build_fact_movie_release(
        self,
        movies_df: pd.DataFrame,
        credits_df: pd.DataFrame,
        dim_movie: pd.DataFrame,
        dim_date: pd.DataFrame,
        dim_director: pd.DataFrame,
        dim_language: pd.DataFrame,
        dim_production_company: pd.DataFrame,
        dim_country: pd.DataFrame
    ) -> pd.DataFrame:
        """Construye tabla de hechos Fact Movie Release."""
        logger.info("Construyendo Fact Movie Release...")
        
        movies_df = movies_df.reset_index(drop=True)
        
        original_to_new_id = dict(zip(movies_df['id'], range(1, len(movies_df) + 1)))
        date_to_id = dict(zip(dim_date['date'].dt.date, dim_date['id']))
        
        movie_to_director = {}
        movie_to_language = {}
        movie_to_company = {}
        movie_to_country = {}
        
        for _, credit_row in credits_df.iterrows():
            movie_id = credit_row['movie_id']
            try:
                crew_json = credit_row['crew']
                if isinstance(crew_json, str):
                    crew = json.loads(crew_json.replace("'", '"'))
                else:
                    crew = crew_json
                
                for person in crew:
                    if person.get('job') == 'Director':
                        director_id = person.get('id')
                        if director_id in dim_director['id'].values:
                            movie_to_director[movie_id] = director_id
                            break
            except Exception:
                pass
        
        for _, movie_row in movies_df.iterrows():
            movie_id = movie_row['id']
            try:
                lang_json = movie_row['spoken_languages']
                if isinstance(lang_json, str):
                    languages = json.loads(lang_json.replace("'", '"'))
                else:
                    languages = lang_json
                
                if languages and len(languages) > 0:
                    iso_code = languages[0].get('iso_639_1')
                    lang_match = dim_language[dim_language['iso_639_1'] == iso_code]
                    if not lang_match.empty:
                        movie_to_language[movie_id] = lang_match['id'].values[0]
            except Exception:
                pass
        
        for _, movie_row in movies_df.iterrows():
            movie_id = movie_row['id']
            try:
                comp_json = movie_row['production_companies']
                if isinstance(comp_json, str):
                    companies = json.loads(comp_json.replace("'", '"'))
                else:
                    companies = comp_json
                
                if companies and len(companies) > 0:
                    company_id = companies[0].get('id')
                    if company_id in dim_production_company['id'].values:
                        movie_to_company[movie_id] = company_id
            except Exception:
                pass
        
        for _, movie_row in movies_df.iterrows():
            movie_id = movie_row['id']
            try:
                country_json = movie_row['production_countries']
                if isinstance(country_json, str):
                    countries = json.loads(country_json.replace("'", '"'))
                else:
                    countries = country_json
                
                if countries and len(countries) > 0:
                    iso_code = countries[0].get('iso_3166_1')
                    country_match = dim_country[dim_country['iso_3166_1'] == iso_code]
                    if not country_match.empty:
                        movie_to_country[movie_id] = country_match['id'].values[0]
            except Exception:
                pass
        
        fact_list = []
        
        for idx, (i, row) in enumerate(movies_df.iterrows()):
            try:
                original_id = row['id']
                movie_id = idx + 1
                
                release_date = pd.to_datetime(row['release_date'], errors='coerce')
                date_id = 1
                
                if pd.notna(release_date):
                    date_id = date_to_id.get(release_date.date(), 1)
                
                fact_list.append({
                    'movie_info_id': movie_id,
                    'release_date_id': date_id,
                    'director_id': movie_to_director.get(original_id),
                    'language_id': movie_to_language.get(original_id),
                    'company_id': movie_to_company.get(original_id),
                    'country_id': movie_to_country.get(original_id),
                    'date_id': date_id,
                    'budget': int(row['budget']),
                    'revenue': int(row['revenue']),
                    'popularity': float(row['popularity']),
                    'vote_average': float(row['vote_average']),
                    'runtime': int(row['runtime']) if pd.notna(row['runtime']) else None,
                })
            except Exception as e:
                logger.warning(f"  ⚠ Error en hecho: {e}")
                continue
        
        fact_df = pd.DataFrame(fact_list)
        fact_df.insert(0, 'id', range(1, len(fact_df) + 1))
        
        logger.info(f"  ✓ Fact Movie Release creada: {len(fact_df)} registros")
        
        return fact_df
    
    def save_gold(
        self,
        dims: dict,
        bridge_df: pd.DataFrame,
        fact_df: pd.DataFrame
    ) -> None:
        """Guarda dimensiones, puente y hechos en Parquet (Gold Layer)."""
        logger.info("Guardando Gold Layer (Parquet)...")
        
        for name, df in dims.items():
            parquet_file = self.gold_path / f"{name}.parquet"
            df.to_parquet(parquet_file, index=False, compression='snappy')
            logger.info(f"  ✓ {parquet_file} ({len(df)} registros)")
        
        bridge_file = self.gold_path / "bridge_movie_genre.parquet"
        bridge_df.to_parquet(bridge_file, index=False, compression='snappy')
        logger.info(f"  ✓ {bridge_file} ({len(bridge_df)} registros)")
        
        fact_file = self.gold_path / "fact_movie_release.parquet"
        fact_df.to_parquet(fact_file, index=False, compression='snappy')
        logger.info(f"  ✓ {fact_file} ({len(fact_df)} registros)")
    
    def load_gold_to_database(self) -> None:
        """Carga datos de Gold (Parquet) a la BD SQLite."""
        logger.info("Cargando Gold Layer a SQLite...")
        
        from sqlmodel import Session, delete
        
        with Session(engine) as session:

            existing_movies = session.exec(select(func.count(DimMovie.id))).one()
        
            if existing_movies > 0:
                logger.info(f"⚠ La BD ya contiene {existing_movies} películas. Saltando carga.")
                logger.info("✓ Base de datos ya poblada, no se cargarán datos duplicados")
                return
        
            logger.info("✓ BD vacía, iniciando carga de datos...\n")
            
            try:
                dim_movie_df = pd.read_parquet(self.gold_path / "dim_movie.parquet")
                for _, row in dim_movie_df.iterrows():
                    obj = DimMovie(**row.to_dict())
                    session.add(obj)
                session.commit()
                logger.info(f"  ✓ DimMovie cargada: {len(dim_movie_df)} registros")
            except Exception as e:
                logger.error(f"  ✗ Error cargando DimMovie: {e}")
            
            try:
                dim_date_df = pd.read_parquet(self.gold_path / "dim_date.parquet")
                for _, row in dim_date_df.iterrows():
                    obj = DimDate(**row.to_dict())
                    session.add(obj)
                session.commit()
                logger.info(f"  ✓ DimDate cargada: {len(dim_date_df)} registros")
            except Exception as e:
                logger.error(f"  ✗ Error cargando DimDate: {e}")
                
            try:
                dim_country_df = pd.read_parquet(self.gold_path / "dim_country.parquet")
                for _, row in dim_country_df.iterrows():
                    obj = DimCountry(**row.to_dict())
                    session.add(obj)
                session.commit()
                logger.info(f"  ✓ DimCountry cargada: {len(dim_country_df)} registros")
            except Exception as e:
                logger.error(f"  ✗ Error cargando DimCountry: {e}")
            
            try: 
                dim_genre_df = pd.read_parquet(self.gold_path / "dim_genre.parquet")
                for _, row in dim_genre_df.iterrows():
                    obj = DimGenre(**row.to_dict())
                    session.add(obj)
                session.commit()
                logger.info(f"  ✓ DimGenre cargada: {len(dim_genre_df)} registros")
            except Exception as e:
                logger.error(f"  ✗ Error cargando DimGenre: {e}")
            
            try: 
                dim_director_df = pd.read_parquet(self.gold_path / "dim_director.parquet")
                for _, row in dim_director_df.iterrows():
                    obj = DimDirector(**row.to_dict())
                    session.add(obj)
                session.commit()
                logger.info(f"  ✓ DimDirector cargada: {len(dim_director_df)} registros")
            except Exception as e:  
                logger.error(f"  ✗ Error cargando DimDirector: {e}")
                
            try:
                dim_language_df = pd.read_parquet(self.gold_path / "dim_language.parquet")
                for _, row in dim_language_df.iterrows():
                    obj = DimLanguage(**row.to_dict())
                    session.add(obj)
                session.commit()
                logger.info(f"  ✓ DimLanguage cargada: {len(dim_language_df)} registros")
            except Exception as e:
                logger.error(f"  ✗ Error cargando DimLanguage: {e}")

            try:
                dim_production_company_df = pd.read_parquet(self.gold_path / "dim_production_company.parquet")
                for _, row in dim_production_company_df.iterrows():
                    obj = DimProductionCompany(**row.to_dict())
                    session.add(obj)
                session.commit()
                logger.info(f"  ✓ DimProductionCompany cargada: {len(dim_production_company_df)} registros")
            except Exception as e:
                logger.error(f"  ✗ Error cargando DimProductionCompany: {e}")

            try:
                fact_df = pd.read_parquet(self.gold_path / "fact_movie_release.parquet")
                for _, row in fact_df.iterrows():
                    obj = FactMovieRelease(**row.to_dict())
                    session.add(obj)
                session.commit()
                logger.info(f"  ✓ FactMovieRelease cargada: {len(fact_df)} registros")
            except Exception as e:
                logger.error(f"  ✗ Error cargando FactMovieRelease: {e}")
    
    
    def run(self) -> None:
        """Ejecuta el pipeline completo ETL."""
        logger.info("\n" + "="*70)
        logger.info("INICIANDO ETL PIPELINE")
        logger.info("="*70 + "\n")
        
        try:
            logger.info("[FASE 1/2] BRONZE → SILVER\n")
            
            movies_df, credits_df = self.read_bronze()
            movies_df = self.clean_movies(movies_df)
            credits_df = self.clean_credits(credits_df)
            self.save_silver(movies_df, credits_df)
            
            logger.info("\n[FASE 2/2] SILVER → GOLD \n")
            
            dim_date = self.build_dim_date(movies_df)
            dim_movie = self.build_dim_movie(movies_df)
            dim_genre = self.build_dim_genre(movies_df)
            dim_director = self.build_dim_director(credits_df)
            dim_production_company = self.build_dim_production_company(movies_df)
            dim_language = self.build_dim_language(movies_df)
            dim_country = self.build_dim_country(movies_df)
            
            dims = {
                'dim_date': dim_date,
                'dim_movie': dim_movie,
                'dim_genre': dim_genre,
                'dim_director': dim_director,
                'dim_production_company': dim_production_company,
                'dim_language': dim_language,
                'dim_country': dim_country,
            }
            
            bridge_df = self.build_bridge_movie_genre(movies_df, dim_movie, dim_genre)
            fact_df = self.build_fact_movie_release(
                movies_df, 
                credits_df,
                dim_movie, 
                dim_date, 
                dim_director,
                dim_language,
                dim_production_company,
                dim_country
            )
            
            self.save_gold(dims, bridge_df, fact_df)
            
            create_db_and_tables()
            logger.info("✓ Tablas de BD creadas: data/gold/warehouse.db")  
            
            logger.info("\n[FASE 3/3] GOLD → SQLite\n")
            self.load_gold_to_database()
            
            logger.info("\n" + "="*70)
            logger.info("ETL PIPELINE COMPLETADO EXITOSAMENTE")
            logger.info("="*70 + "\n")
            
        except Exception as e:
            logger.error(f"\nERROR EN ETL PIPELINE: {e}", exc_info=True)
            raise

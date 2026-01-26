"""Mappers - Conversión entre ORM Models y Domain Entities."""

from typing import List, Optional
from infrastructure.models import (
    DimMovie,
    DimDate,
    DimGenre,
    DimDirector,
    DimCountry,
    DimProductionCompany,
    DimLanguage,
    BridgeMovieGenre,
    FactMovieRelease,
)
from domain import (
    MovieDomainEntity,
    DateDomainEntity,
    GenreDomainEntity,
    DirectorDomainEntity,
    CountryDomainEntity,
    ProductionCompanyDomainEntity,
    LanguageDomainEntity,
    MovieReleaseDomainEntity,
)


class DomainMapper:
    """Mapea ORM Models a Domain Entities."""
    
    @staticmethod
    def to_genre_domain(genre: DimGenre) -> GenreDomainEntity:
        """Convierte DimGenre (ORM) a GenreDomainEntity."""
        return GenreDomainEntity(
            id=genre.id,
            name=genre.name
        )
    
    @staticmethod
    def to_director_domain(director: DimDirector) -> DirectorDomainEntity:
        """Convierte DimDirector (ORM) a DirectorDomainEntity."""
        return DirectorDomainEntity(
            id=director.id,
            name=director.name,
        )
    
    @staticmethod
    def to_country_domain(country: DimCountry) -> CountryDomainEntity:
        """Convierte DimCountry (ORM) a CountryDomainEntity."""
        return CountryDomainEntity(
            id=country.id,
            iso_3166_1=country.iso_3166_1,
            country_name=country.country_name
        )
    
    @staticmethod
    def to_production_company_domain(company: DimProductionCompany) -> ProductionCompanyDomainEntity:
        """Convierte DimProductionCompany (ORM) a ProductionCompanyDomainEntity."""
        return ProductionCompanyDomainEntity(
            id=company.id,
            name=company.name,
            origin_country=company.origin_country
        )
    
    @staticmethod
    def to_language_domain(language: DimLanguage) -> LanguageDomainEntity:
        """Convierte DimLanguage (ORM) a LanguageDomainEntity."""
        return LanguageDomainEntity(
            id=language.id,
            iso_639_1=language.iso_639_1,
            language_name=language.language_name
        )
    
    @staticmethod
    def to_date_domain(date: DimDate) -> DateDomainEntity:
        """Convierte DimDate (ORM) a DateDomainEntity."""
        return DateDomainEntity(
            id=date.id,
            date=date.date,
            year=date.year,
            month=date.month,
            month_name=date.month_name,
            day=date.day,
            day_of_week=date.day_of_week,
            day_of_week_name=date.day_of_week_name,
            quarter=date.quarter,
            week=date.week,
            is_weekend=date.is_weekend
        )
    
    @staticmethod
    def to_movie_domain(
        movie: DimMovie,
        fact: FactMovieRelease,
        genres: List[GenreDomainEntity],
        release_date: Optional[DateDomainEntity] = None
    ) -> MovieDomainEntity:
        """
        Convierte DimMovie + FactMovieRelease + Géneros a MovieDomainEntity.
        
        Esta es la conversión más compleja, combinando datos de múltiples tablas.
        """
        return MovieDomainEntity(
            id=movie.id,
            title=movie.title,
            overview=movie.overview or "",
            original_language=movie.original_language,
            runtime=movie.runtime,
            status=movie.status,
            release_date=release_date.date if release_date else fact.release_date_id,
            budget=fact.budget,
            revenue=fact.revenue,
            popularity=fact.popularity,
            vote_average=fact.vote_average,
            genres=genres
        )
    
    @staticmethod
    def to_movie_release_domain(fact: FactMovieRelease) -> MovieReleaseDomainEntity:
        """Convierte FactMovieRelease (ORM) a MovieReleaseDomainEntity."""
        return MovieReleaseDomainEntity(
            id=fact.id,
            movie_info_id=fact.movie_info_id,
            release_date_id=fact.release_date_id,
            director_id=fact.director_id,
            language_id=fact.language_id,
            company_id=fact.company_id,
            country_id=fact.country_id,
            date_id=fact.date_id,
            budget=fact.budget,
            revenue=fact.revenue,
            popularity=fact.popularity,
            vote_average=fact.vote_average,
            runtime=None  # No está en FactMovieRelease, pero está en MovieDomainEntity
        )

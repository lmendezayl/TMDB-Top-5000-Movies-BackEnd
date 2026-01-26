"""ORM Models - Representación de tablas en la Base de Datos."""

from .fact_movie_release import FactMovieRelease
from .dim_movie import DimMovie
from .dim_genre import DimGenre
from .bridge_movie_genre import BridgeMovieGenre
from .dim_director import DimDirector
from .dim_country import DimCountry
from .dim_language import DimLanguage
from .dim_production_company import DimProductionCompany
from .dim_date import DimDate

__all__ = [
    "DimMovie",
    "DimDate",
    "DimGenre",
    "DimDirector",
    "DimProductionCompany",
    "DimLanguage",
    "DimCountry",
    "BridgeMovieGenre",
    "FactMovieRelease",
]
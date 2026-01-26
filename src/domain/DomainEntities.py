from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List

# ==================== DIMENSIONES ====================

class GenreDomainEntity(BaseModel):
    """Representa un género cinematográfico."""
    id: int
    name: str


class DirectorDomainEntity(BaseModel):
    """Representa un director de cine."""
    id: int
    name: str
    profile_path: Optional[str] = None


class CountryDomainEntity(BaseModel):
    """Representa un país productor."""
    id: int
    iso_3166_1: Optional[str] = None
    country_name: str


class ProductionCompanyDomainEntity(BaseModel):
    """Representa una compañía productora."""
    id: int
    name: str
    origin_country: Optional[str] = None


class LanguageDomainEntity(BaseModel):
    """Representa un idioma."""
    id: int
    iso_639_1: str
    language_name: str


class DateDomainEntity(BaseModel):
    """Representa una fecha en el calendario."""
    id: int
    date: datetime
    year: int
    month: int
    month_name: str
    day: int
    day_of_week: int
    day_of_week_name: str
    quarter: int
    week: int
    is_weekend: bool


# ==================== AGREGADOS / VISTAS ====================

class MovieDomainEntity(BaseModel):
    """
    Representa una película completa desde el punto de vista del negocio.
    Combina datos de múltiples tablas (DimMovie + FactMovieRelease + DimDate).
    """
    id: int
    title: str
    overview: str
    original_language: str
    runtime: Optional[int] = None
    status: str
    release_date: Optional[datetime] = None
    budget: float
    revenue: float
    popularity: float
    vote_average: float
    genres: List[GenreDomainEntity] = Field(default_factory=list)


class MovieReleaseDomainEntity(BaseModel):
    """
    Representa un lanzamiento de película (tabla de hechos).
    Incluye solo los datos de hechos, sin desnormalización.
    """
    id: int
    movie_info_id: int
    release_date_id: Optional[int] = None
    director_id: Optional[int] = None
    language_id: Optional[int] = None
    company_id: Optional[int] = None
    country_id: Optional[int] = None
    date_id: Optional[int] = None
    budget: float
    revenue: float
    popularity: float
    vote_average: float
    runtime: Optional[int] = None
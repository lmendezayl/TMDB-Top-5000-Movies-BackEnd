from typing import Optional
from sqlmodel import Field, SQLModel

class DimMovie(SQLModel, table=True):
    """Dimensión: Película. Información descriptiva de películas."""
    __tablename__ = "dim_movie"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    original_id: int = Field(index=True)
    title: str = Field(index=True)
    original_language: str = Field(default="en")
    overview: Optional[str] = None
    tagline: Optional[str] = None
    status: str = Field(index=True)
    runtime: Optional[int] = None
    
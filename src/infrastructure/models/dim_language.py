from typing import Optional
from sqlmodel import Field, SQLModel

class DimLanguage(SQLModel, table=True):
    """Dimensión: Idioma. Códigos y nombres de idiomas ISO-639-1."""
    __tablename__ = "dim_language"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    iso_639_1: str = Field(index=True, unique=True)
    language_name: str = Field(index=True)
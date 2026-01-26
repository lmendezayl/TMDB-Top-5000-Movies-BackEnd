from typing import Optional
from sqlmodel import Field, SQLModel

class DimCountry(SQLModel, table=True):
    """Dimensión: País. Países de producción con códigos ISO-3166-1."""
    __tablename__ = "dim_country"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    iso_3166_1: Optional[str] = Field(default=None, index=True)
    country_name: str = Field(index=True)
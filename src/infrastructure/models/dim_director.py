from typing import Optional
from sqlmodel import Field, SQLModel

class DimDirector(SQLModel, table=True):
    """Dimensión: Director. Información de directores de cine."""
    __tablename__ = "dim_director"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True)
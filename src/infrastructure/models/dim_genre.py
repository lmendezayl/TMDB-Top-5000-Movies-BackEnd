from typing import Optional
from sqlmodel import Field, SQLModel

class DimGenre(SQLModel, table=True):
    __tablename__ = "dim_genre"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    

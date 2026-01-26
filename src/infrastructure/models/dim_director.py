from typing import Optional
from sqlmodel import Field, SQLModel

class DimDirector(SQLModel, table=True):
    __tablename__ = "dim_director"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True) 
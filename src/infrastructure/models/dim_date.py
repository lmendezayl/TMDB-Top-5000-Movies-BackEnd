from typing import Optional
from sqlmodel import Field, SQLModel

class DimCountry(SQLModel, table=True):
    __tablename__ = "dim_country"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    country_name: str = Field(index=True)
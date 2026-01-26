from typing import Optional
from sqlmodel import Field, SQLModel

class DimProductionCompany(SQLModel, table=True):
    __tablename__ = "dim_production_company"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True)
import datetime
from typing import Optional
from sqlmodel import Field, SQLModel

class DimDate(SQLModel, table=True):
    __tablename__ = "dim_date"
    
    id: int = Field(primary_key=True)
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
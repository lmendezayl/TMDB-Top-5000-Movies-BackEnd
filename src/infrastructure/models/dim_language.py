from typing import Optional
from sqlmodel import Field, SQLModel

class DimLanguage(SQLModel, table=True):
    __tablename__ = "dim_language"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    language_name: str = Field(index=True)
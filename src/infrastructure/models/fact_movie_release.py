from typing import Optional
from sqlmodel import Field, SQLModel

class FactMovieRelease(SQLModel, table=True):
    __tablename__ = "fact_movie_release"
    id: Optional[int] = Field(default=None, primary_key=True)
    
    budget: int 
    revenue: int 
    popularity: float
    vote_average: float
    runtime: Optional[int] = None

    movie_info_id: int = Field(foreign_key="dim_movie.id")
"""     release_date_id: int = Field(foreign_key="dim_date.id")
    director_id: int = Field(foreign_key="dim_person.id")
    language_id: Optional[int] = Field(default=None, foreign_key="dim_language.id")
    company_id: Optional[int] = Field(default=None, foreign_key="dim_company.id")
    
     """
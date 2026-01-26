from typing import Optional
from sqlmodel import Field, SQLModel

class BridgeMovieGenre(SQLModel, table=True):
    __tablename__ = "bridge_movie_genre"
    # Composite key for mtm relationship between movies and genres
    movie_id: int = Field(primary_key=True, foreign_key="dim_movie.id")
    genre_id: int = Field(primary_key=True, foreign_key="dim_genre.id")
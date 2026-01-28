from sqlmodel import Session, select, func
from src.infrastructure.database import engine
from src.infrastructure.models import FactMovieRelease, BridgeMovieGenre, DimMovie

def check_db():
    with Session(engine) as session:
        n_facts = session.exec(select(func.count(FactMovieRelease.id))).one()
        n_bridges = session.exec(select(func.count(BridgeMovieGenre.id))).one()
        
        print(f"Total Facts: {n_facts}")
        print(f"Total Bridges: {n_bridges}")
        
        non_zero_pop = session.exec(select(func.count(FactMovieRelease.id)).where(FactMovieRelease.popularity > 0)).one()
        print(f"Facts with popularity > 0: {non_zero_pop}")
        
        non_zero_budget = session.exec(select(func.count(FactMovieRelease.id)).where(FactMovieRelease.budget > 0)).one()
        print(f"Facts with budget > 0: {non_zero_budget}")

        movie = session.exec(select(DimMovie).where(DimMovie.title.like("%Avatar%"))).first()
        if movie:
            print(f"\nMovie: {movie.title} (ID: {movie.id})")
            fact = session.exec(select(FactMovieRelease).where(FactMovieRelease.movie_info_id == movie.id)).first()
            if fact:
                print(f"  Fact: Pop={fact.popularity}, Budget={fact.budget}, Revenue={fact.revenue}, Vote={fact.vote_average}")
            else:
                print("  No Fact found!")
            
            genres = session.exec(select(BridgeMovieGenre).where(BridgeMovieGenre.movie_id == movie.id)).all()
            print(f"  Genres count: {len(genres)}")
        else:
            print("Avatar not found")

if __name__ == "__main__":
    check_db()

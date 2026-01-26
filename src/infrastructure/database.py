from sqlmodel import SQLModel, create_engine, Session

sqlite_file_name = "data/gold/warehouse.db"
sqlite_url = f"sqlite:///{sqlite_file_name}"

# check_same_thread is needed for sqlite to allow different threads with the same connection on the database
# https://fastapi.tiangolo.com/tutorial/sql-databases/#create-an-engine
connect_args = {"check_same_thread": False}
engine = create_engine(sqlite_url, connect_args=connect_args)

def get_session():
    with Session(engine) as session:
        yield session
    
def create_db_and_tables():
    SQLModel.metadata.create_all(engine)
    
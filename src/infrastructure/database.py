from sqlmodel import SQLModel, create_engine, Session
from pathlib import Path

db_path = Path(__file__).resolve().parent.parent.parent / "data" / "gold" / "warehouse.db"
DATABASE_URL = f"sqlite:///{db_path}"

engine = create_engine(
    DATABASE_URL,
    echo=False,
    connect_args={"check_same_thread": False}
)

def create_db_and_tables() -> None:
    """Crea todas las tablas en la BD."""
    SQLModel.metadata.create_all(engine)
    print("✓ Tablas creadas en SQLite")

def get_session():
    """Generador de sesiones."""
    with Session(engine) as session:
        yield session
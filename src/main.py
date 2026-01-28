"""FastAPI Application - Data Lake API with ETL, Vector DB, and Star Schema."""

import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends

from infrastructure.database import create_db_and_tables, get_session
from infrastructure import models
from infrastructure.typesense_indexer import TypesenseIndexer
from api.routes import movies_router
from api.routes import search as search_router
from core import get_current_user
from services import ETLPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Event handler para el ciclo de vida de la aplicación.
    - Startup: Ejecutar ETL, crear tablas en la BD, indexar en Typesense
    - Shutdown: Cleanup (opcional)
    """
    logger.info("🚀 Iniciando Data Lake API...")
    
    # Ejecutar ETL: Bronze → Silver → Gold
    try:
        etl = ETLPipeline()
        etl.run()
        logger.info("✓ ETL completado exitosamente")
    except Exception as e:
        logger.error(f"✗ Error en ETL: {e}")
        raise
    
    
    # Indexar en Typesense (Vector DB)
    try:
        indexer = TypesenseIndexer()
        if indexer.client.health_check():
            logger.info("Typesense disponible, indexando películas...")
            from sqlmodel import Session
            from infrastructure.database import engine
            with Session(engine) as session:
                count = indexer.index_movies(session)
                logger.info(f"✓ {count} películas indexadas en Typesense")
        else:
            logger.warning("⚠ Typesense no disponible, búsqueda vectorial deshabilitada")
    except Exception as e:
        logger.warning(f"⚠ Error indexando en Typesense: {e}")
    
    yield
    
    logger.info("🛑 Deteniendo Data Lake API...")


app = FastAPI(
    title="TMDB Top 5000 Movies API",
    description="API REST para acceso a Data Lake con ETL y búsqueda vectorial",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    openapi_url="/openapi.json",
)


@app.get("/", tags=["root"])
async def read_root() -> dict:
    """Endpoint raíz de la API."""
    return {
        "message": "TMDB Top 500 Movies API v1.0.0",
        "status": "running",
        "docs": "/docs"
    }


@app.get("/health", tags=["health"])
async def health_check() -> dict:
    """Health check endpoint."""
    return {"status": "ok", "message": "API is operational"}


@app.get("/protected", tags=["auth"])
async def protected_endpoint(username: str = Depends(get_current_user)) -> dict:
    """
    Endpoint protegido que requiere autenticación Basic Auth.
    
    Credenciales de ejemplo:
    - Username: admin, Password: password123
    - Username: user, Password: user123
    """
    return {
        "message": f"¡Acceso concedido para {username}!",
        "username": username
    }

app.include_router(
    movies_router,
    dependencies=[Depends(get_current_user)]
)

app.include_router(
    search_router.router,
    dependencies=[Depends(get_current_user)]
)
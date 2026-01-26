from contextlib import asynccontextmanager
from http import HTTPStatus
from fastapi import FastAPI
from infrastructure.database import create_db_and_tables
from infrastructure import models

# https://fastapi.tiangolo.com/advanced/events/#lifespan
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting up...")
    create_db_and_tables()
    print("Database tables created at data/gold/warehouse.db")
    yield 

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def read_root():
    return {"Hello": "World"}

@app.get("/health")
async def health_check():
    if HTTPStatus.OK:
        return {"status": "ok"}
    if HTTPStatus.SERVICE_UNAVAILABLE:
        return {"status": "unavailable"}
    return {"status": "unknown"}


    
from contextlib import asynccontextmanager
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


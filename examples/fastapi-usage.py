import os
os.environ['AUTODLA_SQL_VERBOSE'] = 'true'
from fastapi import FastAPI
from autodla import Object, primary_key
from autodla.dbs import PostgresDB
from autodla.connectors.fastapi import FastApiWebConnection
from autodla.utils import DataGenerator
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional


class User(Object):
    id: primary_key = primary_key.auto_increment()
    name: str
    age: int
    tags: list[str] = list()

# Connect to DB and register models. MemoryDB keeps a local SQLite store
# and periodically syncs to the PostgreSQL server.
db = PostgresDB()
db.attach([User])


# Create fastapi app and add router
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
conn = FastApiWebConnection(app, db)


@app.get("/list_users")
async def list_users():
    return User.all(limit=None)


@app.post("/new_user")
async def new_user():
    return User.new(
        name=DataGenerator.name(),
        age=DataGenerator.age()
    )

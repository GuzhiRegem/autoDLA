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

class Group(Object):
    id: primary_key = primary_key.auto_increment()
    group_name: str
    participants: list[User]


# Connect to DB and register models
db = PostgresDB()
db.attach([User, Group])


#setup DB
db.clean_db(DO_NOT_ASK=True)
lis = []
for i in range(2):
    lis.append(User.new(
        name=DataGenerator.name(),
        age=DataGenerator.age()
    ))
g = Group.new(
    participants=lis,
    group_name="Group 1"
)


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

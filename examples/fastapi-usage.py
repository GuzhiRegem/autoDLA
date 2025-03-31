import os
os.environ['AUTODLA_SQL_VERBOSE'] = 'true'
from fastapi import FastAPI
from autodla import Object, primary_key
from autodla.dbs import PostgresDB
from autodla.connectors.fastapi import connect_db
from autodla.utils import DataGenerator
from fastapi.middleware.cors import CORSMiddleware


class User(Object):
    id: primary_key = primary_key.auto_increment()
    name: str
    age: int

class Group(Object):
    id: primary_key = primary_key.auto_increment()
    participants: list[User]
    created_by: User = None
    group_name: str

# Connect to DB and register models
db = PostgresDB()
db.attach([User, Group])





#setup DB
for obj in User.all(limit=None):
    obj.delete()
for obj in Group.all(limit=None):
    obj.delete()
lis = []
for i in range(2):
    lis.append(User.new(
        name=DataGenerator.name(),
        age=DataGenerator.age()
    ))
g = Group.new(
    participants=lis,
    created_by=lis[0],
    group_name="Group 1"
)
g2 = Group.new(
    participants=[],
    group_name="Group 2"
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
connect_db(app, db)

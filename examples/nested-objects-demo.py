import os
os.environ['AUTODLA_SQL_VERBOSE'] = 'true'
from autodla import Object, primary_key
from autodla.dbs import PostgresDB


# Create models
class User(Object):
    id: primary_key = primary_key.auto_increment()
    name: str
    age: int

class Group(Object):
    id: primary_key = primary_key.auto_increment()
    participants: list[User] # Nested structure
    group_name: str


# Connect to DB and register models
db = PostgresDB()
db.attach([User, Group])






from autodla import Object, primary_key
from autodla.dbs import PostgresDB







class User(Object):
    id: primary_key = primary_key.auto_increment()
    name: str
    age: int

class Group(Object):
    participants: list[User]
    group_name: str

# Connect to DB and register models
db = PostgresDB()
db.attach([User, Group])





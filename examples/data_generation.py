import os
os.environ['AUTODLA_SQL_VERBOSE'] = 'true'
from autodla import Object, primary_key
from autodla.dbs import PostgresDB
from autodla.utils import DataGenerator




class User(Object):
    id: primary_key = primary_key.auto_increment()
    name: str
    age: int

# Connect to DB and register models
db = PostgresDB()
db.attach([User])





# Create 2 users with generated data
for i in range(2):
    data = {
        'name': DataGenerator.name(),
        'age': DataGenerator.age()
    }
    n = User.new(**data)
    print("added:", n)

# Retrieve all users
users = User.all(limit=None)
for user in users:
    print(user)
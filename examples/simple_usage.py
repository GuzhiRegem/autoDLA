import os
os.environ['AUTODLA_SQL_VERBOSE'] = 'true'
from autodla import Object, primary_key
from autodla.dbs import PostgresDB


# Create model
class User(Object):
    id: primary_key = primary_key.auto_increment()
    name: str
    age: int


# Connect to DB and register models
db = PostgresDB()
db.attach([User])


# Create a user
user = User.new(name="John", age=30)
print("new user:", user)

# Retrieve all users
users = User.all(limit=None)
for user in users:
    print(user)

# Integrity of python id for the percieved same object
print(id(user), id(users[-1]))

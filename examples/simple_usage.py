import os
os.environ['AUTODLA_SQL_VERBOSE'] = True
from autodla import Object, persistance, primary_key
from autodla.dbs import PostgresDB

@persistance
class User(Object):
    id: primary_key = primary_key.auto_increment()
    name: str
    age: int

# Connect to DB and register models
db = PostgresDB()
db.attach([User])

# Create a user
user = User.new(name="John", age=30)
print(user)

# Retrieve all users
users = User.all()
for user in users:
    print(user)

# Filter users
adults = User.filter(lambda x: x.age >= 18)

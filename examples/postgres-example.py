import os
os.environ['AUTODLA_SQL_VERBOSE'] = 'false'
from autodla import Object, primary_key
from autodla.dbs import PostgresDB


# Create model
class User(Object):
    id: primary_key = primary_key.auto_increment()
    name: str
    age: int


# Connect to DB and register models. MemoryDB keeps a local SQLite store
# and periodically syncs to the PostgreSQL server.
db = PostgresDB()
db.attach([User])

# Create a user
user = User.new(name="John", age=30)
user = User.new(name="John", age=31)
user = User.new(name="John", age=32)
user = User.new(name="John", age=33)
user.update(name="Jhony2")
user.update(name="Jhony3")
user.update(name="Jhony4")
print("new user:", user)

users = User.all(limit=10, skip=0)
for user_i in users:
    print("user_i:", user_i)
    user_i.update(age=user_i.age + 1)

db.exit()

# Print usage metrics, shows that PostgresDB executed far less queries than MemoryDB
print(db.usage_metrics)
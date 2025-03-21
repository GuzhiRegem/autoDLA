from dbs.postgresdb import PostgresDB
from datetime import datetime, timedelta, date
import os
from engine.utils.data_generation import DataGenerator

from engine.object import Object, persistance, primary_key

@persistance
class User(Object):
    id: primary_key = primary_key.auto_increment()
    name: str
    age: int
    mass: float
    created_at: datetime

db = PostgresDB()
db.attach([User])

for i in range(2):
    data = {
        'name': DataGenerator.name(),
        'age': DataGenerator.age(),
        'mass': DataGenerator.mass(),
        'created_at': DataGenerator.created_at()
    }
    n = User.new(**data)

print(User.all())
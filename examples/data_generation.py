from autodla import Object, persistance, primary_key
from autodla.dbs import PostgresDB
from autodla.utils import DataGenerator

@persistance
class User(Object):
    id: primary_key = primary_key.auto_increment()
    name: str
    age: int

for i in range(2):
    data = {
        'name': DataGenerator.name(),
        'age': DataGenerator.age()
    }
    n = User.new(**data)

users = User.all()
for user in users:
    print(user)
from auto_dla import Object, persistance, primary_key
from auto_dla.dbs import PostgresDB
from auto_dla.utils import DataGenerator

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
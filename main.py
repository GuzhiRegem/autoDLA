from dbs.postgresdb import PostgresDB
from datetime import datetime, timedelta, date
import os
from engine.utils.data_generation import DataGenerator
from engine.object import Object, persistance, primary_key

def title(text):
    print(f'\n{"-"*20}{text}{"-"*20}\n')

title("DEFINING CLASSES")
@persistance
class User(Object):
    id: primary_key = primary_key.auto_increment()
    name: str
    age: int
    mass: float
    created_at: datetime

@persistance
class Group(Object):
    id: primary_key = primary_key.auto_increment()
    name: str
    participants: list[User]

db = PostgresDB()
db.attach([Group, User])

print('USER')
for k, v in User.get_types().items():
    print(f'{k}:\t{v}')
print()

print('GROUP')
for k, v in Group.get_types().items():
    print(f'{k}:\t{v}')
print()




# TEST
title('CREATE USERS')
for i in range(2):
    data = {
        'name': DataGenerator.name(),
        'age': DataGenerator.age(),
        'mass': DataGenerator.mass(),
        'created_at': DataGenerator.created_at()
    }
    n = User.new(**data)
title('ALL USERS')
a = User.all()
for i in a:
    print(i, id(i))
u1 = a[0]
title('CREATE GROUP')
g = Group.new(name='Group 1', participants=a)
print(g, id(g))
title("GET GROUP")
g2 = Group.get_by_id(g.id)
print(g2, id(g2))
title("UPDATE USER")
print(u1, id(u1))
u1.update(age=u1.age + 10)
print(g2, id(g2))
title("UPDATE GROUP")
g.update(participants=g.participants[1:], name="Group 1.1")
print(g, id(g))
title("GET GROUP 2")
g2 = Group.get_by_id(g.id)
print(g2, id(g2))
h = g2.history()
print(h)

title("filter")
usrs = User.filter(lambda x: x.age > 15, limit=10)
for i in usrs:
    print(i, id(i))
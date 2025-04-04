You can connect AutoDLA with the web framework of your choosing to build Data Apps quickly

## Setup
For this example we are going to use FastAPI
```bash
pip install autodla[fastapi] #install dependency
```

And the next schema:
```python
from autodla import Object, primary_key
from autodla.dbs import PostgresDB

class User(Object):
    id: primary_key = primary_key.auto_increment()
    name: str
    age: int

class Group(Object):
    id: primary_key = primary_key.auto_increment()
    group_name: str
    participants: list[User]

db = PostgresDB()
db.attach([User, Group])
```
We create the data:
```python
users = []
for i in range(2):
    users.append(User.new(
        name=DataGenerator.name(),
        age=DataGenerator.age()
    ))
group = Group.new(
    group_name='Group 1',
    participants=users
)
```
## Connection
To connect AutoDLA with the web framework, you just need to use the function `connnect_db`:
```python
# default fastAPI app creation
from fastapi import FastAPI
app = FastAPI()

# connect it
from autodla.connectors.fastapi import connect_db
connnect_db(app, db)
```
Now, AutoDLA automatically configured the FastAPI app to be able to use it.
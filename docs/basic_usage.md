AutoDLA works with models, to start, you'll need to first build a usable model that inherits from [Object](/reference/object/):
```python
from autodla import Object, primary_key

class User(Object):
    id: primary_key = primary_key.auto_increment()
    name: str
    age: int
```
> **WARNING:** For model definition there is **1 rule** to ensure good data integrity:

> - Each Model should have one and only one field of type [`primary_key`](/reference/primary_key/) (`id` in this case)

If you try to use this, it will fail, as the main focus of the library is to interact with a DataBase, you need a DataBase connection, we'll use PostgreSQL for this example.

```bash
pip install autodla[db-postgres] #install db connector
```

We need to instanciate the DataBase and then attach the Model into it.

```python
from autodla.dbs import PostgresDB

db = PostgresDB()
db.attach([User])
```

Done!

You now can use your object as you would normally and the changes are going to be reflected on the DataBase, enjoy!

---

### Uses

#### Create a user
```python
user = User.new(name="John", age=30)
```

#### Retrieve all users
```python
users = User.all(limit=None)
```

#### Integrity of python id for the percieved same object
```python
print(id(user) === id(users[-1]))
# This prints True
```

---

This example is [available in the repository](https://github.com/GuzhiRegem/autoDLA/blob/main/examples/simple_usage.py)
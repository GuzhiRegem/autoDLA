# AutoDLA

A lightweight, powerful and modern ORM focused on simplifying development.

[Documentation](https://autoDLA.readthedocs.io/en/latest/)

## Installation

### Using PyPi
```bash
pip install autodla
```

### From source
```bash
git clone https://github.com/GuzhiRegem/autoDLA.git
cd autoDLA/
pip install .
```

### Features

To keep AutoDLA as lightweight as possible, you need to install separatelly the features you need to use, that includes the DataBase connection you are going to use, to install a feature, you can run the installation command as follows:
```bash
pip install autodla[<package_name>]
```
For example, PostgreSQL connection:
```bash
pip install autodla[db-postgres]
```
MemoryDB comes bundled with AutoDLA for quick prototyping without any external
dependencies.

## How to use

AutoDLA works with models, to start, you'll need to first build a usable model that inherits from [Object](reference/object.md):
```python
from autodla import Object, primary_key

class User(Object):
    id: primary_key = primary_key.auto_increment()
    name: str
    age: int
```
> **WARNING:** For model definition there is **1 rule** to ensure good data integrity:

> - Each Model should have one and only one field of type [`primary_key`](reference/primary_key.md) (`id` in this case)

If you try to use this, it will fail, as the main focus of the library is to interact with a DataBase, you need a DataBase connection, we'll use PostgreSQL for this example.

```bash
pip install autodla[db-postgres] #install db connector
```

We need to instanciate the DataBase and then attach the Model into it.

```python
from autodla.dbs import PostgresDB, MemoryDB

# Use MemoryDB() for pure in-memory work. PostgresDB maintains a local
# SQLite store and syncs it to PostgreSQL in the background.
db = MemoryDB()
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
users = User.all(limit=None, skip=0)
```

#### Integrity of python id for the percieved same object
```python
print(id(user) === id(users[-1]))
# This prints True
```

---

This is protected under MIT licence
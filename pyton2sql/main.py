from autoDLA.pyton2sql.dbs.postgresdb import PostgresDB
from datetime import datetime, timedelta, date
import os


import random
select_random = lambda x: x[int(random.random() * len(x))]
import builtins
GLOBAL_DICT = {**vars(builtins), **globals(), **locals()}

db = PostgresDB()
schema = {
    "name": str,
    "age": int,
    "mass": float,
    "created_at": datetime
}
db.ensure_table('user', schema)
possible_values = {
    "name": ['James', 'John', 'Robert', 'Michael', 'William', 'David', 'Richard', 'Joseph', 'Thomas', 'Charles',
              'Mary', 'Patricia', 'Jennifer', 'Linda', 'Elizabeth', 'Barbara', 'Susan', 'Jessica', 'Sarah', 'Karen',
              'Emma', 'Olivia', 'Noah', 'Liam', 'Sophia', 'Ava', 'Isabella', 'Mia', 'Abigail', 'Emily',
              'Alexander', 'Ethan', 'Daniel', 'Matthew', 'Aiden', 'Henry', 'Joseph', 'Jackson', 'Samuel', 'Sebastian',
              'Sofia', 'Charlotte', 'Amelia', 'Harper', 'Evelyn', 'Aria', 'Scarlett', 'Grace', 'Chloe', 'Victoria'],
    "age": [i for i in range(10, 30)],
    "mass": [(0.5 + (0.1*i)) for i in range(10, 60)],
    "created_at": [datetime.now() - timedelta(days=i, minutes=10+20*random.random()) for i in range(10, 60)]
}
inserts = []
for i in range(200):
    data = {
        'name': select_random(possible_values["name"]),
        'age': select_random(possible_values["age"]),
        'mass': select_random(possible_values["mass"]),
        'created_at': select_random(possible_values["created_at"])
    }
    inserts.append(data)
db.insert_into_table('user', schema, inserts)
print()

def test_function(func):
    print("LAMBDA:\n", lambda_to_text(func))
    try:
        print("SQL:\n", lambda_to_sql(schema, func, db.data_transformer, ctx_vars={**GLOBAL_DICT, **globals(), **locals()}, ))
    except Exception as e:
        print("ERROR:", e)
    print('---------------\n')

import json
print(json.dumps(db.data_transformer.convert_data_schema(schema), indent=4), '\n')

test_function(lambda x: x.age + 2)
test_function(lambda x: x.age - 3)
test_function(lambda x: x.mass * 1.5)
test_function(lambda x: x.age / 2)

# Comparison operations
test_function(lambda x: x.age > 18)
test_function(lambda x: x.age < 65)
test_function(lambda x: x.age >= 21)
test_function(lambda x: x.age <= 30)
test_function(lambda x: x.age == 25)
test_function(lambda x: x.age != 40)
test_function(lambda x: x.age != 40)

# Logical combinations
test_function(lambda x: x.age > 18 and x.age < 65)
test_function(lambda x: x.age < 18 or x.age > 65)
test_function(lambda x: not (x.age < 18))

# String operations
test_function(lambda x: x.name == 'John')
test_function(lambda x: x.name != 'Jane')
test_function(lambda x: x.name in ['John', 'Jane', 'Doe'])
test_function(lambda x: x.name not in ['Alice', 'Bob'])

# Date comparisons
test_function(lambda x: x.created_at > datetime(2023, 1, 1))
test_function(lambda x: x.created_at < datetime(2024, 1, 1))

# Complex logical conditions
test_function(lambda x: (x.age > 18 and x.age < 65) or x.name == 'Special')
test_function(lambda x: x.mass > 70.5 and (x.age < 30 or x.name in ['Admin', 'SuperUser']))

# Error cases
test_function(lambda x: x.age + '2')  # Type mismatch
test_function(lambda x: x.ages + 2)   # Non-existent column
test_function(lambda x: x.age > 2 and x.age + 3)  # Unusual logical combination

# Additional edge cases
test_function(lambda x: x.name is None)
test_function(lambda x: x.name is not None)
test_function(lambda x: x.mass == 0.0)
test_function(lambda x: x.created_at >= datetime.now())

# use external variables
print("#####################\ncustom_var = 20\n#####################")
custom_var = 20
test_function(lambda x: x.age > custom_var)
test_function(lambda x: x.age > custom_var + 1)
test_function(lambda x: x.age > var2)

print("#####################\ndef my_func(n):\n    return n + 2\n#####################")
# use external functions
def my_func(n):
    return n + 2
test_function(lambda x: x.age > my_func(23))
test_function(lambda x: x.age > my_func2(23))

test_function(lambda x: x.name.startswith("m"))
test_function(lambda x: "martin".startswith("m"))
test_function(lambda x: x.age == round(x.age))
test_function(lambda x: x.age == round(15.5))

#eval
test_function(lambda x: 2 == 2)
test_function(lambda x: 2 + 2)

#conditional
test_function(lambda x: 'Minor' if x.age < 18 else 'Adult')
test_function(lambda x: 'Minor' if 12 < 18 else 'Adult')
from datetime import datetime, timedelta
import random
select_random = lambda x: x[int(random.random() * len(x))]

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

class DataGenerator:
    @staticmethod
    def name() -> str:
        return select_random(possible_values["name"])
    
    @staticmethod
    def age() -> int:
        return select_random(possible_values["age"])
    
    @staticmethod
    def mass() -> float:
        return select_random(possible_values["mass"])
    
    @staticmethod
    def created_at() -> datetime:
        return select_random(possible_values["created_at"])
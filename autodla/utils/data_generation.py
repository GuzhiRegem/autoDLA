from datetime import datetime, timedelta
import random
from typing import List


def select_random(x: list):
    """
    Selects a random element from the provided list.
    :param x: List from which to select a random element.
    :return: A randomly selected element from the list.
    """
    return x[int(random.random() * len(x))]


class PossibleValues:
    name: List[str] = [
        'James', 'John', 'Robert', 'Michael', 'William', 'David', 'Richard',
        'Joseph', 'Thomas', 'Charles', 'Mary', 'Patricia', 'Jennifer', 'Linda',
        'Elizabeth', 'Barbara', 'Susan', 'Jessica', 'Sarah', 'Karen', 'Emma',
        'Olivia', 'Noah', 'Liam', 'Sophia', 'Ava', 'Isabella', 'Mia',
        'Abigail', 'Emily', 'Alexander', 'Ethan', 'Daniel', 'Matthew', 'Aiden',
        'Henry', 'Joseph', 'Jackson', 'Samuel', 'Sebastian', 'Sofia',
        'Charlotte', 'Amelia', 'Harper', 'Evelyn', 'Aria', 'Scarlett', 'Grace',
        'Chloe', 'Victoria'
    ]
    age: List[int] = [i for i in range(10, 30)]
    mass: List[float] = [(0.5 + (0.1*i)) for i in range(10, 60)]
    created_at: List[datetime] = [
        datetime.now() - timedelta(
            days=i, minutes=10+20*random.random()
        ) for i in range(10, 60)
    ]


class DataGenerator:
    @staticmethod
    def name() -> str:
        return select_random(PossibleValues.name)

    @staticmethod
    def age() -> int:
        return select_random(PossibleValues.age)

    @staticmethod
    def mass() -> float:
        return select_random(PossibleValues.mass)

    @staticmethod
    def created_at() -> datetime:
        return select_random(PossibleValues.created_at)

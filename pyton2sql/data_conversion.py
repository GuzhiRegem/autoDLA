from datetime import datetime, date
from dataclasses import dataclass
from typing import Callable, Any

DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

@dataclass
class DataConversion:
    name: str
    transform: Callable[[Any], str] = lambda x: f"{x}"

class DataTransformer:
    TYPE_DICT = {}

    @classmethod
    def get_data_field(cls, v) -> DataConversion:
        return cls.TYPE_DICT.get(v)

    @classmethod
    def convert_data_schema(cls, schema):
        out = {}
        for k, v in schema.items():
            f = cls.get_data_field(v)
            if f is not None:
                out[k] = f.name
        return out
    
    @staticmethod
    def validate_data_from_schema(schema, data):
        extra_keys = []
        for i in data:
            if i not in schema:
                extra_keys.append(i)
        if extra_keys:
            raise ValueError(f"Exta values found: {extra_keys}")
        missing_values = []
        for i in data:
            if i not in schema:
                missing_values.append(i)
        if missing_values:
            raise ValueError(f"Missing values: {missing_values}")
        invalid_types = []
        for i in data:
            if not isinstance(data[i], schema[i]):
                invalid_types.append([i, type(data[i]), schema[i]])
        if invalid_types:
            msg = 'Invalid types:\n'
            for i in invalid_types:
                msg +=  f'{i}: expected {schema[i]} got {type(data[i])}\n'
            raise ValueError(msg)
    
    @classmethod
    def convert_data(cls, data):
        v = cls.get_data_field(type(data))
        if v is not None:
            return v.transform(data)
        if type(data) == list:
            return f"({', '.join([cls.convert_data(i) for i in data])})"
        raise TypeError(f"Missing transformer for class {type(data).__name__}")
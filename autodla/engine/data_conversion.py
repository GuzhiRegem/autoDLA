from datetime import datetime, date
from dataclasses import dataclass
from types import NoneType
from typing import Callable, Any, Literal, Optional, Union, get_args, get_origin
import os

DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
if "DATETIME_FORMAT" in os.environ:
    DATETIME_FORMAT = os.environ.get("DATETIME_FORMAT")

@dataclass
class DataConversion:
    name: str
    transform: Callable[[Any], str] = lambda x: f"{x}"

@dataclass
class MethodArgument:
    st: str
    tp: type

class GlobalTypedMethod:
    def __init__(self, return_type: type, definition : str, args : dict[type]):
        self.return_type = return_type
        self.definition = definition
        self.args = args
    
    def check_type(self, expected_type, compared_type):
        if type(expected_type) == list:
            for type_i in expected_type:
                if type_i is None and compared_type is None:
                    return True
                if compared_type == type_i:
                    return True
        elif compared_type == expected_type:
            return True
        return False
    
    def __call__(self, *args, **kwds):
        keys = list(self.args.keys())
        arguments = {}
        for i, k in enumerate(keys):
            v = None
            if i < len(args):
                v = args[i]
            if k in kwds:
                v = kwds[k]
            if not self.check_type(self.args[k], getattr(v, 'tp', None)):
                raise TypeError(f"expected type {self.args[k]} for argument {k}, got {type(v)}")
            arguments[k] = getattr(v, 'st', None)
        return self.definition(arguments), self.return_type

class TypedMethod(GlobalTypedMethod):
    def __init__(self, caller_type: type, return_type: type, definition : str, args : dict[type]):
        self.caller_type = caller_type
        super().__init__(return_type, definition, args)
    
    def __call__(self, caller: MethodArgument, *args, **kwds):
        if any([(type(arg) != MethodArgument) for arg in args]) or any([(type(arg) != MethodArgument) for arg in list(kwds.values())]):
            raise ValueError("All arguments for data conversion should be MethodArgument")
        keys = list(self.args.keys())
        arguments = {}
        for i, k in enumerate(keys):
            v = None
            if i < len(args):
                v = args[i]
            if k in kwds:
                v = kwds[k]
            if not self.check_type(self.args[k], v.tp if v is not None else None):
                raise TypeError(f"expected type {self.args[k]} for argument {k}, got {v.tp}")
            arguments[k] = v.st
        if not self.check_type(self.caller_type, caller.tp):
            raise TypeError(f"expected caller type {self.caller_type}, got {caller.tp}")
        arguments["caller"] = caller.st
        return self.definition(arguments), self.return_type

class DataTransformer:
    TYPE_DICT = {}
    OPERATOR_DICT = {
        "numeric": {
            'Eq': "="
        },
        "binary": {
            "Add": lambda x, y: f'{x} + {y}',
        },
        "boolean": {
            "And": 'AND',
        },
        "unary": {
            "Not": 'NOT'
        }
    }
    METHODS_MAP = {
        "global": {
            'round': GlobalTypedMethod(
                int,
                lambda data: f"ROUND({data['value']}{str(', ' + data['digits']) if data['digits'] is not None else ''})",
                {"value": [float, int], 'digits': [int, None]}
            ),
            'abs': GlobalTypedMethod(
                int,
                lambda data: f'ABS({data["value"]})',
                {'value': [float, int]}
            )
        },
        "by_type": {
            str: {
                "lower": TypedMethod(
                    str, str,
                    lambda data: f"LOWER({data['caller']})",
                    {}
                ),
                "startswith": TypedMethod(
                    str, bool,
                    lambda data: f"({data['caller']} LIKE '{data['value'][1:-1]}%')",
                    {"value": str}
                )
            },
            datetime: {
                "year": TypedMethod(
                    datetime, int,
                    lambda data: f"EXTRACT(YEAR FROM {data['caller']})",
                    {}
                ),
            }
        }
    }

    NODE_COMPATIBILITY = {}

    @classmethod
    def check_type_compatibilty(cls, tp1, tp2):
        out = tp1 == tp2
        if not out:
            if tp1 in cls.NODE_COMPATIBILITY:
                out = cls.NODE_COMPATIBILITY[tp1] == tp2
        return out

    @classmethod
    def get_type_from_sql_type(cls, sql_type) -> type:
        found = None
        for k, v in cls.TYPE_DICT.items():
            if v.name.upper() == sql_type.upper():
                found = k
        if found is None:
            raise TypeError(f"invalid conversion for sql_type '{sql_type}'")
        return found

    @classmethod
    def get_method(cls, caller_type : type, method_name : str):
        d = cls.METHODS_MAP["global"]
        if caller_type is not None:
            d = cls.METHODS_MAP["by_type"][caller_type]
        return d.get(method_name)

    @classmethod
    def get_operator(cls, operator_type: Literal["numeric", "binary", "boolean", "unary"], op):
        """Convert Python comparison operators to SQL operators"""
        out = cls.OPERATOR_DICT[operator_type].get(type(op).__name__)
        if out is None:
            raise ValueError(f"Unsupported {operator_type} operator: {op.__class__.__name__}")
        return out

    @classmethod
    def get_data_field(cls, v) -> DataConversion:
        if get_origin(v) is Union:
            a = get_args(v)
            if a[1] is NoneType:
                v = a[0]
        return cls.TYPE_DICT.get(v)

    @classmethod
    def convert_data_schema(cls, schema):
        out = {}
        for k, v in schema.items():
            f = cls.get_data_field(v["type"])
            if f is not None:
                out[k] = f.name
                if v.get("nullable") != True:
                    out[k] += " NOT NULL"
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
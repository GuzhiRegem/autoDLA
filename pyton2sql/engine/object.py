from dataclasses import dataclass, field, fields, _MISSING_TYPE
from typing import List, get_origin, ClassVar, Literal, get_args, Any, Optional, TypeVar
import uuid

class primary_key(str):
    @classmethod
    def generate(cls):
        return cls(str(uuid.uuid4()))
    def is_valid(self):
        try:
            uuid.UUID(self)
            return True
        except ValueError:
            return False
    @staticmethod
    def auto_increment():
        return field(default_factory=lambda: primary_key.generate())

@dataclass(kw_only=True)
class Object:
    @classmethod
    def get_types(cls):
        out = {}
        fields = cls.__dict__["__dataclass_fields__"]
        for i in fields:
            if(get_origin(fields[i].type) == ClassVar):
                continue
            type_out = {
                "type": fields[i].type
            }
            if type(fields[i].default) is not _MISSING_TYPE:
                type_out["default"] = fields[i].default
            if type(fields[i].default_factory) is not _MISSING_TYPE:
                type_out["default_factory"] = fields[i].default_factory
            out[i] = type_out
        return out

T = TypeVar('T', bound=Object)
def persistance(cls : type) -> T:
    out = dataclass(cls, kw_only=True)
    if not issubclass(cls, Object):
        raise ValueError(f"{cls.__name__} class should be subclass of 'Object'")
    schema = out.__dataclass_fields__
    has_one_primary_key = [i.type == primary_key for i in schema.values()].count(True) == 1
    if not has_one_primary_key:
        raise ValueError("one primary key should be defined")
    primary_key_field = list(schema.keys())[[i.type for i in schema.values()].index(primary_key)]
    # schema[primary_key_field] = field(default_factory=lambda: primary_key.generate())
    if isinstance(schema[primary_key_field].default_factory, _MISSING_TYPE):
        schema[primary_key_field] = field(
            default_factory=lambda: primary_key.generate()
        )
    out : Object = out
    return out

@persistance
class User(Object):
    id: primary_key = primary_key.auto_increment()
    name: str
    age: int


u = User(name="jhon", age=18)
print(u)
print(User.get_types())
import uuid
from datetime import datetime
from typing import List, get_origin, ClassVar, Literal, get_args, Any, Optional
from dataclasses import dataclass, field, fields, _MISSING_TYPE
import polars as pl

@dataclass(kw_only=True)
class Table:
    DLA_Object_ID: str = field(default_factory=lambda: uuid.uuid4())
    DLA_Operation: Literal["INSERT", "UPDATE", "DELETE"] = field(default="INSERT")
    DLA_Modified_DT: datetime = field(default_factory=lambda: datetime.now())
    DLA_Modified_BY: datetime = field(default_factory=lambda: datetime.now())
    DLA_Is_Current: bool = field(default=True)
    DLA_Is_Active: bool = field(default=True)
    __engine : ClassVar[Any] = None

    @classmethod
    def execute_statement(cls, statement : str) -> Optional[pl.DataFrame]:
        return cls.__engine.execute_statement(statement)

    @classmethod
    def get_table_name(cls):
        return "public.data_class_" + cls.__name__.lower()
    
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
    
    @classmethod
    def all(cls, just_current=True, just_active=True):
        qry = f"SELECT * FROM {cls.get_table_name()}"
        conditions = []
        if just_current:
            conditions.append("DLA_IS_CURRENT")
        if just_active:
            conditions.append("DLA_IS_ACTIVE")
        if conditions != []:
            qry += " WHERE " + " AND ".join(conditions)
        return cls.execute_statement(qry)
    
    def filter(cls, **kwargs):
        schema = cls.get_types()
        for i_field in kwargs:
            if i_field not in schema:
                raise ValueError(f"{cls.get_table_name()}: {i_field} not defined in schema")
            if not isinstance(kwargs[i_field], schema[i_field]["type"]):
                raise ValueError(f"{cls.get_table_name()}: {i_field} expected {schema[i_field]["type"]}, got {type(kwargs[i_field])}")
        qry = f"SELECT * FROM {cls.get_table_name()}"
        conditions = []
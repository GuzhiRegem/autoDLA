from dataclasses import dataclass, field, fields, _MISSING_TYPE
from engine.utils.custom_types import primary_key
from typing import List, get_origin, ClassVar, Literal, get_args, Any, Optional
import polars as pl
from datetime import datetime

@dataclass(kw_only=True)
class Object:
  id: primary_key = field(default_factory=lambda: primary_key.generate())
  __engine : ClassVar[Any] = None

  @classmethod
  def set_engine(cls, engine):
    cls.__engine = engine

  @classmethod
  def execute_statement(cls, statement : str) -> Optional[pl.DataFrame]:
    return cls.__engine.execute_statement(statement)

  @classmethod
  def all(cls):
    return cls.execute_statement(f"SELECT * FROM {cls.get_table_name()}")
  
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
  def get_table_name(cls):
    return "public.data_class_" + cls.__name__.lower()
  
  @classmethod
  def get_by(cls, **kwargs):

    pass
  
  def get_history(self):
    pass
  
  def edit(self, **kwargs):
    pass
  
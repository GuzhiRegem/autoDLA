from datetime import datetime
from engine.utils.custom_types import primary_key


class DataConvertor:
  def __init__(self):
    self.__GENERATORS = {
      "primary_key": self.__primary_key,
      "str": self.__str,
      "int": self.__int,
      "float": self.__float,
      "bool": self.__bool,
      "Literal": self.__literal,
      "datetime": self.__datetime,
      "List": self.__ignore,
    }

  def generate_field_text(self, field_type, field_name, default = None, has_default = False):
    found_func = self.__GENERATORS.get(field_type.__name__)
    if found_func is None:
      raise Exception(f"Field type {field_type.__name__} not found")
    return found_func(field_type, field_name.lower(), default, has_default)

  def __ignore(self, *args, **kwargs):
    return {}
  
  def __primary_key(self, field_type, field_name, default : primary_key = None, has_default = False):
    out = field_name + " SERIAL PRIMARY KEY"
    return {
        "generator": out
    }

  def __str(self, field_type, field_name, default : str = None, has_default = False):
    not_null = " NOT NULL"
    if has_default and default is None:
      not_null = ""
    out = field_name + " TEXT" + not_null
    if default is not None:
      out += f" DEFAULT '{default}'"
    return {
        "generator": out
    }

  def __int(self, field_type, field_name, default : int = None, has_default = False):
    not_null = " NOT NULL"
    if has_default and default is None:
      not_null = ""
    out = field_name + " INTEGER" + not_null
    if default is not None:
      out += f" DEFAULT {default}"
    return {
        "generator": out
    }

  def __float(self, field_type, field_name, default : float = None, has_default = False):
    not_null = " NOT NULL"
    if has_default and default is None:
      not_null = ""
    out = field_name + " REAL" + not_null
    if default is not None:
      out += f" DEFAULT {default}"
    return {
        "generator": out
    }

  def __bool(self, field_type, field_name, default : bool = None, has_default = False):
    not_null = " NOT NULL"
    if has_default and default is None:
      not_null = ""
    out = field_name + " BOOLEAN" + not_null
    if default is not None:
      out += f" DEFAULT {default}"
    return {
        "generator": out
    }

  def __literal(self, field_type, field_name, default : str = None, has_default = False):
    options = field_type.__args__
    max_var_len = max([len(i) for i in options])
    out = field_name + f" VARCHAR({max_var_len}) NOT NULL"
    if default is not None:
      out += f" DEFAULT '{default}'"
    constraint = f"CONSTRAINT literal_contstraint_{field_name} CHECK ({field_name} IN ("
    for i in range(len(options)):
      constraint += f"'{options[i]}'"
      if i != len(options) - 1:
        constraint += ", "
    constraint += "))"
    return {
        "generator": out,
        "constraint": constraint
    }

  def __datetime(self, field_type, field_name, default : datetime = None, has_default = False):
    not_null = " NOT NULL"
    if has_default and default is None:
      not_null = ""
    out = field_name + " TIMESTAMP" + not_null
    if default is not None:
      out += f" DEFAULT '{default}'"
    return {
        "generator": out
    }

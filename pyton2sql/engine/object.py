from dataclasses import dataclass, field, fields, _MISSING_TYPE
from typing import List, get_origin, ClassVar, Literal, get_args, Any, Optional, TypeVar
import uuid
from engine.db import DB_Connection
from pypika import Table as PyPykaTable

from json import JSONEncoder
def _default(self, obj):
	return getattr(obj.__class__, "to_json", _default.default)(obj)
_default.default = JSONEncoder().default
JSONEncoder.default = _default

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

class Table:
	def __init__(self, table_name : str, schema : dict, db : DB_Connection = None):
		self.table_name = "public." + table_name
		self.schema = schema
		if db:
			self.set_db(db)
	
	@property
	def db(self) -> DB_Connection:
		db = self.__db
		if db is None:
			raise ValueError("DB not defined")
		return db
	
	def set_db(self, db : DB_Connection):
		if db is None:
			raise ValueError("DB not defined")
		self.__db = db
		self.__db.ensure_table(self.table_name, self.schema)
		self.__table_query = PyPykaTable(self.table_name)
	
	def get_all(self):
		qry = self.db.query.from_(self.__table_query).select(*list(self.schema.keys())).limit(10)
		return self.db.execute(qry)
	
	def insert(self, data : dict):
		qry = self.db.query.into(self.__table_query).insert().columns(*data.keys()).insert(*data.values())
		self.db.execute(qry)

@dataclass(kw_only=True)
class Object:
	__table : ClassVar[Table] = None

	@classmethod
	def set_db(cls, db : DB_Connection):
		cls.__table = Table(cls.__name__.lower(), cls.get_types(), db)

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
	def new(cls, **kwargs):
		out = cls(**kwargs)
		cls.__table.insert(out.to_dict())
		return out
	
	@classmethod
	def all(cls):
		return cls.__table.get_all()
	
	def to_dict(self):
		out = {}
		fields = { k: v["type"] for k, v in self.__class__.get_types().items() }
		for i in fields:
			if i == '_Object__objects':
				continue
			tpe = fields[i]
			ar = None
			if (type(tpe) != type):
				ar = get_args(tpe)[0]
				tpe = get_origin(tpe)
			if (tpe == list):
				lis = []
				if issubclass(ar, Object):
					for j in getattr(self, i):
						lis.append(j.to_dict())
				else:
					lis.append(j)
				out[i] = lis
			else:
				if issubclass(tpe, Object):
					out[i] = getattr(self, i).to_dict()
				else:
					out[i] = getattr(self, i)
		return out
	
	def to_json(self):
		return self.to_dict()

  
	def __getitem__(self, item):
		return self.to_dict().get(item)

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
	print("returning", out)
	return out
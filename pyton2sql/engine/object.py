from dataclasses import dataclass, field, fields, _MISSING_TYPE
from typing import Callable, List, get_origin, ClassVar, Literal, get_args, Any, Optional, TypeVar
import uuid
from engine.db import DB_Connection
import polars as pl
from engine.lambda_conversion import lambda_to_sql
import builtins

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
		self.__table_alias = "".join(self.table_name.split('.'))
	
	def get_all(self, limit=10):
		qry = self.db.query.select(
			from_table=f'{self.table_name} {self.__table_alias}',
			columns=[f'{self.__table_alias}.{i}' for i in list(self.schema.keys())],
			where='TRUE',
			limit=limit
		)
		return self.db.execute(qry)
	
	def filter(self, l_func, limit=10):
		where_st = lambda_to_sql(self.schema, l_func, self.__db.data_transformer, alias=self.__table_alias)
		qry = self.db.query.select(
			from_table=f'{self.table_name} {self.__table_alias}',
			columns=[f'{self.__table_alias}.{i}' for i in list(self.schema.keys())],
			where=where_st,
			limit=limit
		)
		return self.db.execute(qry)
	
	def insert(self, data : dict):
		qry = self.db.query.insert(self.table_name, [data])
		self.db.execute(qry)

@dataclass(kw_only=True)
class Object:
	__table : ClassVar[Table] = None
	identifier_field : ClassVar[str] = "id"
	__objects_list : ClassVar[List] = []
	__objects_map : ClassVar[dict] = {}

	@classmethod
	def set_db(cls, db : DB_Connection):
		schema = cls.get_types()
		dependecies = {}
		for k, i in schema.items():
			if 'depends' in i:
				table_name = f"{cls.__name__.lower()}__{k}__{i['depends'].__name__.lower()}"
				dependecies[k] = {
					'is_list': get_origin(i['type']) == list,
					'type': i['depends'],
					'table': Table(
						table_name,
						{
							"connection_id": {
								"type": primary_key
							},
							"first_id": {
								"type": primary_key
							},
							"second_id": {
								"type": primary_key
							}
						},
						db
					)
				}
		for i in dependecies:
			del schema[i]
		cls.__table = Table(cls.__name__.lower(), schema, db)
		cls.__dependecies = dependecies

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
			
			ar = fields[i].type
			if get_origin(ar) == list:
				ar = get_args(ar)[0]
			if issubclass(ar, Object):
				type_out["depends"] = ar
			out[i] = type_out
		return out
	
	@classmethod
	def __update_individual(cls, data):
		found = cls.__objects_map.get(data[cls.identifier_field])
		if found is not None:
			found.__dict__.update(data)
			return found
		obj = cls(**data)
		cls.__objects_list.append(obj)
		cls.__objects_map[obj[cls.identifier_field]] = obj
		return obj
	
	@classmethod
	def __update_info(cls, filter = None, limit=10):
		if filter is None:
			res = cls.__table.get_all()
		else:
			res = cls.__table.filter(filter, limit)
		obj_lis = res.to_dicts()
		id_list = res[cls.identifier_field].to_list()
		
		table_results = {}
		dep_tables_required_ids = {}
		for k, v in cls.__dependecies.items():
			table_results[k] = v['table'].filter(lambda x: x.first_id in id_list)
			ids = table_results[k]['second_id']
			t_name = v['type'].__name__
			if t_name not in dep_tables_required_ids:
				dep_tables_required_ids[t_name] = {"type": v['type'], "ids": ids}
			else:
				dep_tables_required_ids[t_name] = dep_tables_required_ids[t_name]["ids"].list.set_union(ids)
		
		dep_tables = {}
		for k, v in dep_tables_required_ids.items():
			l = v['ids'].to_list()
			id_field = v['type'].identifier_field
			res = v['type'].filter(lambda x: x[id_field] in l)
			dep_tables[k] = {}
			for obj in res:
				dep_tables[k][getattr(obj, v['type'].identifier_field)] = obj

		out = []
		for obj in obj_lis:
			for key in cls.__dependecies:
				df = table_results[key]
				lis = df.filter(df['first_id'] == obj[cls.identifier_field])['second_id'].to_list()
				t_name = cls.__dependecies[key]["type"].__name__
				obj[key] = [dep_tables[t_name].get(row) for row in lis]
			out.append(cls.__update_individual(obj))
		return out

	@classmethod
	def new(cls, **kwargs):
		out = cls(**kwargs)
		data = out.to_dict()
		for i in cls.__dependecies:
			del data[i]
		cls.__table.insert(data)
		for field, v in cls.__dependecies.items():
			if v['is_list']:
				new_rows = []
				for i in getattr(out, field):
					new_rows.append({
						'connection_id': primary_key.generate(),
						"first_id": out[cls.identifier_field],
						"second_id": i[v['type'].identifier_field]
					})
				for j in new_rows:
					v['table'].insert(j)
			else:
				v['table'].insert({
					'connection_id': primary_key.generate(),
					"first_id": out[cls.identifier_field],
					"second_id": getattr(out, field)[v['type'].identifier_field]
				})
		return out
	
	def update(self, **kwargs):
		for key, value in kwargs.items():
			setattr(self, key, value)
		## WIP modify table on update

	@classmethod
	def all(cls):
		out = cls.__update_info()
		return out
	
	@classmethod
	def filter(cls, lambda_f):
		out = cls.__update_info(filter=lambda_f)
		return out
	
	@classmethod
	def get_by_id(cls, id_param):
		cls.__update_info(lambda x: x[cls.identifier_field] == id_param, limit=1)
		return cls.__objects_map.get(id_param)
	
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
	
	def __repr__(self):
		schema = self.__class__.get_types()
		out = self.__class__.__name__ + ' (\n'
		for key in schema:
			v = getattr(self, key)
			r = repr(v)
			if type(v) == list:
				lis = ',\n'.join([f'   {repr(j).replace('\n', '\n   ')}' for j in v])
				r = f'[\n{lis}\n]'
			r = r.replace('\n', '\n   ')
			out += f"   {key}:   {r}\n"
		out += ')'
		return out
  
	def __getitem__(self, item):
		return self.to_dict().get(item)

T = TypeVar('T', bound=Object)
def persistance(cls : type) -> T:
	original_repr = cls.__repr__
	out = dataclass(cls, kw_only=True)
	if not issubclass(cls, Object):
		raise ValueError(f"{cls.__name__} class should be subclass of 'Object'")
	schema = out.__dataclass_fields__
	has_one_primary_key = [i.type == primary_key for i in schema.values()].count(True) == 1
	if not has_one_primary_key:
		raise ValueError("one primary key should be defined")
	primary_key_field = list(schema.keys())[[i.type for i in schema.values()].index(primary_key)]
	if isinstance(schema[primary_key_field].default_factory, _MISSING_TYPE):
		schema[primary_key_field] = field(
			default_factory=lambda: primary_key.generate()
		)
	out.identifier_field = primary_key_field
	out.__repr__ = original_repr
	out : Object = out
	print("returning", out)
	return out
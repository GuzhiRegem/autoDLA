from dataclasses import dataclass, field, fields, _MISSING_TYPE
from datetime import datetime
from typing import List, get_origin, ClassVar, Literal, get_args, TypeVar
import uuid
import polars as pl
from ..engine.db import DB_Connection
from ..engine.lambda_conversion import lambda_to_sql

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
	
	def __eq__(self, value):
		if isinstance(value, str):
			return super().__eq__(value)
		if isinstance(value, uuid.UUID):
			return uuid.UUID(self) == value

def dla_dict(operation : Literal["INSERT", "UPDATE", "DELETE"], modified_at=datetime.now(), modified_by="SYSTEM", is_current=False, is_active=True):
	def out():
		return {
			'DLA_object_id': primary_key.generate(),
			'DLA_modified_at': modified_at,
			'DLA_operation': operation,
			'DLA_modified_by': modified_by,
			'DLA_is_current': is_current,
			'DLA_is_active': is_active
		}
	return out

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
	
	def get_all(self, limit=10, only_current=True, only_active=True):
		conditions = ["TRUE"]
		if only_current:
			conditions.append("DLA_is_current = true")
		if only_active:
			conditions.append("DLA_is_active = true")
		where_st = " AND ".join(conditions)
		qry = self.db.query.select(
			from_table=f'{self.table_name} {self.__table_alias}',
			columns=[f'{self.__table_alias}.{i}' for i in list(self.schema.keys())],
			where=where_st,
			limit=limit
		)
		return self.db.execute(qry)
	
	def filter(self, l_func, limit=10, only_current=True, only_active=True):
		conditions = [lambda_to_sql(self.schema, l_func, self.__db.data_transformer, alias=self.__table_alias)]
		if only_current:
			conditions.append("DLA_is_current = true")
		if only_active:
			conditions.append("DLA_is_active = true")
		where_st = " AND ".join(conditions)
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
	
	def update(self, l_func, data):
		where_st = lambda_to_sql(self.schema, l_func, self.__db.data_transformer, alias=self.__table_alias)
		update_data = {f'{key}': value for key, value in data.items()}
		qry = self.db.query.update(
			f'{self.table_name} {self.__table_alias}',
			where=where_st,
			values=update_data
		)
		return self.db.execute(qry)

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
		common_fields = {
			'DLA_object_id': {
				"type": uuid.UUID
			},
			'DLA_modified_at': {
				"type": datetime
			},
			'DLA_operation': {
				"type": str
			},
			'DLA_modified_by': {
				"type": str
			},
			'DLA_is_current': {
				"type": bool
			},
			'DLA_is_active': {
				'type': bool
			}
		}
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
							},
							"list_index": {
								"type": int
							}
							,**common_fields
						},
						db
					)
				}
		for i in dependecies:
			del schema[i]
		cls.__table = Table(cls.__name__.lower(), {**schema,**common_fields}, db)
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
	def __update_individual(cls, data_inp):
		data = {}
		for k, v in data_inp.items():
			if not k.upper().startswith("DLA_"):
				data[k] = v
		found = cls.__objects_map.get(data[cls.identifier_field])
		if found is not None:
			found.__dict__.update(data)
			return found
		obj = cls(**data)
		cls.__objects_list.append(obj)
		cls.__objects_map[obj[cls.identifier_field]] = obj
		return obj
	
	@classmethod
	def __update_info(cls, filter = None, limit=10, only_current=True, only_active=True):
		if filter is None:
			res = cls.__table.get_all(limit, only_current, only_active)
		else:
			res = cls.__table.filter(filter, limit, only_current, only_active)
		obj_lis = res.to_dicts()
		id_list = res[cls.identifier_field].to_list()
		
		table_results = {}
		dep_tables_required_ids = {}
		for k, v in cls.__dependecies.items():
			table_results[k] = v['table'].filter(lambda x: x.first_id in id_list, None, only_current=only_current, only_active=only_active)
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
				if not cls.__dependecies[key]['is_list']:
					obj[key] = obj[key][0]
			out.append(cls.__update_individual(obj))
		return out

	@classmethod
	def new(cls, **kwargs):
		out = cls(**kwargs)
		data = out.to_dict()
		for i in cls.__dependecies:
			del data[i]
		dla_data = dla_dict("INSERT", is_current=True)
		cls.__table.insert({**data, **dla_data()})
		for field, v in cls.__dependecies.items():
			if v['is_list']:
				new_rows = []
				for idx, i in enumerate(getattr(out, field)):
					new_rows.append({
						'connection_id': primary_key.generate(),
						"first_id": out[cls.identifier_field],
						"second_id": i[v['type'].identifier_field],
						"list_index": idx,
						**dla_data()
					})
				for j in new_rows:
					v['table'].insert(j)
			else:
				v['table'].insert({
					'connection_id': primary_key.generate(),
					"first_id": out[cls.identifier_field],
					"second_id": getattr(out, field)[v['type'].identifier_field],
					"list_index": 0,
					**dla_data()
				})
		cls.__objects_map[str(out[cls.identifier_field])] = out
		cls.__objects_list.append(out)
		return out
	
	def history(self):
		self_res = self.__table.filter(lambda x: x[self.identifier_field] == getattr(self, self.identifier_field), limit=None, only_active=False, only_current=False)
		out = {
			"self": self_res,
			"dependecies": {}
		}
		for k, v in self.__dependecies.items():
			dep_res = v['table'].filter(lambda x: x.first_id == getattr(self, self.identifier_field), limit=None, only_active=False, only_current=False)
			out['dependecies'][k] = dep_res
		return out
	
	def update(self, **kwargs):
		data = {**self.to_dict(), **kwargs}
		dla_data_insert = dla_dict("UPDATE", is_current=True)
		for key, value in kwargs.items():
			if key in self.__dependecies:
				del data[key]
				dependency = self.__dependecies[key]
				dependency['table'].update(lambda x: x.first_id == self.id, {'DLA_is_current': False})
				new_rows = []
				if dependency['is_list']:
					for idx, i in enumerate(value):
						new_rows.append({
							'connection_id': primary_key.generate(),
							"first_id": self[self.identifier_field],
							"second_id": i[dependency['type'].identifier_field],
							"list_index": idx,
							**dla_data_insert()
						})
				else:
					new_rows.append({
						'connection_id': primary_key.generate(),
						"first_id": self[self.identifier_field],
						"second_id": value[dependency['type'].identifier_field],
						"list_index": 0,
						**dla_data_insert()
					})
				for j in new_rows:
					dependency['table'].insert(j)
			setattr(self, key, value)
		self.__table.update(lambda x: x[self.identifier_field] == self.id, {'DLA_is_current': False})
		self.__table.insert({**data, **dla_data_insert()})
	
	def delete(self):
		data = {**self.to_dict()}
		dla_data_delete = dla_dict("DELETE", is_current=True, is_active=False)
		for key, dependency in self.__dependecies.items():
			del data[key]
			dependency['table'].update(lambda x: x.first_id == self.id, {'DLA_is_current': False})
			value = getattr(self, key)
			new_rows = []
			if dependency['is_list']:
				for idx, i in enumerate(value):
					new_rows.append({
						'connection_id': primary_key.generate(),
						"first_id": self[self.identifier_field],
						"second_id": i[dependency['type'].identifier_field],
						"list_index": idx,
						**dla_data_delete()
					})
			else:
				new_rows.append({
					'connection_id': primary_key.generate(),
					"first_id": self[self.identifier_field],
					"second_id": value[dependency['type'].identifier_field],
					"list_index": 0,
					**dla_data_delete()
				})
			for j in new_rows:
				dependency['table'].insert(j)
		self.__table.update(lambda x: x[self.identifier_field] == self.id, {'DLA_is_current': False})
		self.__table.insert({**data, **dla_data_delete()})



	@classmethod
	def all(cls, limit=10):
		out = cls.__update_info(limit=limit)
		return out
	
	@classmethod
	def filter(cls, lambda_f, limit=10):
		out = cls.__update_info(filter=lambda_f, limit=limit)
		return out
	
	@classmethod
	def get_by_id(cls, id_param):
		cls.__update_info(lambda x: x[cls.identifier_field] == id_param, limit=1)
		return cls.__objects_map.get(id_param)
	
	@classmethod
	def get_table_res(cls, limit=10, only_current=True, only_active=True) -> pl.DataFrame:
		return cls.__table.get_all(limit=limit, only_current=only_current, only_active=only_active)
	
	def to_dict(self):
		out = {}
		fields = { k: v["type"] for k, v in self.__class__.get_types().items() }
		for i in fields:
			val = getattr(self, i)
			tpe = fields[i]
			ar = None
			if (type(tpe) != type):
				ar = get_args(tpe)[0]
				tpe = get_origin(tpe)
			if (tpe == list):
				lis = []
				if issubclass(ar, Object):
					for j in val:
						lis.append(j.to_dict())
				else:
					lis.append(j)
				out[i] = lis
			else:
				if issubclass(tpe, Object):
					out[i] = val.to_dict()
				else:
					out[i] = val
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
	return out
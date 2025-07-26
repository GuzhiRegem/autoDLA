from __future__ import annotations
from json import JSONEncoder
from dataclasses import field, _MISSING_TYPE
from datetime import datetime
from types import NoneType
from typing import (
    Any,
    Callable,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
    get_origin,
    ClassVar,
    Literal,
    get_args
)
import uuid
import polars as pl
from autodla.engine.lambda_conversion import lambda_to_sql
from pydantic import GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema, PydanticUndefinedType
from autodla.utils.logger import logger
from autodla.engine.interfaces import (
    DB_Connection_Interface,
    Object_Interface,
    Table_Interface,
    primary_key_Interface
)
import warnings
warnings.filterwarnings('error')


def _default(self, obj):
    return getattr(obj.__class__, "to_json", _default.default)(obj)


_default.default = JSONEncoder().default  # type: ignore
JSONEncoder.default = _default  # type: ignore

primary_key_type = TypeVar('primary_key_type', bound='primary_key')


class primary_key(primary_key_Interface):
    @classmethod
    def generate(cls: Type[primary_key_type]) -> primary_key_type:
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

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_after_validator_function(cls, handler(str))

    def __hash__(self):
        return super().__hash__()


def dla_dict(
        operation: Literal["INSERT", "UPDATE", "DELETE"],
        modified_at: datetime = datetime.now(),
        modified_by: str = "SYSTEM",
        is_current: bool = False,
        is_active: bool = True
):
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


class Table(Table_Interface):
    def __init__(
            self,
            table_name: str,
            schema: dict,
            db: Optional[DB_Connection_Interface | None] = None
    ):
        self.schema = schema
        if db:
            table_name_res = db.get_table_name(table_name)
            self.table_name = table_name
            self.__table_alias = table_name_res.alias
            self.set_db(db)

    @property
    def db(self) -> DB_Connection_Interface:
        db = self.__db
        if db is None:
            raise ValueError("DB not defined")
        return db

    def set_db(self, db: DB_Connection_Interface) -> None:
        if db is None:
            raise ValueError("DB not defined")
        self.__db = db
        self.__db.ensure_table(self.table_name, self.schema, save=True)

    def get_all(
            self,
            limit: Optional[int] = 10,
            only_current: bool = True,
            only_active: bool = True,
            skip: int = 0
    ) -> Optional[pl.DataFrame]:
        conditions = ["TRUE"]
        if only_current:
            conditions.append("DLA_is_current = true")
        if only_active:
            conditions.append("DLA_is_active = true")
        where_st = " AND ".join(conditions)
        qry = self.db.query.select(
            from_table=''
            + f'{self.__db.get_table_name(self.table_name).name} '
            + f'{self.__table_alias}',
            columns=[f'{self.__table_alias}.{i}' for i in list(
                self.schema.keys())],
            where=where_st,
            limit=limit,
            offset=skip
        )
        return self.db.execute(qry)

    def filter(
            self,
            l_func: Callable,
            limit: Optional[int] = 10,
            only_current: bool = True,
            only_active: bool = True,
            skip: int = 0
    ) -> Optional[pl.DataFrame]:
        conditions = [
            lambda_to_sql(
                self.schema,
                l_func,
                self.__db.data_transformer,
                alias=self.__table_alias
            )
        ]
        if only_current:
            conditions.append("DLA_is_current = true")
        if only_active:
            conditions.append("DLA_is_active = true")
        where_st = " AND ".join(conditions)
        qry = self.db.query.select(
            from_table=f'{
                self.__db.get_table_name(self.table_name).name
            } {
                self.__table_alias
            }',
            columns=[f'{self.__table_alias}.{i}' for i in list(
                self.schema.keys())],
            where=where_st,
            limit=limit,
            offset=skip
        )
        return self.db.execute(qry)

    def insert(self, data: dict) -> None:
        qry = self.db.query.insert(
            self.__db.get_table_name(self.table_name).name, [data])
        self.db.execute(qry)

    def update(self, l_func: Callable, data: dict) -> None:
        where_st = lambda_to_sql(
            self.schema, l_func, self.__db.data_transformer, alias="")
        update_data = {f'{key}': value for key, value in data.items()}
        qry = self.db.query.update(
            f'{self.__db.get_table_name(self.table_name).name}',
            where=where_st,
            values=update_data
        )
        self.db.execute(qry)

    def delete_all(self) -> None:
        qry = self.db.query.delete(
            self.__db.get_table_name(self.table_name).name, "TRUE")
        self.db.execute(qry)


class Object(Object_Interface):
    __table: ClassVar[Optional[Table | None]] = None
    __dependencies: ClassVar[dict[str, Object_Interface.ObjectDependency]] = (
        field(
            default_factory=dict
        )
    )
    identifier_field: ClassVar[str] = "id"
    __objects_list: ClassVar[List['Object']] = list()
    __objects_map: ClassVar[dict[str, 'Object']] = dict()

    @classmethod
    def delete_all(cls) -> None:
        cls.__objects_list = []
        cls.__objects_map = {}
        if cls.__table is not None:
            cls.__table.delete_all()

    @classmethod
    def set_db(cls, db: DB_Connection_Interface) -> None:
        schema = cls.get_types()
        dependencies: dict[str, Object.ObjectDependency] = {}
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
                table_name = '' \
                    + f"{cls.__name__.lower()}"\
                    + f"__{k}__"\
                    + f"{i['depends'].__name__.lower()}"
                dependencies[k] = Object.ObjectDependency(
                    is_list=i.get("is_list") is True,
                    is_value=False,
                    type=i['depends'],
                    table=Table(
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
                            }, **common_fields
                        },
                        db
                    )
                )
            elif 'is_list' in i and i["type"] is not None:
                table_name = f"{cls.__name__.lower()}__{k}"
                dependencies[k] = Object.ObjectDependency(
                    is_list=i.get("is_list") is True,
                    is_value=True,
                    type=i["type"],
                    table=Table(
                        table_name,
                        {
                            "connection_id": {
                                "type": primary_key
                            },
                            "first_id": {
                                "type": primary_key
                            },
                            "value": {
                                "type": i["type"]
                            },
                            "list_index": {
                                "type": int
                            }, **common_fields
                        },
                        db
                    )
                )
        for key in dependencies.keys():
            del schema[key]
        cls.__table = Table(cls.__name__.lower(), {
                            **schema, **common_fields}, db)
        cls.__dependencies = dependencies

    @classmethod
    def get_types(cls) -> dict[str, Object_Interface.TypeDictionary]:
        out = {}
        fields = cls.model_fields
        for i in fields:
            if (get_origin(fields[i].annotation) == ClassVar):
                continue
            type_out: Object_Interface.TypeDictionary = {}
            tp: Optional[Type | None] = fields[i].annotation
            ori, arg = get_origin(tp), get_args(tp)
            if ori == Union:
                if arg[1] == NoneType:
                    type_out["nullable"] = True
                    tp = arg[0]
                    ori, arg = get_origin(tp), get_args(tp)
            if (
                    type_out.get('nullable') is True
                    and fields[i].default is not None):
                raise TypeError(
                    'Field with type Optional must initialize to None')
            if (
                    type_out.get('nullable') is not True
                    and fields[i].default is None):
                raise TypeError(
                    'Field initialized to None must be of type Optional')
            if type(fields[i].default) not in [
                    _MISSING_TYPE,
                    PydanticUndefinedType]:
                type_out["default"] = fields[i].default
            if type(fields[i].default_factory) not in [
                    _MISSING_TYPE,
                    PydanticUndefinedType]:
                type_out["default_factory"] = fields[i].default_factory
            if ori == list:
                tp = arg[0]
                ori, arg = get_origin(tp), get_args(tp)
                type_out["is_list"] = True
            if tp is not None:
                if issubclass(tp, Object):
                    type_out["depends"] = tp
            type_out["type"] = tp
            out[i] = type_out
        return out

    @classmethod
    def _update_individual(
        cls,
        data_inp: dict[str, Any]
    ) -> Optional['Object_Interface']:
        logger.debug(f"UPDATE INDIVIDUAL: {cls} {data_inp}")
        data: dict[str, Any] = {}
        for k, v in data_inp.items():
            if not k.upper().startswith("DLA_"):
                data[k] = v
        found = cls.__objects_map.get(data[cls.identifier_field])
        try:
            cls.model_validate(data)
        except Exception as e:
            logger.error(
                f"Validation error for {cls.__name__} with data {data}: {e}")
            return None
        if found is not None:
            found.__dict__.update(data)
            return found
        obj = cls(**data)
        cls.__objects_list.append(obj)
        cls.__objects_map[obj[cls.identifier_field]] = obj
        return obj

    @classmethod
    def _update_info(
        cls,
        filter: Optional[Callable[[Any], bool]] = None,
        limit: Optional[int] = 10,
        skip: int = 0,
        only_current: bool = True,
        only_active: bool = True
    ) -> list['Object_Interface']:
        if cls.__table is None:
            return []
        if filter is None:
            res = cls.__table.get_all(
                limit, only_current, only_active, skip=skip)
        else:
            res = cls.__table.filter(
                filter, limit, only_current, only_active, skip=skip)
        obj_lis: list[dict[str, Any]] = []
        if res is not None:
            obj_lis = res.to_dicts()
        if obj_lis == []:
            return []
        if res is None:
            return []
        id_list = res[cls.identifier_field].to_list()

        table_results: dict[str, Optional[pl.DataFrame]] = {}
        dep_tables_required_ids: dict[
            str, Object_Interface.DependencyRequiredIds] = {}
        for k, v in cls.__dependencies.items():
            if v.is_value:
                continue
            dep_table_: Table_Interface = v.table
            table_results[k] = dep_table_.filter(
                lambda x: x.first_id in id_list,
                None, only_current=only_current,
                only_active=only_active
            )
            ids: set[str] = set()
            df: Optional[pl.DataFrame] = table_results[k]
            if df is not None:
                ids = set(df['second_id'].to_list())
            t_name: str = v.type.__class__.__name__
            if t_name not in dep_tables_required_ids:
                tp: 'Type[Object_Interface]' = v.type
                dep_tables_required_ids[t_name] = (
                    Object_Interface.DependencyRequiredIds(
                        type=tp, ids=ids
                    )
                )
            else:
                row = dep_tables_required_ids[t_name]
                row.ids = row.ids.union(ids)

        dep_tables: dict[str, dict[str, "Object_Interface"]] = {}
        for k_, v_ in dep_tables_required_ids.items():
            l: list[str] = list(v_.ids)
            tp_: 'Type[Object_Interface]' = v_.type
            id_field = tp_.identifier_field
            dep_tables[k_] = {}
            if len(l) == 0:
                continue
            filter_res = tp_.filter(lambda x: x[id_field] in l)
            for obj in filter_res:
                dep_tables[k_][getattr(obj, tp_.identifier_field)] = obj

        out = []
        for obj_dic in obj_lis:
            dep_key: str
            for dep_key in cls.__dependencies.keys():
                dep: Object.ObjectDependency = cls.__dependencies[dep_key]
                if dep.is_value:
                    obj_id: primary_key = obj_dic[cls.identifier_field]
                    dep_table: Table_Interface = dep.table
                    table_df: Optional[pl.DataFrame] = dep_table.filter(
                        lambda x: x.first_id == obj_id
                    )
                    table_res: list[Any] = []
                    if table_df is not None:
                        table_res = table_df['value'].to_list()
                    obj_dic[dep_key] = table_res
                    continue
                df = table_results[dep_key]
                val_lis = []
                t_name = dep.type.__class__.__name__
                if df is not None and len(df) > 0:
                    lis = df.filter(
                        df['first_id'] == obj[cls.identifier_field]
                    )[
                        'second_id'].to_list()
                    for row in lis:
                        val = dep_tables[t_name].get(str(row))
                        if val is not None:
                            val_lis.append(val)
                obj_dic[dep_key] = val_lis
                if not cls.__dependencies[dep_key].is_list:
                    if obj[dep_key] != []:
                        obj_dic[dep_key] = obj[dep_key][0]
                    else:
                        obj_dic[dep_key] = None
            update_obj: Optional['Object_Interface'] = cls._update_individual(
                obj_dic)
            if update_obj is not None:
                out.append(update_obj)
        return out

    @classmethod
    def new(cls, **kwargs) -> 'Object':
        if cls.__table is None:
            raise ImportError('DB not defined')
        if cls.identifier_field in kwargs:
            del kwargs[cls.identifier_field]
        out = cls(**kwargs)
        data = out.to_dict()
        for i in cls.__dependencies:
            del data[i]
        dla_data = dla_dict("INSERT", is_current=True)
        cls.__table.insert({**data, **dla_data()})
        for field_i, v in cls.__dependencies.items():
            if v.is_list:
                new_rows = []
                if v.is_value:
                    for idx, i in enumerate(getattr(out, field_i)):
                        new_rows.append({
                            'connection_id': primary_key.generate(),
                            "first_id": out[cls.identifier_field],
                            "value": i,
                            "list_index": idx,
                            **dla_data()
                        })
                else:
                    for idx, i in enumerate(getattr(out, field_i)):
                        new_rows.append({
                            'connection_id': primary_key.generate(),
                            "first_id": out[cls.identifier_field],
                            "second_id": getattr(i, v.type.identifier_field),
                            "list_index": idx,
                            **dla_data()
                        })
                for j in new_rows:
                    v.table.insert(j)
            else:
                val = getattr(out, field_i)
                if val is not None:
                    v.table.insert({
                        'connection_id': primary_key.generate(),
                        "first_id": out[cls.identifier_field],
                        "second_id": val[v.type.identifier_field],
                        "list_index": 0,
                        **dla_data()
                    })
        cls.__objects_map[str(out[cls.identifier_field])] = out
        cls.__objects_list.append(out)
        return out

    def history(self) -> dict[str, list[dict[str, Any]]]:
        if self.__table is None:
            return {}
        self_res = self.__table.filter(
            lambda x: x[self.identifier_field] == getattr(
                self, self.identifier_field
            ),
            limit=None,
            only_active=False,
            only_current=False
        )
        out: dict[str, list[dict[str, Any]]] = {
            "self": self_res.to_dicts() if self_res is not None else [],
        }
        for k, v in self.__dependencies.items():
            dep_res = v.table.filter(lambda x: x.first_id == getattr(
                self, self.identifier_field), limit=None, only_active=False,
                only_current=False)
            out[k] = dep_res.to_dicts() if dep_res is not None else []
        return out

    def update(self, **kwargs) -> None:
        if self.__table is None:
            return
        data = {}
        for key in self.to_dict():
            if key in kwargs:
                data[key] = kwargs[key]
            elif key not in self.__class__.__dependencies:
                data[key] = getattr(self, key)
        dla_data_insert = dla_dict("UPDATE", is_current=True)
        for key, value in kwargs.items():
            if key in self.__dependencies:
                del data[key]
                dependency = self.__dependencies[key]
                dependency.table.update(
                    lambda x: x.first_id == self[self.identifier_field], {
                        'DLA_is_current': False})
                new_rows = []
                if dependency.is_list:
                    if dependency.is_value:
                        for idx, i in enumerate(value):
                            new_rows.append({
                                'connection_id': primary_key.generate(),
                                "first_id": self[self.identifier_field],
                                "value": i,
                                "list_index": idx,
                                **dla_data_insert()
                            })
                    else:
                        for idx, i in enumerate(value):
                            new_rows.append({
                                'connection_id': primary_key.generate(),
                                "first_id": self[self.identifier_field],
                                "second_id": i[
                                    dependency.type.identifier_field],
                                "list_index": idx,
                                **dla_data_insert()
                            })
                else:
                    if value is not None:
                        new_rows.append({
                            'connection_id': primary_key.generate(),
                            "first_id": self[self.identifier_field],
                            "second_id": value[
                                dependency.type.identifier_field],
                            "list_index": 0,
                            **dla_data_insert()
                        })
                for j in new_rows:
                    dependency.table.insert(j)
            setattr(self, key, value)
        self.__table.update(lambda x: x[self.identifier_field] == self[
            self.identifier_field], {
            'DLA_is_current': False})
        self.__table.insert({**data, **dla_data_insert()})

    def delete(self) -> None:
        if self.__table is None:
            return
        data = {}
        for key in self.__class__.model_fields:
            data[key] = getattr(self, key)
        dla_data_delete = dla_dict("DELETE", is_current=True, is_active=False)
        for key, dependency in self.__dependencies.items():
            del data[key]
            dependency.table.update(lambda x: x.first_id == self[
                self.identifier_field], {'DLA_is_current': False})
            value = getattr(self, key)
            if value is None:
                continue
            new_rows = []
            if dependency.is_list:
                if dependency.is_value:
                    for idx, i in enumerate(value):
                        new_rows.append({
                            'connection_id': primary_key.generate(),
                            "first_id": self[self.identifier_field],
                            "value": i,
                            "list_index": idx,
                            **dla_data_delete()
                        })
                else:
                    for idx, i in enumerate(value):
                        new_rows.append({
                            'connection_id': primary_key.generate(),
                            "first_id": self[self.identifier_field],
                            "second_id": i[dependency.type.identifier_field],
                            "list_index": idx,
                            **dla_data_delete()
                        })
            else:
                if value is not None:
                    new_rows.append({
                        'connection_id': primary_key.generate(),
                        "first_id": self[self.identifier_field],
                        "second_id": value[dependency.type.identifier_field],
                        "list_index": 0,
                        **dla_data_delete()
                    })
            for j in new_rows:
                dependency.table.insert(j)
        self.__table.update(lambda x: x[self.identifier_field] == self[
            self.identifier_field], {
            'DLA_is_current': False})
        self.__table.insert({**data, **dla_data_delete()})

    @classmethod
    def all(cls, limit=10, skip=0) -> list[Object_Interface]:
        out = cls._update_info(limit=limit, skip=skip)
        return out

    @classmethod
    def filter(cls, lambda_f, limit=10, skip=0) -> list[Object_Interface]:
        out = cls._update_info(filter=lambda_f, limit=limit, skip=skip)
        return out

    @classmethod
    def get_by_id(cls, id_param) -> Optional["Object"]:
        cls._update_info(
            lambda x: x[cls.identifier_field] == id_param, limit=1, skip=0)
        return cls.__objects_map.get(id_param)

    @classmethod
    def get_table_res(
        cls, limit=10, skip=0, only_current=True,
        only_active=True
    ) -> Optional[pl.DataFrame]:
        if cls.__table is None:
            return None
        return cls.__table.get_all(
            limit=limit, only_current=only_current,
            only_active=only_active, skip=skip)

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()

    def to_json(self) -> str:
        return self.model_dump_json()

    def __getitem__(self, item: str) -> Any:
        return getattr(self, item)

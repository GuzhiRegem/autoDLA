import polars as pl
from typing import Callable, Optional, Type, get_origin, get_args
from autodla.utils.logger import logger

from autodla.engine.interfaces import (
    DB_Connection_Interface,
    TableName,
    Object_Interface,
    QueryBuilder_Interface,
    DataTransformer_Interface,
    TableField
)


class DB_Connection(DB_Connection_Interface):
    __data_transformer: DataTransformer_Interface
    __query: QueryBuilder_Interface
    __classes: dict[str, Type[Object_Interface]] = {}
    _table_schemas: dict[str, dict[str, Type]] = {}
    _tables: list[str] = []

    @property
    def usage_metrics(self) -> dict[str, int]:
        """
        Returns the usage metrics of the database connection.
        This is a placeholder method and should be implemented in subclasses.
        """
        raise NotImplementedError(
            "This method should be implemented in subclasses."
        )

    def exit(self) -> None:
        pass

    def __init__(
        self,
        data_transformer: DataTransformer_Interface,
        query: QueryBuilder_Interface
    ) -> None:
        self.__data_transformer = data_transformer
        self.__query = query
        self.__classes: dict[str, Type[Object_Interface]] = {}
        self._table_schemas: dict[str, dict[str, Type]] = {}
        self._tables: list[str] = []

    @property
    def query(self) -> QueryBuilder_Interface:
        return self.__query

    @property
    def data_transformer(self) -> DataTransformer_Interface:
        return self.__data_transformer

    def get_table_name(
        self,
        table_name: str
    ) -> TableName:
        """
        Returns the table name in uppercase.
        This is used to ensure that the table names
        are consistent with the SQL standard.
        """
        return TableName(name=table_name.upper(), alias=table_name.lower())

    def clean_db(
        self,
        DO_NOT_ASK: bool = False
    ) -> None:
        if not DO_NOT_ASK:
            logger.debug("Are you sure you want to clean the database? (y/n)")
            answer = input()
            if answer != "y":
                raise Exception("User did not confirm the action")
        logger.debug("Cleaning database...")
        for class_i in self.__classes.values():
            class_i.delete_all()
        logger.debug("Database cleaned")

    def get_table_definition(
        self,
        table_name: str
    ) -> dict[str, type]:
        return {}

    def attach(
        self,
        objects: list[Type[Object_Interface]]
    ) -> None:
        ordered_objects: list[Type[Object_Interface]] = []
        pending = objects[:]
        while True:
            if pending == []:
                break
            tmp = pending[:]
            for obj in tmp:
                schema = obj.get_types()
                class_dependencies = []
                for i in schema.values():
                    if 'depends' in i:
                        class_dependencies.append(i.get('depends'))
                if all([dep in ordered_objects for dep in class_dependencies]):
                    ordered_objects.append(obj)
                    pending.remove(obj)
        for obj in ordered_objects:
            self.__classes[obj.__name__] = obj
            obj.set_db(self)

    def get_json_schema(self) -> dict[str, dict[str, TableField]]:
        out: dict[str, dict[str, TableField]] = {}
        for class_key, class_i in self.__classes.items():
            class_def = class_i.get_types()
            class_out: dict[str, TableField] = {}
            for k, f in class_def.items():
                if f["type"] is None:
                    continue
                type_st = f["type"].__name__
                if get_origin(f['type']) == list:
                    arg = get_args(f["type"])
                    if len(arg) == 1:
                        type_st += f'[{arg[0].__name__}]'
                class_out[k] = {
                    "type": type_st
                }
                if class_i.identifier_field == k:
                    class_out[k]["primary_key"] = True
                if "depends" in f:
                    class_out[k]["depends"] = f'$ref:{f["depends"].__name__}'
                if "is_list" in f:
                    class_out[k]["is_list"] = f["is_list"]
                if "nullable" in f:
                    class_out[k]["nullable"] = f["nullable"]
            out[class_key] = class_out
        return out

    @property
    def classes(self) -> list[Type["Object_Interface"]]:
        return list(self.__classes.values())

    def execute(
        self,
        query: str
    ) -> Optional[pl.DataFrame]:
        return pl.DataFrame()

    def normalize_statement(
        self,
        statement: str
    ) -> str:
        if not isinstance(statement, str):
            statement = str(statement)
        statement = statement.lstrip().rstrip()
        if statement[-1] != ";":
            statement += ";"
        return statement

    def ensure_table(
        self,
        table_name: str,
        schema: dict[str, Type],
        save: bool = False,
        current_data_schema: Optional[dict[str, Type]] = None,
        execute_function: Optional[
            Callable[[str], Optional[pl.DataFrame]]] = None,
        query_builder: Optional[QueryBuilder_Interface] = None,
        data_transformer: Optional[DataTransformer_Interface] = None,
        change_name=True
    ) -> None:
        if execute_function is None:
            execute_function = self.execute
        if query_builder is None:
            query_builder = self.query
        if data_transformer is None:
            data_transformer = self.data_transformer
        logger.debug(f"ENSURE TABLE {self.__class__.__name__} {table_name}")
        if save:
            self._table_schemas[table_name] = schema
            self._tables.append(table_name)
        data_schema = {k.upper(): v["type"] for k, v in schema.items()}
        if current_data_schema is None:
            current_data_schema = self.get_table_definition(table_name)
        if all([
            data_transformer.check_type_compatibility(
                data_schema.get(k), current_data_schema.get(k)
            ) for k in list(
                set(data_schema.keys()).union(set(data_schema.keys()))
            )
        ]):
            return
        logger.debug(data_schema)
        logger.debug(current_data_schema)
        if data_schema == current_data_schema:
            return
        converted_schema = data_transformer.convert_data_schema(schema)
        table_name_db = (
            self.get_table_name(table_name).name
            if change_name else table_name
        )
        execute_function(
            query_builder.drop_table(table_name_db, if_exists=True))
        qry = query_builder.create_table(table_name_db, converted_schema)
        execute_function(qry)
        updated_data_schema = self.get_table_definition(table_name)
        key: str  # type annotation for key
        for key in set(
            *[list(updated_data_schema.keys()) + list(data_schema.keys())]
        ):
            if key not in updated_data_schema or key not in data_schema:
                raise ValueError("DATA SCHEMA NOT UPDATED")

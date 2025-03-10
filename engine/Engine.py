
import polars as pl
from dataclasses import dataclass, field, fields, _MISSING_TYPE
import os
import uuid
from typing import List, get_origin, ClassVar, Literal, get_args
from datetime import datetime
import json

from engine.utils.custom_types import primary_key
from engine.utils.DataConvertor import DataConvertor
from engine.Object import Object

import psycopg2

CONNECTION_URL = "postgresql://my_user:password@localhost/my_db"



class Engine:
    def __init__(self, classes):
        self.__classes = classes
        self.__db_connection = psycopg2.connect(CONNECTION_URL)
        self.__data_convertor = DataConvertor()
        self.__cache = {}
        self.__initialize()

    def generate_statement_create_table(self, obj):
        statement = f"CREATE TABLE IF NOT EXISTS {obj.get_table_name()} (\n\t"
        fields_texts = []
        constraints = []
        schema = obj.get_types()
        for i in schema:
          generated_field = self.__data_convertor.generate_field_text(
            schema[i]["type"],
            i,
            schema[i].get("default"),
            'default' in schema[i]
          )
          if "generator" in generated_field:
            fields_texts.append(generated_field['generator'])
          if "constraint" in generated_field:
            constraints.append(generated_field['constraint'])
        statement += ",\n\t".join(fields_texts + constraints)
        statement += "\n);"
        return statement

    def execute_statement(self, statement, commit=False):
        if statement[-1] != ';':
            statement += ';'
        with self.__db_connection.cursor() as cursor:
            cursor.execute(statement)
            if commit:
                self.__db_connection.commit()
            try:
                rows = cursor.fetchall()
                schema = [desc[0] for desc in cursor.description]
                return pl.DataFrame(rows, schema=schema)
            except:
                return None
            

    def generate_statement_create_list_table(self, obj, field):
        list_type = get_args(obj.get_types()[field]['type'])[0]
        is_object = issubclass(list_type, Object)
        statement = [
            f"CREATE TABLE IF NOT EXISTS {obj.get_table_name()}__list__{field} (\n\t",
            "id SERIAL PRIMARY KEY,\n\t",
            f"parent_id INTEGER REFERENCES {obj.get_table_name()}(id),\n\t",
            "index INTEGER NOT NULL,\n\t"
        ]
        if is_object:
            statement.append(f"object_id INTEGER REFERENCES {list_type.get_table_name()}(id)")
        else:
            statement.append(self.__data_convertor.generate_field_text(list_type,"value")['generator'])
        statement.append("\n);")
        statement = "".join(statement)
        return statement

    def __initialize(self):
        actions = self.create_action_list(self.__classes)
        for action in actions:
            if action["action"] == "create_table":
                obj = action["object"]
                statement = self.generate_statement_create_table(obj)
                self.execute_statement(statement, commit=True)
            if action["action"] == "create_table_list":
                obj = action["object"]
                field = action["field"]
                statement = self.generate_statement_create_list_table(obj, field)
                self.execute_statement(statement, commit=True)
        for i in self.__classes:
            i.set_engine(self)

    def get_dependencies(self, input_class):
        dependencies = []
        for field in fields(input_class):
            check_type = field.type
            if get_origin(check_type) == list:
                check_type = get_args(check_type)[0]
            if issubclass(check_type, Object):
                dependencies.append(check_type)
        return dependencies

    def create_dependency_tree(self, class_list):
        stack = []
        pending = class_list[:]
        while pending:
            p = pending[:]
            for i in p:
                if all([j in stack for j in self.get_dependencies(i)]):
                    stack.append(i)
                    pending.remove(i)
        return stack
    
    def create_action_list(self, class_list):
        stack = self.create_dependency_tree(class_list)
        action_list = []
        for i in stack:
            schema = i.get_types()
            action_list.append({"action": "create_table", "object": i})
            for key in schema:
                if get_origin(schema[key]['type']) == list:
                    action_list.append({"action": "create_table_list", "object": i, "field": key})      
        return action_list
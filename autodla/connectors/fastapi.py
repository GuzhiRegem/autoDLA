from fastapi import APIRouter
from fastapi.staticfiles import StaticFiles
from typing import Type, TypeVar, Generic, get_type_hints
from pydantic import create_model, ConfigDict

def json_to_lambda_str(json_condition):
    """
    Transforms a SQL-inspired JSON condition to a Python lambda string representation.
    
    Args:
        json_condition (dict): A condition object that can be:
            - Simple: {"field": "age", "operator": "gt", "value": 10}
            - Complex: {"and": [condition1, condition2, ...]} or {"or": [condition1, condition2, ...]}
    
    Returns:
        str: A string representation of the lambda function
    """
    # Check if this is a complex condition with AND/OR
    if "and" in json_condition:
        sub_conditions = [json_to_lambda_str(cond) for cond in json_condition["and"]]
        return f"lambda x: {' and '.join(f'({cond})' for cond in sub_conditions)}"
    
    elif "or" in json_condition:
        sub_conditions = [json_to_lambda_str(cond) for cond in json_condition["or"]]
        return f"lambda x: {' or '.join(f'({cond})' for cond in sub_conditions)}"
    
    # Handle negation
    elif "not" in json_condition:
        sub_condition = json_to_lambda_str(json_condition["not"])
        # Extract the condition part (after "lambda x: ")
        cond_part = sub_condition.split("lambda x: ", 1)[1]
        return f"lambda x: not ({cond_part})"
    
    # Handle simple condition
    elif all(k in json_condition for k in ["field", "operator"]):
        field = json_condition.get("field")
        operator = json_condition.get("operator")
        value = json_condition.get("value")
        
        # Map of operators to Python comparison operators
        operator_map = {
            "eq": "==",
            "neq": "!=",
            "gt": ">",
            "gte": ">=",
            "lt": "<",
            "lte": "<=",
            "in": "in",
            "nin": "not in"
        }
        
        if operator not in operator_map:
            raise ValueError(f"Unsupported operator: {operator}")
        
        # Generate the lambda function string
        op_str = operator_map[operator]
        
        # Format the value appropriately
        if isinstance(value, str):
            formatted_value = f"'{value}'"
        elif isinstance(value, list):
            # Format each element in the list
            formatted_elements = []
            for elem in value:
                if isinstance(elem, str):
                    formatted_elements.append(f"'{elem}'")
                else:
                    formatted_elements.append(str(elem))
            formatted_value = f"[{', '.join(formatted_elements)}]"
        else:
            formatted_value = str(value)
        
        return f"lambda x: x.{field} {op_str} {formatted_value}"
    else:
        raise ValueError(f"Invalid condition format: {json_condition}")

def create_soap_router(cls, prefix=None, tags=[]) -> APIRouter:
    if prefix is None:
        prefix = f"/{cls.__name__}"
    if tags == []:
        tags = [cls.__name__]
    router = APIRouter(prefix=prefix, tags=tags)
    
    import json
    import inspect
    @router.get("/list")
    async def read_object(limit=10, filter:str=None):
        if filter is None:
            res = cls.all(limit)
        else:
            filter_dict = json.loads(filter)
            lambda_st = json_to_lambda_str(filter_dict)
            res = cls.filter(lambda_st, limit)
        out = []
        for i in res:
            out.append(i.to_dict())
        return out
   
    @router.get("/get/{id_param}")
    async def get_object_id(id_param: str):
        res = cls.get_by_id(id_param)
        return res.to_dict()

    @router.get("/get_history/{id_param}")
    async def get_object_history_id(id_param: str):
        res = cls.get_by_id(id_param)
        return res.history()
    
    @router.get('/table')
    async def read_table(limit=10, only_current=True, only_active=True):
        res = cls.get_table_res(limit=limit, only_current=only_current, only_active=only_active).to_dicts()
        return res

    fields = get_type_hints(cls)
    RequestModel = create_model(f"{cls.__name__}Request", **{k: (v, ...) for k, v in fields.items()})
    
    @router.post('/new')
    async def create_object(obj: RequestModel):
        n = cls.new(**obj.model_dump())
        return n.to_dict()
    
    @router.put('/edit/{id_param}')
    async def edit_object(id_param, data: dict):
        obj = cls.get_by_id(id_param)
        obj.update(**data)
        return obj.to_dict()

    
    @router.delete("/delete/{id_param}")
    async def delete_object(id_param: str):
        obj = cls.get_by_id(id_param)
        obj.delete()
        return {"status": "done"}
    
    return router

def connect_db(app, db):
    @app.get('/get_json_schema')
    def get_schema():
        return db.get_json_schema()
    
    for cls in db.classes:
        r = create_soap_router(cls)
        app.include_router(r)
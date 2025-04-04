from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from typing import Annotated, Type, TypeVar, Generic, get_type_hints
from pydantic import BaseModel, create_model, ConfigDict
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
import hashlib
import uuid
import os
from importlib import resources as impresources
from autodla import static as staticdir
import tempfile
import shutil
# Create a temporary directory to extract the static files
temp_dir = tempfile.mkdtemp()
# Copy the static files from the package to the temporary directory
static_package_dir = impresources.files(staticdir)
static_temp_dir = os.path.join(temp_dir, 'static')
os.makedirs(static_temp_dir, exist_ok=True)
def copy_dir_recursively(source_dir, dest_dir):
    # Create the destination directory if it doesn't exist
    os.makedirs(dest_dir, exist_ok=True)
    
    # Iterate through all items in the source directory
    for item in source_dir.iterdir():
        # Get the destination path
        dest_path = os.path.join(dest_dir, item.name)
        
        if item.is_file():
            # If it's a file, copy it directly
            with item.open('rb') as src, open(dest_path, 'wb') as dst:
                shutil.copyfileobj(src, dst)
        elif item.is_dir():
            # If it's a directory, recursively copy it
            copy_dir_recursively(item, dest_path)

# Call the function with your directories
copy_dir_recursively(static_package_dir, static_temp_dir)

def hash(st):
    return hashlib.sha256(st.encode()).hexdigest()

AUTODLAWEB_USER = 'autodla'
if "AUTODLAWEB_USER" in os.environ:
    AUTODLAWEB_USER = os.environ.get("AUTODLAWEB_USER")
AUTODLAWEB_PASSWORD = hash('password')
if "AUTODLAWEB_PASSWORD" in os.environ:
    AUTODLAWEB_PASSWORD = hash(os.environ.get("AUTODLAWEB_PASSWORD"))

def generate_token():
    return hash(str(uuid.uuid4()))
current_token = ""
def create_new_token():
    global current_token
    new_token = generate_token()
    current_token = new_token
    return new_token
def validate_token(token):
    global current_token
    if token != current_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

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

def create_soap_router(cls, prefix=None, tags=[], oauth2_scheme:OAuth2PasswordBearer=None) -> APIRouter:
    if prefix is None:
        prefix = f"/{cls.__name__}"
    if tags == []:
        tags = [f"autodla_{cls.__name__}"]
    router = APIRouter(prefix=prefix, tags=tags)
    
    import json
    import inspect
    @router.get("/list")
    async def read_object(token: Annotated[str, Depends(oauth2_scheme)], limit=10, filter:str=None):
        validate_token(token)
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
    async def get_object_id(token: Annotated[str, Depends(oauth2_scheme)], id_param: str):
        validate_token(token)
        res = cls.get_by_id(id_param)
        if res is None:
            return HTTPException(400, f'{cls.__name__} not found')
        return res.to_dict()

    @router.get("/get_history/{id_param}")
    async def get_object_history_id(token: Annotated[str, Depends(oauth2_scheme)], id_param: str):
        validate_token(token)
        res = cls.get_by_id(id_param)
        if res is None:
            return HTTPException(400, f'{cls.__name__} not found')
        return res.history()
    
    @router.get('/table')
    async def read_table(token: Annotated[str, Depends(oauth2_scheme)], limit=10, only_current=True, only_active=True):
        validate_token(token)
        res = cls.get_table_res(limit=limit, only_current=only_current, only_active=only_active).to_dicts()
        return res

    fields = get_type_hints(cls)
    RequestModel = create_model(f"{cls.__name__}Request", **{k: (v, ...) for k, v in fields.items()})
    
    @router.post('/new')
    async def create_object(token: Annotated[str, Depends(oauth2_scheme)], obj: RequestModel):
        validate_token(token)
        n = cls.new(**obj.model_dump())
        return n.to_dict()
    
    @router.put('/edit/{id_param}')
    async def edit_object(token: Annotated[str, Depends(oauth2_scheme)], id_param, data: dict):
        validate_token(token)
        obj = cls.get_by_id(id_param)
        obj.update(**data)
        return obj.to_dict()

    
    @router.delete("/delete/{id_param}")
    async def delete_object(token: Annotated[str, Depends(oauth2_scheme)], id_param: str):
        validate_token(token)
        obj = cls.get_by_id(id_param)
        obj.delete()
        return {"status": "done"}
    
    return router

def connect_db(app, db):
    admin_endpoints_prefix = '/autodla-admin'

    oauth2_scheme = OAuth2PasswordBearer(tokenUrl=f"{admin_endpoints_prefix}/admin/token")

    web_router = APIRouter(prefix="/autodla-web", tags=[f"autodla_web"])
    sub_directories = ['', 'assets/']
    for sub_directory in sub_directories:
        @web_router.get('/' + sub_directory + '{filename}')
        async def static_files(filename = 'index.html'):
            return FileResponse(f'{static_temp_dir}/{sub_directory}{filename}')
    @web_router.get('/')
    async def static_home():
        return FileResponse(f'{static_temp_dir}/index.html')
    
    admin_router = APIRouter(prefix="/admin", tags=[f"autodla_admin"])
    @admin_router.post("/token")
    async def login(form_data: Annotated[OAuth2PasswordRequestForm, Depends()]):
        global current_token
        if form_data.username != AUTODLAWEB_USER:
            raise HTTPException(status_code=400, detail="Incorrect username or password")
        hashed_password = hash(form_data.password)
        if not hashed_password == AUTODLAWEB_PASSWORD:
            raise HTTPException(status_code=400, detail="Incorrect username or password")
        current_token = generate_token()
        return {"access_token": current_token, "token_type": "bearer"}

    @admin_router.get(f'/get_json_schema')
    def get_schema(token: Annotated[str, Depends(oauth2_scheme)]):
        validate_token(token)
        return db.get_json_schema()
    
    app.include_router(web_router)
    app.include_router(admin_router, prefix=admin_endpoints_prefix)
    for cls in db.classes:
        r = create_soap_router(cls, oauth2_scheme=oauth2_scheme)
        app.include_router(r, prefix=admin_endpoints_prefix)
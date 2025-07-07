from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import FileResponse
from typing import Annotated, Callable,  get_type_hints
from pydantic import create_model
from fastapi.security import OAuth2PasswordRequestForm
import json
from autodla.engine.web_connection import EndpointMaker, WebConnection
from autodla.engine.lambda_conversion import json_to_lambda_str
import inspect

class FastApiEndpointMaker(EndpointMaker):
    @classmethod
    def list(cls, object) -> Callable:
        async def read_object(limit=10, skip=0, filter:str=None):
            if filter is None:
                res = object.all(limit, skip=skip)
            else:
                filter_dict = json.loads(filter)
                lambda_st = json_to_lambda_str(filter_dict)
                res = object.filter(lambda_st, limit, skip=skip)
            out = []
            for i in res:
                out.append(i.to_dict())
            return out
        return read_object

    @classmethod
    def get(cls, object) -> Callable:
        async def get_object_id(id_param: str):
            res = object.get_by_id(id_param)
            if res is None:
                return HTTPException(400, f'{object.__name__} not found')
            return res.to_dict()
        return get_object_id

    @classmethod
    def get_history(cls, object) -> Callable:
        async def get_object_history_id(id_param: str):
            res = object.get_by_id(id_param)
            if res is None:
                return HTTPException(400, f'{object.__name__} not found')
            return res.history()
        return get_object_history_id

    @classmethod
    def table(cls, object) -> Callable:
        async def read_table(limit=10, skip=0, only_current=True, only_active=True):
            res = object.get_table_res(limit=limit, skip=skip, only_current=only_current, only_active=only_active).to_dicts()
            return res
        return read_table

    @classmethod
    def new(cls, object) -> Callable:
        fields = get_type_hints(object)
        RequestModel = create_model(f"{object.__name__}Request", **{k: (v, ...) for k, v in fields.items()})
        async def create_object(obj: RequestModel):
            n = object.new(**obj.model_dump())
            return n.to_dict()
        return create_object

    @classmethod
    def edit(cls, object) -> Callable:
        async def edit_object(id_param, data: dict):
            obj = object.get_by_id(id_param)
            obj.update(**data)
            return obj.to_dict()
        return edit_object

    @classmethod
    def delete(cls, object) -> Callable:
        async def delete_object(id_param: str):
            obj = object.get_by_id(id_param)
            obj.delete()
            return {"status": "done"}
        return delete_object

class FastApiWebConnection(WebConnection):
    def __init__(self, app, db, setup_autodla_web=True, admin_endpoints_prefix='/autodla-admin'):
        self.app = app
        self.db = db
        self.admin_endpoints_prefix = admin_endpoints_prefix
        super().__init__(FastApiEndpointMaker(), setup_autodla_web)

    def admin_endpoint_validate(self, func):
        if not inspect.iscoroutinefunction(func):
            raise Exception("Admin endpoint must be an async function")
        return func
    
    def normalize_endpoint(self, func):
        sig = inspect.signature(func)
        orig_params = list(sig.parameters.values())
        if orig_params and orig_params[0].name == "request":
            return func
        async def new_func(request : Request, *args, **kwargs):
            return await func(*args, **kwargs)
        new_func.__signature__ = sig.replace(parameters=[inspect.Parameter("request", inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=Request)] + list(sig.parameters.values()))
        new_func.__name__ = func.__name__
        new_func.__doc__ = func.__doc__
        sig = inspect.signature(new_func)
        orig_params = list(sig.parameters.values())
        return new_func
    
    
    async def extract_token(self, *args, **kwargs):
        request = kwargs.get("request") or (args[0] if args else None)
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Invalid token format")
        token = auth_header.split(" ")[1]
        return token
    
    def setup_admin_endpoints(self):
        self.admin_router = APIRouter(prefix="/admin", tags=[f"autodla_admin"])

        @self.admin_router.post("/token")
        async def login(form_data: Annotated[OAuth2PasswordRequestForm, Depends()]):
            current_token = self.login(form_data.username, form_data.password)
            return {"access_token": current_token, "token_type": "bearer"}

        @self.admin_router.get(f'/get_json_schema')
        @self.admin_endpoint
        async def get_schema():
            return self.db.get_json_schema()
        
        self.app.include_router(self.admin_router, prefix=self.admin_endpoints_prefix)
        


    def create_crud_router(self, object, prefix=None, tags=[], auth_wrapper=None) -> APIRouter:
        if prefix is None:
            prefix = f"/{object.__name__}"
        if tags == []:
            tags = [f"autodla_{object.__name__}"]
        router = APIRouter(prefix=prefix, tags=tags)
        endpoints = [
            ("list", "get", "/list"),
            ("get", "get", "/get/{id_param}"),
            ("get_history", "get", "/get_history/{id_param}"),
            ("table", "get", "/table"),
            ("new", "post", "/new"),
            ("edit", "put", "/edit/{id_param}"),
            ("delete", "delete", "/delete/{id_param}")
        ]
        for func_name, method, path in endpoints:
            endpoint_func = getattr(self.endpoint_maker, func_name)(object)
            router_wrapper = getattr(router, method)
            if auth_wrapper is not None:
                endpoint_func = auth_wrapper(endpoint_func)
            router_wrapper(path)(endpoint_func)
        return router
    
    def create_static_router(self):
        static_temp_dir = self.static_temp_dir
        web_router = APIRouter(prefix="/autodla-web", tags=[f"autodla_web"])
        sub_directories = ['', 'assets/']
        for sub_directory in sub_directories:
            @web_router.get('/' + sub_directory + '{filename}')
            async def static_files(filename = 'index.html'):
                return FileResponse(f'{static_temp_dir}/{sub_directory}{filename}')
        @web_router.get('/')
        async def static_home():
            return FileResponse(f'{static_temp_dir}/index.html')
        return web_router
    
    def setup_autodla_web_endpoints(self):
        web_router = self.create_static_router()
        self.app.include_router(web_router)
        for cls in self.db.classes:
            r = self.create_crud_router(cls, auth_wrapper=self.admin_endpoint)
            self.app.include_router(r, prefix=self.admin_endpoints_prefix)
    
    
    @classmethod
    def unauthorized_handler(cls):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    @classmethod
    def invalid_admin_credentials_handler(cls):
        raise HTTPException(status_code=400, detail="Incorrect username or password")

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import FileResponse
from typing import (
    Annotated,
    Any,
    Callable,
    Optional,
    Type,
)
from fastapi.security import OAuth2PasswordRequestForm
import json
from autodla.engine.interfaces import DB_Connection_Interface
from autodla.engine.object import Object
from autodla.engine.web_connection import EndpointMaker, WebConnection
from autodla.engine.lambda_conversion import json_to_lambda_str
import inspect
import asyncio
from contextlib import asynccontextmanager


class FastApiEndpointMaker(EndpointMaker):
    @classmethod
    def list(cls, object: Type[Object]) -> Callable:
        async def read_object(
            limit: int = 10,
            skip: int = 0,
            filter: Optional[str] = None
        ):
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
    def get(cls, object: Type[Object]) -> Callable:
        async def get_object_id(
            id_param: str
        ):
            res = object.get_by_id(id_param)
            if res is None:
                return HTTPException(400, f'{object.__name__} not found')
            return res.to_dict()
        return get_object_id

    @classmethod
    def get_history(cls, object: Type[Object]) -> Callable:
        async def get_object_history_id(
            id_param: str
        ):
            res = object.get_by_id(id_param)
            if res is None:
                return HTTPException(400, f'{object.__name__} not found')
            return res.history()
        return get_object_history_id

    @classmethod
    def table(cls, object: Type[Object]) -> Callable:
        async def read_table(
            limit: int = 10,
            skip: int = 0,
            only_current: bool = True,
            only_active: bool = True
        ):
            if not (res := object.get_table_res(
                limit=limit,
                skip=skip,
                only_current=only_current,
                only_active=only_active
            )):
                return HTTPException(400, f'{object.__name__} not found')
            return res.to_dicts()
        return read_table

    @classmethod
    def new(cls, object_class: Type[Object]) -> Callable:
        async def create_object(request: Request):
            obj = object_class(**(await request.json()))
            n = object_class.new(**obj.model_dump())
            return n.to_dict()
        return create_object

    @classmethod
    def edit(cls, object: Type[Object]) -> Callable:
        async def edit_object(id_param, data: dict):
            if (obj := object.get_by_id(id_param)) is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"{object.__name__} with id {id_param} not found"
                )
            obj.update(**data)
            return obj.to_dict()
        return edit_object

    @classmethod
    def delete(cls, object: Type[Object]) -> Callable:

        async def delete_object(id_param: str):
            if (obj := object.get_by_id(id_param)) is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"{object.__name__} with id {id_param} not found"
                )
            obj.delete()
            return {"status": "done"}
        return delete_object


class FastApiWebConnection(WebConnection):
    def __init__(
        self,
        app: FastAPI,
        db: DB_Connection_Interface,
        setup_autodla_web: bool = True,
        admin_endpoints_prefix: str = '/autodla-admin'
    ):
        self.app = app
        self.db = db
        if poll_watchdog := getattr(db, "poll_watchdog", None):
            @asynccontextmanager
            async def lifespan(app):
                asyncio.create_task(poll_watchdog())
                yield
            app.lifespan = lifespan  # type: ignore

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

        async def new_func(request: Request, *args, **kwargs):
            return await func(*args, **kwargs)
        new_func.__signature__ = sig.replace(
            parameters=[
                inspect.Parameter(
                    "request",
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    annotation=Request
                )
            ] + list(sig.parameters.values())
        )
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
        self.admin_router = APIRouter(prefix="/admin", tags=["autodla_admin"])

        @self.admin_router.post("/token")
        async def login(
            form_data: Annotated[OAuth2PasswordRequestForm, Depends()]
        ):
            current_token = self.login(form_data.username, form_data.password)
            return {"access_token": current_token, "token_type": "bearer"}

        @self.admin_router.get('/get_json_schema')
        @self.admin_endpoint
        async def get_schema():
            return self.db.get_json_schema()

        self.app.include_router(
            self.admin_router,
            prefix=self.admin_endpoints_prefix
        )

    def create_crud_router(
        self,
        object_class: Type[Object],
        prefix=None,
        tags=[],
        auth_wrapper=None
    ) -> APIRouter:
        if prefix is None:
            prefix = f"/{object_class.__name__}"
        if tags == []:
            tags = [f"autodla_{object_class.__name__}"]
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
            endpoint_func = getattr(
                self.endpoint_maker, func_name
            )(object_class)
            router_wrapper = getattr(router, method)
            if auth_wrapper is not None:
                endpoint_func = auth_wrapper(endpoint_func)
            router_args = {
                "path": path
            }
            if method == "new":
                router_args["response_model"] = object_class.__name__
            router_wrapper(path)(endpoint_func)
        return router

    def create_static_router(self):
        static_temp_dir = self.static_temp_dir
        web_router = APIRouter(prefix="/autodla-web", tags=["autodla_web"])
        sub_directories = ['', 'assets/']
        for sub_directory in sub_directories:

            @web_router.get('/' + sub_directory + '{filename}')
            async def static_files(filename: str = 'index.html'):
                return FileResponse(
                    f'{static_temp_dir}/{sub_directory}{filename}'
                )

        @web_router.get('/')
        async def static_home():
            return FileResponse(f'{static_temp_dir}/index.html')
        return web_router

    def setup_autodla_web_endpoints(self):
        web_router = self.create_static_router()
        self.app.include_router(web_router)
        for cls_type in self.db.classes:
            r = self.create_crud_router(
                cls_type,
                auth_wrapper=self.admin_endpoint
            )
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
        raise HTTPException(
            status_code=400,
            detail="Incorrect username or password"
        )

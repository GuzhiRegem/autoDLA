import os
from functools import wraps
from typing import Callable
import inspect

import hashlib
def hash(st):
    return hashlib.sha256(st.encode()).hexdigest()

AUTODLAWEB_USER = 'autodla'
if "AUTODLAWEB_USER" in os.environ:
    AUTODLAWEB_USER = os.environ.get("AUTODLAWEB_USER")
AUTODLAWEB_PASSWORD = hash('password')
if "AUTODLAWEB_PASSWORD" in os.environ:
    AUTODLAWEB_PASSWORD = hash(os.environ.get("AUTODLAWEB_PASSWORD"))


import uuid
def generate_token():
    return hash(str(uuid.uuid4()))

class EndpointMaker:
    @classmethod
    def list(cls, object) -> Callable:
        pass

    @classmethod
    def get(cls, object) -> Callable:
        pass

    @classmethod
    def get_history(cls, object) -> Callable:
        pass

    @classmethod
    def table(cls, object) -> Callable:
        pass

    @classmethod
    def new(cls, object) -> Callable:
        pass

    @classmethod
    def edit(cls, object) -> Callable:
        pass

    @classmethod
    def delete(cls, object) -> Callable:
        pass


class WebConnection:
    def __init__(self, endpoint_maker: EndpointMaker, setup_autodla_web=True):
        self.endpoint_maker = endpoint_maker
        self.current_token = generate_token()
        self.setup_admin_endpoints()
        if setup_autodla_web:
            self.setup_autodla_web_static_files()
            self.setup_autodla_web_endpoints()

    def setup_admin_endpoints(self):
        pass

    def setup_autodla_web_static_files(self):
        import os
        from importlib import resources as impresources
        from .. import static as staticdir
        import tempfile
        import shutil
        temp_dir = tempfile.mkdtemp()
        static_package_dir = impresources.files(staticdir)
        static_temp_dir = os.path.join(temp_dir, 'static')
        os.makedirs(static_temp_dir, exist_ok=True)
        def copy_dir_recursively(source_dir, dest_dir):
            os.makedirs(dest_dir, exist_ok=True)
            for item in source_dir.iterdir():
                dest_path = os.path.join(dest_dir, item.name)
                if item.is_file():
                    with item.open('rb') as src, open(dest_path, 'wb') as dst:
                        shutil.copyfileobj(src, dst)
                elif item.is_dir():
                    copy_dir_recursively(item, dest_path)
        copy_dir_recursively(static_package_dir, static_temp_dir)
        self.static_temp_dir = static_temp_dir

    def setup_autodla_web_endpoints(self):
        pass
    
    ###### Admin token management
    
    _current_admin_token = ""
    
    def create_new_admin_token(self):
        new_token = generate_token()
        self._current_admin_token = new_token
        return new_token
    
    def validate_token(self, token):
        if token != self._current_admin_token:
            self.unauthorized_handler()
        return True
    
    def login(self, username, password):
        if username != AUTODLAWEB_USER or hash(password) != AUTODLAWEB_PASSWORD:
            self.invalid_admin_credentials_handler()
        return self.create_new_admin_token()
    
    def normalize_endpoint(self, func):
        return func

    def admin_endpoint_validate(self, func):
        return func
    
    async def extract_token(self, *args, **kwargs):
        pass

    def admin_endpoint(self, func):
        self.admin_endpoint_validate(func)
        func = self.normalize_endpoint(func)
        @wraps(func)
        async def wrapper(*args, **kwargs):
            token = await self.extract_token(*args, **kwargs)
            self.validate_token(token)
            return await func(*args, **kwargs)
        return wrapper
    
    ### Handlers
    @classmethod
    def unauthorized_handler(cls):
        raise NotImplementedError("This method should be implemented in subclasses.")
    @classmethod
    def invalid_admin_credentials_handler(cls):
        raise NotImplementedError("This method should be implemented in subclasses.")


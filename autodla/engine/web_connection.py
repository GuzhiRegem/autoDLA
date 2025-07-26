from datetime import datetime
import os
from functools import wraps
from typing import Callable, Optional
import hashlib
import uuid
from abc import ABC, abstractmethod


def hash(st):
    return hashlib.sha256(st.encode()).hexdigest()


AUTODLAWEB_USER: Optional[str] = 'autodla'
if "AUTODLAWEB_USER" in os.environ:
    AUTODLAWEB_USER = os.environ.get("AUTODLAWEB_USER")
AUTODLAWEB_PASSWORD = hash('password')
if "AUTODLAWEB_PASSWORD" in os.environ:
    AUTODLAWEB_PASSWORD = hash(os.environ.get("AUTODLAWEB_PASSWORD"))


def generate_token():
    return hash(str(uuid.uuid4()))


class EndpointMaker(ABC):
    @classmethod
    @abstractmethod
    def list(cls, object) -> Callable:
        ...

    @classmethod
    @abstractmethod
    def get(cls, object) -> Callable:
        ...

    @classmethod
    @abstractmethod
    def get_history(cls, object) -> Callable:
        ...

    @classmethod
    @abstractmethod
    def table(cls, object) -> Callable:
        ...

    @classmethod
    @abstractmethod
    def new(cls, object) -> Callable:
        ...

    @classmethod
    @abstractmethod
    def edit(cls, object) -> Callable:
        ...

    @classmethod
    @abstractmethod
    def delete(cls, object) -> Callable:
        ...


class WebConnection(ABC):
    def __init__(
        self,
        endpoint_maker: EndpointMaker,
        setup_autodla_web: bool = True
    ) -> None:
        self.endpoint_maker = endpoint_maker
        self.current_token = generate_token()
        self.setup_admin_endpoints()
        self._current_admin_tokens: dict[str, datetime] = {}
        if setup_autodla_web:
            self.setup_autodla_web_static_files()
            self.setup_autodla_web_endpoints()

    @abstractmethod
    def setup_admin_endpoints(self) -> None:
        ...

    def setup_autodla_web_static_files(self) -> None:
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

    @abstractmethod
    def setup_autodla_web_endpoints(self) -> None:
        ...

    # -- Admin token management

    def create_new_admin_token(self) -> str:
        new_token = generate_token()
        self._current_admin_tokens[new_token] = datetime.now()
        return new_token

    def validate_token(self, token: str) -> bool:
        now = datetime.now()
        for token, timestamp in self._current_admin_tokens.items():
            if (now - timestamp).total_seconds() > 60 * 60 * 3:
                del self._current_admin_tokens[token]
        if self._current_admin_tokens.get(token, None) is None:
            self.unauthorized_handler()
        return True

    def login(self, username: str, password: str):
        if (username != AUTODLAWEB_USER
                or hash(password) != AUTODLAWEB_PASSWORD):
            self.invalid_admin_credentials_handler()
        return self.create_new_admin_token()

    def normalize_endpoint(self, func):
        return func

    def admin_endpoint_validate(self, func):
        return func

    @abstractmethod
    async def extract_token(self, *args, **kwargs):
        ...

    def admin_endpoint(self, func):
        self.admin_endpoint_validate(func)
        func = self.normalize_endpoint(func)

        @wraps(func)
        async def wrapper(*args, **kwargs):
            token = await self.extract_token(*args, **kwargs)
            self.validate_token(token)
            return await func(*args, **kwargs)
        return wrapper

    # Handlers
    @classmethod
    @abstractmethod
    def unauthorized_handler(cls):
        ...

    @classmethod
    @abstractmethod
    def invalid_admin_credentials_handler(cls):
        ...

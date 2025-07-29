from dataclasses import Field, dataclass
from typing import (
    Any,
    NotRequired,
    Self,
    Tuple,
    Type,
    ClassVar,
    Optional,
    Callable,
    TypedDict,
    Union,
    Literal,
    List
)
import polars as pl
from pydantic import BaseModel, ConfigDict, GetCoreSchemaHandler
from pydantic_core import CoreSchema

from abc import ABC, abstractmethod


@dataclass
class DataConversion:
    name: str
    transform: Callable[[Any], str] = lambda x: f"{x}"


@dataclass
class MethodArgument:
    st: str
    tp: Type


class GlobalTypedMethod_Interface(ABC):
    @abstractmethod
    def __init__(
        self,
        return_type: Type,
        definition: Callable,
        args: dict[str, Union[Optional[Type], list[Optional[Type]]]]
    ) -> None:
        ...

    @abstractmethod
    def check_type(
        self,
        expected_type: Union[Optional[Type], list[Optional[Type]]],
        compared_type: Optional[Type]
    ) -> bool:
        ...

    @abstractmethod
    def __call__(
        self, *args, **kwds
    ) -> Tuple[Any, Type]:
        ...


class TypedMethod_Interface(GlobalTypedMethod_Interface):
    NODE_COMPATIBILITY: dict[Type, Type]

    @abstractmethod
    def __init__(
        self,
        caller_type: Type,
        return_type: Type,
        definition: Callable,
        args: dict[str, Union[Optional[Type], list[Optional[Type]]]]
    ) -> None:
        ...

    @abstractmethod
    def __call__(
        self,
        caller: MethodArgument,
        *args, **kwds
    ) -> Tuple[Any, Type]:
        ...


class DataTransformer_Interface(ABC):
    TYPE_DICT: dict[Type, DataConversion] = {}

    class OperatorsDict(TypedDict):
        numeric: NotRequired[dict[str, str]]
        binary: NotRequired[dict[str, Callable[[Any, Any], str]]]
        boolean: NotRequired[dict[str, str]]
        unary: NotRequired[dict[str, str]]
    OPERATOR_DICT: OperatorsDict

    class MethodsDict(TypedDict):
        globals: dict[str, GlobalTypedMethod_Interface]
        by_type: dict[Type, dict[str, TypedMethod_Interface]]
    METHODS_MAP: MethodsDict

    NODE_COMPATIBILITY: dict[Type, Type]

    @classmethod
    @abstractmethod
    def check_type_compatibility(
        cls,
        tp1: Optional[Type],
        tp2: Optional[Type]
    ) -> bool:
        ...

    @classmethod
    @abstractmethod
    def get_type_from_sql_type(
        cls,
        sql_type: str
    ) -> Type:
        ...

    @classmethod
    @abstractmethod
    def get_method(
        cls,
        caller_type: Type,
        method_name: str
    ) -> Union[GlobalTypedMethod_Interface, TypedMethod_Interface]:
        ...

    @classmethod
    @abstractmethod
    def get_operator(
        cls,
        operator_type: Literal["numeric", "binary", "boolean", "unary"],
        op: Any
    ) -> Union[Callable, str]:
        ...

    @classmethod
    @abstractmethod
    def get_data_field(
        cls,
        v: Any
    ) -> Optional[DataConversion]:
        ...

    @classmethod
    @abstractmethod
    def convert_data_schema(
        cls,
        schema: dict[str, Type]
    ) -> dict[str, str]:
        ...

    @staticmethod
    @abstractmethod
    def validate_data_from_schema(
        schema: dict[str, Type],
        data: dict[str, Any]
    ) -> None:
        ...

    @classmethod
    @abstractmethod
    def convert_data(
        cls,
        data: Any
    ) -> str:
        ...


class QueryBuilder_Interface(ABC):
    @abstractmethod
    def __init__(
        self,
        data_transformer: DataTransformer_Interface
    ) -> None:
        self._data_transformer = data_transformer

    @abstractmethod
    def select(
        self,
        from_table: str,
        columns: List[str],
        where: Optional[str] = None,
        limit: Optional[int] = 10,
        order_by: Optional[str] = None,
        group_by: Optional[list[str]] = None,
        offset: Optional[int] = None
    ) -> str:
        ...

    @abstractmethod
    def insert(
        self,
        into_table: str,
        values: List[dict]
    ) -> str:
        ...

    @abstractmethod
    def update(
        self,
        table: str,
        values: dict,
        where: str
    ) -> str:
        ...

    @abstractmethod
    def delete(
        self,
        table: str,
        where: str
    ) -> str:
        ...

    @abstractmethod
    def create_table(
        self,
        table_name: str,
        schema: dict,
        if_exists: bool = False
    ) -> str:
        ...

    @abstractmethod
    def drop_table(
        self,
        table_name: str,
        if_exists: bool = False
    ) -> str:
        ...


class primary_key_Interface(str, ABC):
    @classmethod
    @abstractmethod
    def generate(cls) -> "primary_key_Interface":
        ...

    @abstractmethod
    def is_valid(self) -> bool:
        ...

    @staticmethod
    @abstractmethod
    def auto_increment() -> Field:
        ...

    @abstractmethod
    def __eq__(
        self,
        value: Any
    ) -> bool:
        ...

    @classmethod
    @abstractmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        ...

    def __hash__(self) -> int:
        return super().__hash__()


class Table_Interface(ABC):
    @abstractmethod
    def __init__(
        self,
        table_name: str,
        schema: dict[str, Type],
        db: Optional["DB_Connection_Interface"] = None
    ) -> None:
        ...

    @property
    @abstractmethod
    def db(self) -> "DB_Connection_Interface":
        ...

    @abstractmethod
    def set_db(
        self,
        db: "DB_Connection_Interface"
    ) -> None:
        ...

    @abstractmethod
    def get_all(
        self,
        limit: Optional[int] = 10,
        only_current: bool = True,
        only_active: bool = True,
        skip: int = 0
    ) -> Optional[pl.DataFrame]:
        ...

    @abstractmethod
    def filter(
        self,
        l_func: Callable[[Any], bool],
        limit: Optional[int] = 10,
        only_current: bool = True,
        only_active: bool = True,
        skip: int = 0
    ) -> Optional[pl.DataFrame]:
        ...

    @abstractmethod
    def insert(
        self,
        data: dict[str, Any]
    ) -> None:
        ...

    @abstractmethod
    def update(
        self,
        l_func: Callable[[Any], bool],
        data: dict[str, Any]
    ) -> None:
        ...

    @abstractmethod
    def delete_all(self) -> None:
        ...


class Object_Interface(ABC):
    class DependencyRequiredIds(BaseModel):
        type: Type["Object_Interface"]
        ids: set[str]

    class ObjectDependency(BaseModel):
        model_config = ConfigDict(arbitrary_types_allowed=True)
        is_list: bool
        is_value: bool
        type: Type
        table: 'Table_Interface'

    _table: ClassVar[Optional['Table_Interface']] = None
    _dependencies: ClassVar[dict[str, ObjectDependency]] = dict()
    _identifier_field: ClassVar[str] = "id"
    _objects_list: ClassVar[List[Self]] = list()
    _objects_map: ClassVar[dict[str, Self]] = dict()

    @classmethod
    @abstractmethod
    def delete_all(cls) -> None:
        ...

    @classmethod
    @abstractmethod
    def set_db(
        cls,
        db: "DB_Connection_Interface"
    ) -> None:
        ...

    class TypeDictionary(TypedDict):
        default: NotRequired[Any]
        default_factory: NotRequired[Optional[Callable]]
        is_list: NotRequired[bool]
        nullable: NotRequired[bool]
        type: NotRequired[Optional[Type]]
        depends: NotRequired[Type]

    @classmethod
    @abstractmethod
    def get_types(cls) -> dict[str, TypeDictionary]:
        ...

    @classmethod
    @abstractmethod
    def _update_individual(
        cls,
        data_inp: dict[str, Any]
    ) -> Optional[Self]:
        ...

    @classmethod
    @abstractmethod
    def _update_info(
        cls,
        filter: Optional[Callable[[Any], bool]] = None,
        limit: Optional[int] = 10,
        skip: int = 0,
        only_current: bool = True,
        only_active: bool = True
    ) -> list[Self]:
        ...

    @classmethod
    @abstractmethod
    def new(cls, **kwargs) -> Self:
        ...

    @abstractmethod
    def history(self) -> dict[str, list[dict[str, Any]]]:
        ...

    @abstractmethod
    def update(self, **kwargs) -> None:
        ...

    @abstractmethod
    def delete(self) -> None:
        ...

    @classmethod
    @abstractmethod
    def all(
        cls,
        limit: Optional[int] = 10,
        skip: int = 0
    ) -> list[Self]:
        ...

    @classmethod
    @abstractmethod
    def filter(
        cls,
        lambda_f: Optional[Callable[[Any], bool]],
        limit: Optional[int] = 10,
        skip: int = 0
    ) -> list[Self]:
        ...

    @classmethod
    @abstractmethod
    def get_by_id(
        cls,
        id_param: str
    ) -> Optional[Self]:
        ...

    @classmethod
    @abstractmethod
    def get_table_res(
        cls,
        limit: int = 10,
        skip: int = 0,
        only_current: bool = True,
        only_active: bool = True
    ) -> Optional[pl.DataFrame]:
        ...

    @abstractmethod
    def to_dict(self) -> dict[str, Any]:
        ...

    @abstractmethod
    def to_json(self) -> str:
        ...

    @abstractmethod
    def __getitem__(self, item: str) -> Any:
        ...


class TableName(BaseModel):
    name: str
    alias: str


class TableField(TypedDict):
    primary_key: NotRequired[bool]
    type: str
    is_list: NotRequired[bool]
    nullable: NotRequired[bool]
    depends: NotRequired[str]


class DB_Connection_Interface(ABC):
    __data_transformer: DataTransformer_Interface
    __query: QueryBuilder_Interface
    __classes: dict[str, Type[Object_Interface]] = {}
    _table_schemas: dict[str, dict[str, Type]] = {}
    _tables: list[str] = []

    @property
    @abstractmethod
    def usage_metrics(self) -> dict[str, int]:
        ...

    @abstractmethod
    def exit(self) -> None:
        ...

    @abstractmethod
    def __init__(
        self,
        data_transformer: DataTransformer_Interface,
        query: QueryBuilder_Interface
    ) -> None:
        ...

    @property
    @abstractmethod
    def query(self) -> QueryBuilder_Interface:
        ...

    @property
    @abstractmethod
    def data_transformer(self) -> DataTransformer_Interface:
        ...

    @abstractmethod
    def get_table_name(
        self,
        table_name: str
    ) -> TableName:
        ...

    @abstractmethod
    def clean_db(
        self,
        DO_NOT_ASK: bool = False
    ) -> None:
        ...

    @abstractmethod
    def get_table_definition(
        self,
        table_name: str
    ) -> dict[str, Type]:
        ...

    @abstractmethod
    def attach(
        self,
        objects: list[Type[Object_Interface]]
    ) -> None:
        ...

    @abstractmethod
    def get_json_schema(self) -> dict[str, dict[str, TableField]]:
        ...

    @property
    @abstractmethod
    def classes(self) -> list[Type["Object_Interface"]]:
        ...

    @abstractmethod
    def execute(
        self,
        query: str
    ) -> Optional[pl.DataFrame]:
        ...

    @abstractmethod
    def normalize_statement(
        self,
        statement: str
    ) -> str:
        ...

    @abstractmethod
    def ensure_table(
        self,
        table_name: str,
        schema: dict[str, Type],
        save: bool = False,
        current_data_schema: Optional[dict[str, Type]] = None
    ) -> None:
        ...

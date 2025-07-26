# The `DataTransformer` class in the provided Python code contains
# methods for data conversion, type checking, method retrieval,
# and SQL operator mapping.
from typing import (
    Callable,
    Any,
    Literal,
    Optional,
    Tuple,
    Type,
    Union
)
from autodla.engine.interfaces import (
    DataConversion,
    MethodArgument,
    GlobalTypedMethod_Interface,
    TypedMethod_Interface,
    DataTransformer_Interface
)
import os

DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
if "DATETIME_FORMAT" in os.environ:
    DATETIME_FORMAT = os.environ.get("DATETIME_FORMAT", "")


class GlobalTypedMethod(GlobalTypedMethod_Interface):
    def __init__(
        self,
        return_type: Type,
        definition: Callable,
        args: dict[str, Union[Optional[Type], list[Optional[Type]]]]
    ) -> None:
        self.return_type = return_type
        self.definition = definition
        self.args = args

    def check_type(
        self,
        expected_type: Union[Optional[Type], list[Optional[Type]]],
        compared_type: Optional[Type]
    ) -> bool:
        if type(expected_type) is list:
            for type_i in expected_type:
                if type_i is None and compared_type is None:
                    return True
                if compared_type == type_i:
                    return True
        elif compared_type == expected_type:
            return True
        return False

    def __call__(
        self, *args, **kwds
    ) -> Tuple[Any, Type]:
        keys = list(self.args.keys())
        arguments = {}
        for i, k in enumerate(keys):
            v = None
            if i < len(args):
                v = args[i]
            if k in kwds:
                v = kwds[k]
            if not self.check_type(self.args[k], getattr(v, 'tp', None)):
                raise TypeError(
                    "expected type {} for argument {}, got {}".format(
                        self.args[k],
                        k,
                        type(v)
                    )
                )
            arguments[k] = getattr(v, 'st', None)
        return self.definition(arguments), self.return_type


class TypedMethod(GlobalTypedMethod, TypedMethod_Interface):
    def __init__(
        self,
        caller_type: Type,
        return_type: Type,
        definition: Callable,
        args: dict[str, Union[Optional[Type], list[Optional[Type]]]]
    ) -> None:
        self.caller_type = caller_type
        super().__init__(return_type, definition, args)

    def __call__(
        self,
        caller: MethodArgument,
        *args, **kwds
    ) -> Tuple[Any, Type]:
        if (
            any([(
                type(arg) is not MethodArgument
            ) for arg in args])
            or any([(
                type(arg) is not MethodArgument
            ) for arg in list(kwds.values())])
        ):
            raise ValueError(
                "All arguments for data conversion should be MethodArgument"
            )
        keys = list(self.args.keys())
        arguments = {}
        for i, k in enumerate(keys):
            v: Optional[MethodArgument] = None
            if i < len(args):
                v = args[i]
            if k in kwds:
                v = kwds[k]
            if v is None:
                raise ValueError(f"Argument {k} is required")
            if not self.check_type(
                self.args[k], v.tp
            ):
                raise TypeError(
                    "expected type {} for argument {}, got {}".format(
                        self.args[k],
                        k,
                        v.tp
                    )
                )
            arguments[k] = v.st
        if not self.check_type(self.caller_type, caller.tp):
            raise TypeError(
                "expected caller type {}, got {}".format(
                    self.caller_type,
                    caller.tp
                )
            )
        arguments["caller"] = caller.st
        return self.definition(arguments), self.return_type


class DataTransformer(DataTransformer_Interface):
    TYPE_DICT: dict[Type, DataConversion]
    OPERATOR_DICT: DataTransformer_Interface.OperatorsDict
    METHODS_MAP: DataTransformer_Interface.MethodsDict
    NODE_COMPATIBILITY: dict[Type, Type] = {}

    @classmethod
    def check_type_compatibility(cls, tp1, tp2) -> bool:
        out = tp1 == tp2
        if not out:
            if tp1 in cls.NODE_COMPATIBILITY:
                out = cls.NODE_COMPATIBILITY[tp1] == tp2
        return out

    @classmethod
    def get_type_from_sql_type(cls, sql_type) -> type:
        found = None
        for k, v in cls.TYPE_DICT.items():
            if v.name.upper() == sql_type.upper():
                found = k
        if found is None:
            raise TypeError(f"invalid conversion for sql_type '{sql_type}'")
        return found

    @classmethod
    def get_method(
        cls,
        caller_type: Type,
        method_name: str
    ) -> Union[GlobalTypedMethod_Interface, TypedMethod_Interface]:
        d: Union[
            dict[str, GlobalTypedMethod_Interface],
            dict[str, TypedMethod_Interface]
        ] = cls.METHODS_MAP["globals"]
        if caller_type is not None:
            d = cls.METHODS_MAP["by_type"][caller_type]
        out = d.get(method_name)
        if out is None:
            raise ValueError(
                "Method '{}' not found for type {}".format(
                    method_name,
                    caller_type.__name__ if caller_type else 'None'
                )
            )
        return out

    @classmethod
    def get_operator(
        cls,
        operator_type: Literal["numeric", "binary", "boolean", "unary"],
        op: Any
    ) -> Union[Callable, str]:
        out = cls.OPERATOR_DICT[operator_type].get(type(op).__name__)
        if out is None:
            raise ValueError(
                "Unsupported {} operator: {}".format(
                    operator_type,
                    op.__class__.__name__
                )
            )
        return out

    @classmethod
    def get_data_field(
        cls,
        v: Any
    ) -> Optional[DataConversion]:
        return cls.TYPE_DICT.get(v)

    @classmethod
    def convert_data_schema(
        cls,
        schema: dict[str, Type]
    ) -> dict[str, str]:
        out = {}
        for k, v in schema.items():
            f = cls.get_data_field(v["type"])
            if f is not None:
                out[k] = f.name
                if v.get("nullable") is not True:
                    out[k] += " NOT NULL"
        return out

    @staticmethod
    def validate_data_from_schema(
        schema: dict[str, Type],
        data: dict[str, Any]
    ) -> None:
        extra_keys = []
        for i in data:
            if i not in schema:
                extra_keys.append(i)
        if extra_keys:
            raise ValueError(f"Extra values found: {extra_keys}")
        missing_values = []
        for i in data:
            if i not in schema:
                missing_values.append(i)
        if missing_values:
            raise ValueError(f"Missing values: {missing_values}")
        invalid_types: list[str] = []
        for i in data:
            if not isinstance(data[i], schema[i]):
                invalid_types.append(i)
        if invalid_types:
            msg = 'Invalid types:\n'
            for invalid_type in invalid_types:
                msg += '{}: expected {} got {}\n'.format(
                    invalid_type,
                    schema[invalid_type],
                    type(data[invalid_type])
                )
            raise ValueError(msg)

    @classmethod
    def convert_data(
        cls,
        data: Any
    ) -> str:
        v = cls.get_data_field(type(data))
        if v is not None:
            return v.transform(data)
        if type(data) is list:
            return f"({', '.join([cls.convert_data(i) for i in data])})"
        raise TypeError(
            f"Missing transformer for class {type(data).__name__}"
        )

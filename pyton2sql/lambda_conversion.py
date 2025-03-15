import ast
import inspect
from dataclasses import dataclass
from typing import Literal
from data_conversion import DataTransformer
from datetime import datetime

class LambdaFinder(ast.NodeVisitor):
    def __init__(self): 
        self.found = None

    def find(self, tree):
        self.visit(tree)
        return self.found

    def generic_visit(self, node):
        if isinstance(node, ast.Lambda):
            self.found = node
            return
        ast.NodeVisitor.generic_visit(self, node)

def lambda_to_ast(lambda_func):
    try:
        source = inspect.getsource(lambda_func).strip()
    except (IOError, TypeError) as e:
        raise TypeError(f"Failed to get lambda source: {str(e)}")
    root_tree = ast.parse(source)
    return LambdaFinder().find(root_tree)

def lambda_to_text(lambda_func):
    lambda_node = lambda_to_ast(lambda_func)
    return ast.unparse(lambda_node)

OPERATOR_DICT = {
    "numeric": {
        'Eq': "=",
        'NotEq': "<>",
        'Lt': "<",
        'LtE': "<=",
        'Gt': ">",
        'GtE': ">=",
        'In': "IN",
        'NotIn': "NOT IN",
        'Is': "IS",
        'IsNot': "IS NOT"
    },
    "binary": {
        "Add": lambda x, y: f'{x} + {y}',
        "Sub": lambda x, y: f'{x} - {y}',
        "Mult": lambda x, y: f'{x} * {y}',
        "Div": lambda x, y: f'{x} / {y}',
        "FloorDiv": lambda x, y: f'FLOOR({x} / {y})',
        "Mod": lambda x, y: f'{x} % {y}',
        "Pow": lambda x, y: f'POWER({x},{y})'
    },
    "boolean": {
        "And": 'AND',
        "Or": 'OR',
    },
    "unary": {
        "Not": 'NOT'
    }
}

class TypedMethod:
    def __init__(self, definition : str, args : dict[type], is_global=False):
        self.definition = definition
        self.args = args
        self.is_global = is_global

METHODS_MAP = {
    "global": {
        'round': TypedMethod("ROUND({value}, {digits})", {"value": [float, int], 'digits': int}, True),
        'abs': TypedMethod('ABS({value})', {'value': [float, int]}, True)
    },
    "by_type": {
        str: {
            "startswith": TypedMethod("{caller} LIKE '{value}%'", {"value": str}),
            "endswith": TypedMethod("{caller} LIKE '%{value}'", {"value": str}),
            "contains": TypedMethod("{caller} LIKE '%{value}%'", {"value": str}),
            "lower": TypedMethod("LOWER({caller})", None),
            "upper": TypedMethod("UPPER({caller})", None),
            "strip": TypedMethod("TRIM({caller})", None),
            "lstrip": TypedMethod("LTRIM({caller})", None),
            "rstrip": TypedMethod("RTRIM({caller})", None),
            "replace": TypedMethod("REPLACE({caller}, {old}, {new})", {"old": str, "new": str})
        },
        datetime: {
            "year": TypedMethod("EXTRACT(YEAR FROM {caller})", None),
            "month": TypedMethod("EXTRACT(YEAR FROM {caller})", None),
            "day": TypedMethod("EXTRACT(YEAR FROM {caller})", None)
        }
    }
}

@dataclass
class NodeReturn:
    st: str
    tp: type

class LambdaToSql(ast.NodeVisitor):
    def __init__(self, root : ast.Lambda, schema, data_transformer : DataTransformer, ctx_vars = {}):
        self.ctx_vars = ctx_vars
        self.schema = schema
        self.root = root
        self.data_transformer = data_transformer
    
    def get_operator(self, operator_type: Literal["numeric", "binary", "boolean", "unary"], op):
        """Convert Python comparison operators to SQL operators"""
        out = OPERATOR_DICT[operator_type].get(type(op).__name__)
        if out is None:
            raise ValueError(f"Unsupported {operator_type} operator: {op.__class__.__name__}")
        return out

    def parse_node(self, node):
        match type(node).__name__:
            case 'Call':
                print(ast.dump(node))
                print(ast.unparse(node.func))
                print(eval(ast.unparse(node.func)))
                function = None
                if type(node.func).__name__ == "Attribute":
                    function_name = f"{node.func.value.id}.{node.func.attr}"
                    module = self.ctx_vars.get(node.func.value.id)
                    if module is not None:
                        function = getattr(module, node.func.attr, None)
                else:
                    function_name = f"{node.func.id}"
                    function = self.ctx_vars.get(node.func.id)
                if function is None:
                    raise ValueError(f"function '{function_name}' is not defined")
                args = [eval(ast.unparse(i)) for i in node.args]
                value = function(*args)
                transformed_value = self.data_transformer.convert_data(value)
                return NodeReturn(transformed_value, type(value))
            case 'List':
                values = [self.parse_node(i).st for i in node.elts]
                return NodeReturn(
                    f'({", ".join(values)})',
                    list
                )
            case 'UnaryOp':
                op = self.get_operator("unary", node.op)
                child = self.parse_node(node.operand)
                return NodeReturn(
                    f'({op} {child.st})',
                    bool
                )
            case 'BoolOp':
                values = [self.parse_node(i) for i in node.values]
                for i in values:
                    if i.tp != bool:
                        raise TypeError(f"expression '{i.st}' is not a valid Boolean")
                op = self.get_operator("boolean", node.op)
                out = f' {op} '.join([i.st for i in values])
                return NodeReturn(
                    f'({out})',
                    bool
                )
            case 'BinOp':
                left = self.parse_node(node.left)
                right = self.parse_node(node.right)
                func = self.get_operator("binary", node.op)
                if left.tp != right.tp:
                    raise TypeError(f"invalid operation between {left.tp} and {right.tp}")
                return NodeReturn(
                    f'({func(left.st, right.st)})',
                    left.tp
                )
            case 'Name':
                if node.id == 'x':
                    return NodeReturn('x', str)
                value = self.ctx_vars.get(node.id)
                if value is not None:
                    transformed_value = self.data_transformer.convert_data(value)
                    return NodeReturn(transformed_value, type(value))
                raise ValueError(f"'{node.id}' is not defined")
            case 'Constant':
                return NodeReturn(
                    self.data_transformer.convert_data(node.value),
                    type(node.value)
                )
            case 'Compare':
                left = self.parse_node(node.left)
                op = self.get_operator("numeric", node.ops[0])
                right = self.parse_node(node.comparators[0])
                if left.tp != right.tp and left.tp != list and right.tp != list:
                    raise TypeError(f"invalid comparission between {left.tp} and {right.tp}")
                return NodeReturn(
                    f'({left.st} {op} {right.st})', bool
                )
            case 'Attribute':
                if node.attr not in self.schema:
                    raise AttributeError(f"invalid attribute '{node.attr}'")
                return NodeReturn(f'{self.parse_node(node.value).st}.{node.attr}', self.schema.get(node.attr))
            case _:
                print(ast.dump(node))
                raise ValueError(f'Invalid node type: {type(node).__name__}')


    def transform(self):
        definition_error = False
        if len(self.root.args.args) != 1:
            definition_error = True
        if list(self.root.args.args)[0].arg != 'x':
            definition_error = True
        if definition_error:
            raise SyntaxError("lambda definition should be -> 'lambda x:'")
        out = self.parse_node(self.root.body)
        return out


def extract_variables_from_lambda(lambda_func, ctx_vars = {}):
    out = {}
    lambda_node = lambda_to_ast(lambda_func)
    for node in ast.walk(lambda_node):
        if type(node).__name__ == 'Name':
            out[node.id] = ctx_vars.get(node.id)
    return out

def lambda_to_sql(schema, lambda_func, data_transformer : DataTransformer, ctx_vars={}):
    lambda_node = lambda_to_ast(lambda_func)
    vars_dict = extract_variables_from_lambda(lambda_func, ctx_vars)
    out = LambdaToSql(lambda_node, schema, data_transformer=data_transformer, ctx_vars=ctx_vars).transform()
    return out.st
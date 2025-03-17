import ast
import inspect
from dataclasses import dataclass
from typing import Any, Callable, Literal, get_origin
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

@dataclass
class NodeReturn:
    st: str
    tp: type
    eval: Any = None

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

class GlobalTypedMethod:
    def __init__(self, return_type: type, definition : str, args : dict[type]):
        self.return_type = return_type
        self.definition = definition
        self.args = args
    
    def check_type(self, expected_type, compared_type):
        if type(expected_type) == list:
            for type_i in expected_type:
                if type_i is None and compared_type is None:
                    return True
                if compared_type == type_i:
                    return True
        elif compared_type == expected_type:
            return True
        return False
    
    def __call__(self, *args, **kwds):
        keys = list(self.args.keys())
        arguments = {}
        for i, k in enumerate(keys):
            v = None
            if i < len(args):
                v = args[i]
            if k in kwds:
                v = kwds[k]
            if not self.check_type(self.args[k], getattr(v, 'tp', None)):
                raise TypeError(f"expected type {self.args[k]} for argument {k}, got {type(v)}")
            arguments[k] = getattr(v, 'st', None)
        return self.definition(arguments), self.return_type

class TypedMethod(GlobalTypedMethod):
    def __init__(self, caller_type: type, return_type: type, definition : str, args : dict[type]):
        self.caller_type = caller_type
        super().__init__(return_type, definition, args)
    
    def __call__(self, caller, *args, **kwds):
        keys = list(self.args.keys())
        arguments = {}
        for i, k in enumerate(keys):
            v = None
            if i < len(args):
                v = args[i]
            if k in kwds:
                v = kwds[k]
            if not self.check_type(self.args[k], v.tp if v is not None else None):
                raise TypeError(f"expected type {self.args[k]} for argument {k}, got {v.tp}")
            arguments[k] = v.st
        if not self.check_type(self.caller_type, caller.tp):
            raise TypeError(f"expected caller type {self.caller_type}, got {caller.tp}")
        arguments["caller"] = caller.st
        return self.definition(arguments), self.return_type



METHODS_MAP = {
    "global": {
        'round': GlobalTypedMethod(
            int,
            lambda data: f"ROUND({data['value']}{str(', ' + data['digits']) if data['digits'] is not None else ''})",
            {"value": [float, int], 'digits': [int, None]}
        ),
        'abs': GlobalTypedMethod(
            int,
            lambda data: f'ABS({data["value"]})',
            {'value': [float, int]}
        )
    },
    "by_type": {
        str: {
            "lower": TypedMethod(
                str, str,
                lambda data: f"LOWER({data['caller']})",
                {}
            ),
            "startswith": TypedMethod(
                str, bool,
                lambda data: f"({data['caller']} LIKE '{data['value'][1:-1]}%')",
                {"value": str}
            )
        },
        datetime: {
            "year": TypedMethod(
                datetime, int,
                lambda data: f"EXTRACT(YEAR FROM {data['caller']})",
                {}
            ),
        }
    }
}

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
    
    def evaluate_node(self, node):
        return eval(str(ast.unparse(node)), self.ctx_vars)
    
    def repr(self, obj):
        st = repr(obj)
        if type(obj) in [Callable, type(round), type("".startswith), type(inspect)]:
            st = obj.__name__
            if hasattr(obj, '__self__'):
                st = self.repr(obj.__self) + '.' + st
        return st
    
    def obj_to_node(self, obj):
        st = self.repr(obj)
        if type(obj) in [int, float, bool, str, datetime]:
            return ast.Constant(value=obj)
        return ast.parse(st).body[0].value

    def evaluate_and_parse_node(self, node):
        out = self.obj_to_node(self.evaluate_node(node))
        return self.parse_node(out)

    def parse_node(self, node):
        match type(node).__name__:
            case 'Call':
                args = [self.parse_node(arg) for arg in node.args]
                
                caller = None
                func_name = ""
                func = None
                if type(node.func).__name__ == "Attribute":
                    caller = self.parse_node(node.func.value)
                    func_name = node.func.attr
                    if caller.eval is not None:
                        func = getattr(caller.eval, func_name)
                else:
                    func_name = node.func.id
                    if func_name in self.ctx_vars:
                        f = self.ctx_vars.get(func_name)
                        if type(f) in [Callable, type(round)]:
                            func = f
                        if type(f) == type(int) and all([arg.eval is not None for arg in args]):
                            res = f(*[arg.eval for arg in args])
                            n = self.obj_to_node(res)
                            return self.parse_node(self.obj_to_node(res))
                            
                
                if func is not None and all([arg.eval is not None for arg in args]):
                    return self.evaluate_and_parse_node(node)
                
                if caller is None:
                    found = METHODS_MAP["global"].get(func_name)
                    if found is not None:
                        st, tp = found(*args)
                        return NodeReturn(st, tp)
                else:
                    found = METHODS_MAP["by_type"][caller.tp].get(func_name)
                    if found is not None:
                        st, tp = found(caller, *args)
                        return NodeReturn(st, tp)
                raise ValueError(f'function {func_name} not found')
            
            case 'List':
                values = [self.parse_node(i) for i in node.elts]
                st_out = f'({", ".join([node_i.st for node_i in values])})'
                eval_list = [node_i.eval for node_i in values]
                if all([node_i is not None for node_i in eval_list]):
                    return NodeReturn(st_out, list, eval_list)
                return NodeReturn(st_out, list)
            case 'UnaryOp':
                op = self.get_operator("unary", node.op)
                child = self.parse_node(node.operand)
                if child.eval is not None:
                    return self.evaluate_and_parse_node(node)
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
                if all([arg.eval is not None for arg in values]):
                    return self.evaluate_and_parse_node(node)
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
                if left.eval is not None and right.eval is not None:
                    return self.evaluate_and_parse_node(node)
                return NodeReturn(
                    f'({func(left.st, right.st)})',
                    left.tp
                )
            case 'Name':
                if node.id == 'x':
                    return NodeReturn('x', str)
                value = self.ctx_vars.get(node.id)
                if value is not None:
                    transformed_value = node.id
                    if not isinstance(value, Callable):
                        transformed_value = self.data_transformer.convert_data(value)
                    return NodeReturn(transformed_value, type(value), value)
                raise ValueError(f"'{node.id}' is not defined")
            case 'Constant':
                return NodeReturn(
                    self.data_transformer.convert_data(node.value),
                    type(node.value),
                    node.value
                )
            case 'Compare':
                left = self.parse_node(node.left)
                op = self.get_operator("numeric", node.ops[0])
                right = self.parse_node(node.comparators[0])
                if left.tp != right.tp and left.tp != list and right.tp != list:
                    raise TypeError(f"invalid comparission between {left.tp} and {right.tp}")
                if left.eval is not None and right.eval is not None:
                    return self.evaluate_and_parse_node(node)
                return NodeReturn(
                    f'({left.st} {op} {right.st})', bool
                )
            case 'Attribute':
                caller = self.parse_node(node.value)
                if caller.eval is None:
                    if node.attr not in self.schema:
                        raise AttributeError(f"invalid attribute for x: '{node.attr}'")
                    return NodeReturn(f'{self.parse_node(node.value).st}.{node.attr}', self.schema.get(node.attr))
                val = getattr(caller.eval, node.attr)
                if val is None:
                    raise AttributeError(f"attribute not found: '{node.attr}'")
                if type(val) == Callable:
                    return NodeReturn(node.attr, Callable, val)
                return self.evaluate_and_parse_node(node)
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
import ast
import inspect
from dataclasses import dataclass
from typing import Any, Callable
from autodla.engine.data_conversion import DataTransformer, MethodArgument
from datetime import datetime
from autodla.utils.logger import logger


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
        if type(lambda_func) == str:
            source = lambda_func
        else:
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

def pn(node):
        logger.debug("")
        if isinstance(node, ast.AST):
            logger.debug(ast.dump(node))
        else:
            logger.debug(node)
        logger.debug("")

class LambdaToSql(ast.NodeVisitor):
    def __init__(self, root : ast.Lambda, schema, data_transformer : DataTransformer, ctx_vars = {}, alias='x'):
        self.ctx_vars = ctx_vars
        self.schema = schema
        self.root = root
        self.data_transformer = data_transformer
        self.alias = alias
    
    def evaluate_node(self, node):
        return eval(str(ast.unparse(node)), self.ctx_vars)
    
    def repr(self, obj):
        st = repr(obj)
        if isinstance(obj, Callable):
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
    
    def node_compatibility(self, node1 : NodeReturn, node2: NodeReturn):
        if isinstance(node1, type(node2)) or isinstance(node2, type(node1)):
            return True
        if node1 is None or node2 is None:
            return True
        if isinstance(node1, list):
            if all([self.node_compatibility(arg, node2) for arg in node1]):
                return True
        elif isinstance(node2, list):
            if all([self.node_compatibility(arg, node1) for arg in node2]):
                return True
        return False

    def parse_node(self, node):
        match type(node).__name__:
            case 'Subscript':
                caller = self.parse_node(node.value)
                slice_node = self.parse_node(node.slice)
                attr = slice_node.st if slice_node.eval is None else slice_node.eval
                if caller.eval is None:
                    if attr not in self.schema:
                        raise AttributeError(f"invalid attribute for {caller.st}: '{node.attr}'")
                    if caller.st == "":
                        return NodeReturn(attr, self.schema.get(attr))
                    return NodeReturn(f'{caller.st}.{attr}', self.schema.get(attr))
                if slice_node.eval is None:
                    raise ValueError(f'invalid slice node: {slice_node.st}')
                val = getattr(caller.eval, slice_node.eval)
                if val is None:
                    raise AttributeError(f"attribute not found: '{node.attr}'")
                if type(val) == Callable:
                    return NodeReturn(node.attr, Callable, val)
                return self.evaluate_and_parse_node(node)
            case 'IfExp':
                condition = self.parse_node(node.test)
                condition_value = self.parse_node(node.body)
                else_value = self.parse_node(node.orelse)
                if all([getattr(arg, 'eval', None) is not None for arg in [condition, condition_value, else_value]]):
                    return self.evaluate_and_parse_node(node)
                if condition.tp != bool:
                    raise TypeError('condition is not a valid bool')
                if not self.node_compatibility(condition_value, else_value):
                    raise TypeError('values for if should be of same type')
                st = f'(CASE WHEN {condition.st} THEN {condition_value.st} ELSE {else_value.st} END)'
                return NodeReturn(st, condition_value.tp)
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
                    f = self.ctx_vars.get(func_name)
                    if f is not None:
                        if isinstance(f, Callable):
                            func = f
                        if type(f) == type(int) and all([arg.eval is not None for arg in args]):
                            res = f(*[arg.eval for arg in args])
                            n = self.obj_to_node(res)
                            return self.parse_node(self.obj_to_node(res))
                            
                
                if func is not None and all([arg.eval is not None for arg in args]):
                    return self.evaluate_and_parse_node(node)
                
                if caller is None:
                    found = self.data_transformer.get_method(None, func_name)
                    if found is not None:
                        st, tp = found(*[MethodArgument(arg.st, arg.tp) for arg in args])
                        return NodeReturn(st, tp)
                else:
                    found = self.data_transformer.get_method(caller.tp, func_name)
                    if found is not None:
                        st, tp = found(MethodArgument(caller.st, caller.tp), *[MethodArgument(arg.st, arg.tp) for arg in args])
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
                op = self.data_transformer.get_operator("unary", node.op)
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
                op = self.data_transformer.get_operator("boolean", node.op)
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
                func = self.data_transformer.get_operator("binary", node.op)
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
                    return NodeReturn(self.alias, str)
                value = self.ctx_vars.get(node.id)
                if value is not None:
                    try:
                        transformed_value = self.data_transformer.convert_data(value)
                    except:
                        transformed_value = node.id
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
                op = self.data_transformer.get_operator("numeric", node.ops[0])
                right = self.parse_node(node.comparators[0])
                if not self.node_compatibility(left, right):
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
                    if caller.st == "":
                        return NodeReturn(node.attr, self.schema.get(node.attr))
                    return NodeReturn(f'{caller.st}.{node.attr}', self.schema.get(node.attr))
                val = getattr(caller.eval, node.attr)
                if val is None:
                    raise AttributeError(f"attribute not found: '{node.attr}'")
                if type(val) == Callable:
                    return NodeReturn(node.attr, Callable, val)
                return self.evaluate_and_parse_node(node)
            case _:
                try:
                    evaluation = self.evaluate_node(node)
                    new_node=ast.Constant(value=evaluation)
                    return self.parse_node(new_node)
                except:
                    logger.debug(ast.dump(node))
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

import builtins
def get_context_from_lamba(lambda_func):
    file = lambda_func.__code__.co_filename
    line = lambda_func.__code__.co_firstlineno
    found = None
    for frame in inspect.stack():
        if frame.filename == file and frame.lineno == line:
            found = frame
            break
    if found is None:
        raise ValueError('frame not found')
    return {**vars(builtins), **found.frame.f_locals}

def lambda_to_sql(schema, lambda_func, data_transformer : DataTransformer, ctx_vars={}, alias='x') -> str:
    if not type(lambda_func) == str:
        ctx_vars = get_context_from_lamba(lambda_func)
    lambda_node = lambda_to_ast(lambda_func)
    out = LambdaToSql(lambda_node, schema, data_transformer=data_transformer, ctx_vars=ctx_vars, alias=alias).transform()
    return out.st

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
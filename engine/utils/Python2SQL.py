import ast
import inspect

# Characters to strip from string values when cleaning
STRIP_STR = '"' + "'" + '\\'

# Dictionary of Python string methods to SQL function equivalents
SQL_METHOD_MAP = {
    # String methods
    "startswith": lambda column, arg: f"{column} LIKE '{arg}%'",
    "endswith": lambda column, arg: f"{column} LIKE '%{arg}'", 
    "contains": lambda column, arg: f"{column} LIKE '%{arg}%'",
    "lower": lambda column: f"LOWER({column})",
    "upper": lambda column: f"UPPER({column})",
    "strip": lambda column: f"TRIM({column})",
    "lstrip": lambda column: f"LTRIM({column})",
    "rstrip": lambda column: f"RTRIM({column})",
    "replace": lambda column, old, new: f"REPLACE({column}, '{old}', '{new}')",
    
    # Numeric methods
    "round": lambda column, digits=0: f"ROUND({column}, {digits})",
    "abs": lambda column: f"ABS({column})",
    
    # Date methods - these will vary by SQL dialect
    "year": lambda column: f"EXTRACT(YEAR FROM {column})",
    "month": lambda column: f"EXTRACT(MONTH FROM {column})",
    "day": lambda column: f"EXTRACT(DAY FROM {column})",
}

class LambdaToSQLTransformer(ast.NodeVisitor):
    def __init__(self, type_dict=None):
        self.sql_parts = []
        self.type_dict = type_dict or {}
        self.current_column = None  # Track current column for type checking
        self.errors = []  # Track errors for reporting
        
    def visit_Compare(self, node):
        # Handle comparison operations (==, !=, <, >, <=, >=)
        self.current_column = None
        left = self.process_node(node.left)
        column_name = self.current_column
        
        for i, op in enumerate(node.ops):
            right = self.process_node(node.comparators[i])
            
            # Type checking for the comparison
            if column_name and column_name in self.type_dict:
                column_type = self.type_dict[column_name]
                
                # Check if we're comparing a value with the correct type
                try:
                    # Extract the actual value if it's a literal constant
                    if isinstance(node.comparators[i], ast.Constant):
                        compare_value = node.comparators[i].value
                        self.check_type_compatibility(column_name, column_type, compare_value)
                except Exception as e:
                    self.errors.append(f"Type error in comparison: {str(e)}")
                
                # Apply type-specific conversion
                right = self.apply_type_conversion(right, column_type)
                
            self.sql_parts.append(f"{left} {self.get_operator(op)} {right}")
            
    def visit_BoolOp(self, node):
        # Handle boolean operations (and, or)
        op_str = " AND " if isinstance(node.op, ast.And) else " OR "
        parts = []
        
        for value in node.values:
            sub_transformer = LambdaToSQLTransformer()
            sub_transformer.visit(value)
            parts.append("(" + " ".join(sub_transformer.sql_parts) + ")")
            
        self.sql_parts.append(op_str.join(parts))
        
    def visit_UnaryOp(self, node):
        # Handle unary operations (not)
        if isinstance(node.op, ast.Not):
            sub_transformer = LambdaToSQLTransformer()
            sub_transformer.visit(node.operand)
            self.sql_parts.append(f"NOT ({' '.join(sub_transformer.sql_parts)})")
        else:
            raise ValueError(f"Unsupported unary operator: {node.op.__class__.__name__}")
    
    def visit_Call(self, node):
        # Handle function calls like row.name.startswith("A")
        
        # Check if it's a method call on an attribute
        if isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Attribute):
            # Extract the column and method name
            if self.process_node(node.func.value.value) == "row":
                column_name = node.func.value.attr
                method_name = node.func.attr
                
                # Check if column exists
                if self.type_dict and column_name not in self.type_dict:
                    self.errors.append(f"Column '{column_name}' not found in type dictionary")
                
                # Check if method is supported
                if method_name in SQL_METHOD_MAP:
                    # Process arguments
                    args = [self.process_node(arg) for arg in node.args]
                    
                    # Clean the arguments (remove quotes for string args)
                    clean_args = []
                    for arg in args:
                        # Remove outer quotes for string arguments
                        if arg.startswith("'") and arg.endswith("'"):
                            clean_args.append(arg[1:-1])
                        else:
                            clean_args.append(arg)
                    
                    # Apply the SQL method transformation
                    try:
                        sql_func = SQL_METHOD_MAP[method_name]
                        result = sql_func(column_name, *clean_args)
                        self.sql_parts.append(result)
                        return
                    except Exception as e:
                        self.errors.append(f"Error applying SQL method '{method_name}': {str(e)}")
                else:
                    self.errors.append(f"Unsupported string method: '{method_name}'. "
                                      f"Supported methods: {', '.join(SQL_METHOD_MAP.keys())}")
        
        # Handle other function calls like len(), count(), etc.
        func_name = self.process_node(node.func)
        args = [self.process_node(arg) for arg in node.args]
        
        self.sql_parts.append(f"{func_name}({', '.join(args)})")
            
    def visit_Attribute(self, node):
        # Handle attribute access (e.g., row.column)
        value = self.process_node(node.value)
        attr = node.attr
        
        if value == "row":
            # If it's a row attribute, treat it as a column name
            self.sql_parts.append(attr)
        else:
            # For other attributes, use dot notation
            self.sql_parts.append(f"{value}.{attr}")
    
    def process_node(self, node):
        if isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Constant):
            if isinstance(node.value, str):
                return f"'{node.value}'"
            elif node.value is None:
                return "NULL"
            else:
                return str(node.value)
        elif isinstance(node, ast.Attribute):
            value = self.process_node(node.value)
            if value == "row":
                attr_name = node.attr
                # Check if column exists in type dictionary
                if self.type_dict and attr_name not in self.type_dict:
                    self.errors.append(f"Column '{attr_name}' not found in type dictionary. Available columns: {', '.join(self.type_dict.keys())}")
                # Store the column name for later type checking
                self.current_column = attr_name
                return attr_name
            return f"{value}.{node.attr}"
        else:
            sub_transformer = LambdaToSQLTransformer(self.type_dict)
            sub_transformer.visit(node)
            # Propagate errors from sub-transformers
            self.errors.extend(sub_transformer.errors)
            return " ".join(sub_transformer.sql_parts)
    
    def check_type_compatibility(self, column_name, expected_type, value):
        """Check if a value is compatible with the expected type"""
        if value is None:
            return  # NULL can be compared with any type
            
        if expected_type == str:
            if not isinstance(value, str):
                raise TypeError(f"Column '{column_name}' expects string, but got {type(value).__name__}: {value}")
        
        elif expected_type == int:
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                raise TypeError(f"Column '{column_name}' expects integer, but got {type(value).__name__}: {value}")
            if isinstance(value, float) and value != int(value):
                raise TypeError(f"Column '{column_name}' expects integer, but got float with decimal part: {value}")
        
        elif expected_type == float:
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                raise TypeError(f"Column '{column_name}' expects float, but got {type(value).__name__}: {value}")
        
        elif expected_type == bool:
            if not isinstance(value, bool) and value not in (0, 1):
                raise TypeError(f"Column '{column_name}' expects boolean, but got {type(value).__name__}: {value}")
        
        elif expected_type == "date" or expected_type == "datetime":
            if not isinstance(value, str):
                raise TypeError(f"Column '{column_name}' expects date string, but got {type(value).__name__}: {value}")
            # Could add date format validation here
            
    def apply_type_conversion(self, value, column_type):
        """Apply type-specific conversions for SQL compatibility"""
        # String already has quotes
        if column_type == str:
            # Make sure string values are properly quoted
            if value.startswith("'") and value.endswith("'"):
                return value
            return f"'{value}'" if not value == "NULL" else value
        
        # Integer and float need no special handling in most SQL dialects
        elif column_type in (int, float):
            return value
        
        # Boolean conversion
        elif column_type == bool:
            if value.lower() == "true":
                return "1"  # or TRUE depending on SQL dialect
            elif value.lower() == "false":
                return "0"  # or FALSE depending on SQL dialect
            return value
        
        # Date handling
        elif column_type == "date":
            if not (value.startswith("'") and value.endswith("'")):
                return f"DATE '{value.strip(STRIP_STR)}'"
            return f"DATE {value}"
        
        # Datetime handling
        elif column_type == "datetime":
            if not (value.startswith("'") and value.endswith("'")):
                return f"TIMESTAMP '{value.strip(STRIP_STR)}'"
            return f"TIMESTAMP {value}"
        
        # Default: return as is
        return value
    
    def get_operator(self, op):
        """Convert Python comparison operators to SQL operators"""
        if isinstance(op, ast.Eq):
            return "="
        elif isinstance(op, ast.NotEq):
            return "<>"
        elif isinstance(op, ast.Lt):
            return "<"
        elif isinstance(op, ast.LtE):
            return "<="
        elif isinstance(op, ast.Gt):
            return ">"
        elif isinstance(op, ast.GtE):
            return ">="
        elif isinstance(op, ast.In):
            return "IN"
        elif isinstance(op, ast.NotIn):
            return "NOT IN"
        elif isinstance(op, ast.Is):
            return "IS"
        elif isinstance(op, ast.IsNot):
            return "IS NOT"
        else:
            raise ValueError(f"Unsupported operator: {op.__class__.__name__}")


def lambda_to_sql(lambda_func, type_dict=None):
    """
    Convert a Python lambda function to a SQL condition string
    
    Args:
        lambda_func: A lambda function with a single parameter (row)
        type_dict: Dictionary mapping column names to their Python types
                  e.g., {"name": str, "age": int}
        
    Returns:
        tuple: (sql_string, errors_list)
            - sql_string: The SQL condition string
            - errors_list: List of errors encountered during conversion
    """
    # Get the source code of the lambda function
    try:
        source = inspect.getsource(lambda_func).strip()
    except (IOError, TypeError) as e:
        return "", [f"Failed to get lambda source: {str(e)}"]
    
    # Extract just the lambda expression part
    try:
        lambda_expr = source.split('=', 1)[1].strip()
    except IndexError:
        return "", ["Failed to parse lambda expression. Make sure it's a valid lambda function."]
    
    # Parse the lambda expression into an AST
    try:
        tree = ast.parse(lambda_expr)
    except SyntaxError as e:
        return "", [f"Syntax error in lambda expression: {str(e)}"]
    
    # Extract the lambda body
    try:
        lambda_node = tree.body[0].value
        
        if not isinstance(lambda_node, ast.Lambda):
            return "", ["Input must be a lambda function"]
    except (IndexError, AttributeError):
        return "", ["Failed to extract lambda body. Invalid lambda expression."]
    
    # Apply our transformer to the lambda body
    transformer = LambdaToSQLTransformer(type_dict)
    
    try:
        transformer.visit(lambda_node.body)
    except Exception as e:
        return "", [f"Error during lambda conversion: {str(e)}"]
    
    # If there are errors, return them along with whatever SQL was generated
    if transformer.errors:
        return " ".join(transformer.sql_parts), transformer.errors
    
    # Join all SQL parts and return
    return " ".join(transformer.sql_parts), []


# Example usage
if __name__ == "__main__":
    # Define a type dictionary for our columns
    type_dict = {
        "name": str,
        "age": int,
        "status": str,
        "enrolled": bool,
        "score": float,
        "joined_date": "date"  # Using string for SQL-specific types
    }
    
    # Simple equality
    condition1 = lambda row: row.age == 30
    sql1, errors1 = lambda_to_sql(condition1, type_dict)
    print(f"SQL: {sql1}")
    if errors1:
        print(f"Errors: {errors1}")
    
    # Complex condition with AND
    condition2 = lambda row: row.age > 25 and row.status == 'active'
    sql2, errors2 = lambda_to_sql(condition2, type_dict)
    print(f"\nSQL: {sql2}")
    if errors2:
        print(f"Errors: {errors2}")
    
    # Condition with OR and nested conditions
    condition3 = lambda row: (row.age < 20 or row.age > 60) and row.enrolled == True
    sql3, errors3 = lambda_to_sql(condition3, type_dict)
    print(f"\nSQL: {sql3}")
    if errors3:
        print(f"Errors: {errors3}")
    
    # String methods with SQL translation
    condition4 = lambda row: row.name.startswith("A")
    sql4, errors4 = lambda_to_sql(condition4, type_dict)
    print(f"\nSQL: {sql4}")
    if errors4:
        print(f"Errors: {errors4}")
    
    # Combined string methods
    condition5 = lambda row: row.name.startswith("A") and row.status.endswith("active")
    sql5, errors5 = lambda_to_sql(condition5, type_dict)
    print(f"\nSQL: {sql5}")
    if errors5:
        print(f"Errors: {errors5}")
    
    # String contains
    condition6 = lambda row: row.name.upper().contains("SMITH")
    sql6, errors6 = lambda_to_sql(condition6, type_dict)
    print(f"\nSQL: {sql6}")
    if errors6:
        print(f"Errors: {errors6}")
    
    # Unsupported method
    condition7 = lambda row: row.name.split()[0] == "John"
    sql7, errors7 = lambda_to_sql(condition7, type_dict)
    print(f"\nSQL: {sql7}")
    if errors7:
        print(f"Errors: {errors7}")
    
    # Date handling with extraction
    condition8 = lambda row: row.joined_date.year() == 2023
    sql8, errors8 = lambda_to_sql(condition8, type_dict)
    print(f"\nSQL: {sql8}")
    if errors8:
        print(f"Errors: {errors8}")
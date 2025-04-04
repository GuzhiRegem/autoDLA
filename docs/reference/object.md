## `Object (BaseModel)`
### Class Methods
- > #### **new(`**kwargs: dict`)** -> `Object`
> Creates a new instance of Object based on the arguments passed
- > #### **all(`limit: int = 10`)** -> `list[Object]`
> Get a list with all currently active Object instances
- > #### **filter(`lambda_f: LambdaFunction`, `limit: int = 10`)** -> `list[Object]`
> Get a list with all currently active Object instances that fullfill the condition passed in `lambda_f`
- > #### **get_by_id(`id_param: str`)** -> `Object`
> Returns the active Object instance that has the specified id
- > #### **get_table_res(`limit: int = 10`, `only_current: bool = True`, `only_active: bool = True`)** -> `list[dict]`
> Returns a list of dicts representing the table containing the data for all Object instances
### Instance Methods
- > #### **update(`**kwargs: dict`)** -> `None`
> Modifies current Object instance with passed arguments
- > #### **delete(`**kwargs: dict`)** -> `None`
> Deletes current Object instance
- > #### **history(`**kwargs: dict`)** -> `dict[str, list[dict]]`
> Returns the complete history of rows for current Object instance in all relevant tables
- > #### **to_dict(`**kwargs: dict`)** -> `dict`
> Returns a dictionary representation of current Object instance

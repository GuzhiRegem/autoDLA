import polars as pl
from typing import Dict, List, Tuple, Any, Optional, Union, Callable


class DB:
    def execute(self, query: str) -> pl.DataFrame:
        pass
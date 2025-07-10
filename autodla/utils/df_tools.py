from typing import Literal
import polars as pl

def ensure_dtype_equality(df1: pl.DataFrame, df2: pl.DataFrame) -> None:
    if set(df1.columns) != set(df2.columns):
        raise ValueError("DataFrames have different columns.")
    for col in df1.columns:
        expected_dtype = df1[col].dtype
        if df1[col].dtype == df2[col].dtype:
            expected_dtype = df1[col].dtype
        if df1[col].dtype == pl.Null and df2[col].dtype != pl.Null:
            expected_dtype = df2[col].dtype
        if df2[col].dtype == pl.Null and df1[col].dtype != pl.Null:
            expected_dtype = df1[col].dtype
        if (df1[col].dtype == pl.String) or (df2[col].dtype == pl.String):
            expected_dtype = pl.String
        if expected_dtype == pl.Null:
            continue
        
        # Ensure both DataFrames have the same dtype for the column
        if df1[col].dtype != expected_dtype:
            df1 = df1.with_columns(pl.col(col).cast(expected_dtype))
        if df2[col].dtype != expected_dtype:
            df2 = df2.with_columns(pl.col(col).cast(expected_dtype))
    return df1, df2

        

def df_comparator(df1: pl.DataFrame, df2: pl.DataFrame, method: Literal["insert", "update"]) -> pl.DataFrame:
    df1, df2 = ensure_dtype_equality(df1, df2)
    out = pl.DataFrame(data=None, schema=df1.columns)
    match method:
        case "insert":
            if df1.is_empty() or df2.is_empty():
                return df1
            return df1.join(df2, on="dla_object_id", how="anti")
        case "update":
            if df1.is_empty() or df2.is_empty():
                return out
            joined = df1.join(df2, on="dla_object_id", how="inner", suffix="_ext")

            # Build expression to detect differences between columns (excluding ID)
            diff_exprs = []
            for col in df1.columns:
                if col != "dla_object_id":
                    diff_exprs.append(pl.col(col) != pl.col(f"{col}_ext"))

            if diff_exprs:
                # Combine all diff expressions using logical OR
                condition = diff_exprs[0]
                for expr in diff_exprs[1:]:
                    condition = condition | expr

                # Filter rows where any column differs
                modified_rows = joined.filter(condition)

                if not modified_rows.is_empty():
                    # Optionally, keep only the columns from the current snapshot
                    return modified_rows.select(df1.columns)
    return out

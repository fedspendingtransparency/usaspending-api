from pyspark.sql import functions as sf, Column


def collect_concat(col_name: str | Column, concat_str: str = "; ", alias: str | None = None) -> Column:
    """Aggregates columns into a string of values seperated by some delimiter"""
    if alias is None:
        alias = col_name if isinstance(col_name, str) else str(col_name._jc)
    return sf.concat_ws(concat_str, sf.sort_array(sf.collect_set(col_name))).alias(alias)

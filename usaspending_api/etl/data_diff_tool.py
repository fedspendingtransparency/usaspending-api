from gresearch.spark.diff import *
import pandas as pd
import numpy as np


def data_diff_function(df1, df2):
    #assumes dataframes as a delta table
    table1 = spark.read.load(df1)
    table2 = spark.read.load(df2)
    return (table1.diff(table2, table1.columns).show)


def diff(left: DataFrame, right: DataFrame, unique_key_col="id", compare_cols=[], collect=False):
    # unique_key_col = "id"

    # If compare_cols is left as an empty list, ALL columns will be compared to each other
    # (for rows where unique_key_col values match).
    # Otherwise, specify the columns that determine "sameness" of rows
    # compare_cols = []
    # compare_cols = ["value", "v2"]

    if not compare_cols:
        compare_cols = set(left.schema.names + right.schema.names)
    distinct_stmts = " ".join([f"WHEN l.{c} IS DISTINCT FROM r.{c} THEN 'C'" for c in compare_cols])
    compare_expr = f"""
    CASE 
        WHEN l.exists IS NULL THEN 'I' 
        WHEN r.exists IS NULL THEN 'D' 
        {distinct_stmts}
        ELSE 'N'
    END
    """

    differences = left.withColumn("exists", lit(1)).alias("l").join(
        right.withColumn("exists", lit(1)).alias("r"),
        left[unique_key_col] == right[unique_key_col],
        "fullouter"
    ).withColumn("diff", expr(compare_expr))
    cols_to_show = ["diff"] + [f"l.{c}" for c in compare_cols] + [f"r.{c}" for c in compare_cols]
    differences = differences.select(*cols_to_show)
    # NOTE: .show() shows only a few rows. Call with collect=True to get ALL results, then show
    if collect:
        return differences.collect()
    else:
        differences.show(truncate=False)

"""Integration tests exercising the usaspending_api.common.etl.spark.py ETL utility functions"""

import random
import uuid

from copy import deepcopy
from pyspark.sql import SparkSession, Row
from pytest import raises

from usaspending_api.common.etl.spark import diff


def test_diff_no_changes(spark: SparkSession):
    """Diff the same data in two different DataFrames to each other, and ensure no differences found"""
    data_left = [
        {"first_col": "row 1", "id": str(uuid.uuid4()), "color": "blue", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 2", "id": str(uuid.uuid4()), "color": "green", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 3", "id": str(uuid.uuid4()), "color": "pink", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 4", "id": str(uuid.uuid4()), "color": "yellow", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 5", "id": str(uuid.uuid4()), "color": "red", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 6", "id": str(uuid.uuid4()), "color": "orange", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 7", "id": str(uuid.uuid4()), "color": "magenta", "numeric_val": random.randint(-100, 100)},
    ]

    data_right = deepcopy(data_left)

    left_df = spark.createDataFrame([Row(**data_row) for data_row in data_left])
    right_df = spark.createDataFrame([Row(**data_row) for data_row in data_right])

    diff_df = diff(left_df, right_df)

    assert diff_df.count() == 0


def test_diff_no_changes_include_unchanged(spark: SparkSession):
    """Diff the same data in two different DataFrames to each other, and ask that all results be shown. Ensure all
    are shown, but all are 'N' = no change"""
    data_left = [
        {"first_col": "row 1", "id": str(uuid.uuid4()), "color": "blue", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 2", "id": str(uuid.uuid4()), "color": "green", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 3", "id": str(uuid.uuid4()), "color": "pink", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 4", "id": str(uuid.uuid4()), "color": "yellow", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 5", "id": str(uuid.uuid4()), "color": "red", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 6", "id": str(uuid.uuid4()), "color": "orange", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 7", "id": str(uuid.uuid4()), "color": "magenta", "numeric_val": random.randint(-100, 100)},
    ]

    data_right = deepcopy(data_left)

    left_df = spark.createDataFrame([Row(**data_row) for data_row in data_left])
    right_df = spark.createDataFrame([Row(**data_row) for data_row in data_right])

    diff_df = diff(left_df, right_df, include_unchanged_rows=True)

    diff_rows = diff_df.count()
    assert diff_rows > 0
    assert diff_rows == len(data_left)


def test_diff_one_change(spark: SparkSession):
    """Diff two DataFrames that differ by one row and ensure 'C' is found"""
    data_left = [
        {"first_col": "row 1", "id": str(uuid.uuid4()), "color": "blue", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 2", "id": str(uuid.uuid4()), "color": "green", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 3", "id": str(uuid.uuid4()), "color": "pink", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 4", "id": str(uuid.uuid4()), "color": "yellow", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 5", "id": str(uuid.uuid4()), "color": "red", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 6", "id": str(uuid.uuid4()), "color": "orange", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 7", "id": str(uuid.uuid4()), "color": "magenta", "numeric_val": random.randint(-100, 100)},
    ]

    data_right = deepcopy(data_left)
    changed_row_id = data_right[0]["id"]
    data_right[0]["numeric_val"] = data_right[0]["numeric_val"] * 2

    left_df = spark.createDataFrame([Row(**data_row) for data_row in data_left])
    right_df = spark.createDataFrame([Row(**data_row) for data_row in data_right])

    diff_df = diff(left_df, right_df)

    diff_row_count = diff_df.count()
    assert diff_row_count == 1
    assert [f.name for f in diff_df.schema.fields] == [
        "diff",
        "id",
        "id",
        "first_col",
        "first_col",
        "color",
        "color",
        "numeric_val",
        "numeric_val",
    ]
    diff_df_data = diff_df.collect()
    assert diff_df_data[0][0] == "C"
    assert diff_df_data[0].diff == "C"
    assert diff_df_data[0][1] == changed_row_id
    assert diff_df_data[0][2] == changed_row_id
    assert diff_df_data[0][3] == diff_df_data[0][4]
    assert diff_df_data[0][5] == diff_df_data[0][6]
    assert diff_df_data[0][7] != diff_df_data[0][8]
    assert diff_df_data[0][8] == 2 * diff_df_data[0][7]


def test_diff_one_insert(spark: SparkSession):
    """Diff two DataFrames where the right has one more record than left"""
    data_left = [
        {"first_col": "row 1", "id": str(uuid.uuid4()), "color": "blue", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 2", "id": str(uuid.uuid4()), "color": "green", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 3", "id": str(uuid.uuid4()), "color": "pink", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 4", "id": str(uuid.uuid4()), "color": "yellow", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 5", "id": str(uuid.uuid4()), "color": "red", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 6", "id": str(uuid.uuid4()), "color": "orange", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 7", "id": str(uuid.uuid4()), "color": "magenta", "numeric_val": random.randint(-100, 100)},
    ]

    data_right = deepcopy(data_left)
    d = {"first_col": "row 8", "id": str(uuid.uuid4()), "color": "chartreuse", "numeric_val": random.randint(-100, 100)}
    data_right += [d]
    changed_row_id = data_right[-1]["id"]

    left_df = spark.createDataFrame([Row(**data_row) for data_row in data_left])
    right_df = spark.createDataFrame([Row(**data_row) for data_row in data_right])

    diff_df = diff(left_df, right_df)

    diff_row_count = diff_df.count()
    assert diff_row_count == 1
    assert [f.name for f in diff_df.schema.fields] == [
        "diff",
        "id",
        "id",
        "first_col",
        "first_col",
        "color",
        "color",
        "numeric_val",
        "numeric_val",
    ]
    diff_df_data = diff_df.collect()
    assert diff_df_data[0][0] == "I"
    assert diff_df_data[0].diff == "I"
    assert diff_df_data[0][1] is None
    assert diff_df_data[0][2] == changed_row_id
    assert diff_df_data[0][3] is None
    assert diff_df_data[0][4] == d["first_col"]
    assert diff_df_data[0][5] is None
    assert diff_df_data[0][6] == d["color"]
    assert diff_df_data[0][7] is None
    assert diff_df_data[0][8] == d["numeric_val"]


def test_diff_one_delete(spark: SparkSession):
    """Diff two DataFrames where the right has one less record than left"""
    data_left = [
        {"first_col": "row 1", "id": str(uuid.uuid4()), "color": "blue", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 2", "id": str(uuid.uuid4()), "color": "green", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 3", "id": str(uuid.uuid4()), "color": "pink", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 4", "id": str(uuid.uuid4()), "color": "yellow", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 5", "id": str(uuid.uuid4()), "color": "red", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 6", "id": str(uuid.uuid4()), "color": "orange", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 7", "id": str(uuid.uuid4()), "color": "magenta", "numeric_val": random.randint(-100, 100)},
    ]

    data_right = deepcopy(data_left)
    d = data_right.pop(3)
    changed_row_id = d["id"]

    left_df = spark.createDataFrame([Row(**data_row) for data_row in data_left])
    right_df = spark.createDataFrame([Row(**data_row) for data_row in data_right])

    diff_df = diff(left_df, right_df)

    diff_row_count = diff_df.count()
    assert diff_row_count == 1
    assert [f.name for f in diff_df.schema.fields] == [
        "diff",
        "id",
        "id",
        "first_col",
        "first_col",
        "color",
        "color",
        "numeric_val",
        "numeric_val",
    ]
    diff_df_data = diff_df.collect()
    assert diff_df_data[0][0] == "D"
    assert diff_df_data[0].diff == "D"
    assert diff_df_data[0][1] == changed_row_id
    assert diff_df_data[0][2] is None
    assert diff_df_data[0][3] == d["first_col"]
    assert diff_df_data[0][4] is None
    assert diff_df_data[0][5] == d["color"]
    assert diff_df_data[0][6] is None
    assert diff_df_data[0][7] == d["numeric_val"]
    assert diff_df_data[0][8] is None


def test_diff_schema_mismatch(spark: SparkSession):
    """Diff DataFrames that have mismatched columns, and ensure ValueError is thrown"""
    data_left = [
        {"first_col": "row 1", "id": str(uuid.uuid4()), "color": "blue", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 2", "id": str(uuid.uuid4()), "color": "green", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 3", "id": str(uuid.uuid4()), "color": "pink", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 4", "id": str(uuid.uuid4()), "color": "yellow", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 5", "id": str(uuid.uuid4()), "color": "red", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 6", "id": str(uuid.uuid4()), "color": "orange", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 7", "id": str(uuid.uuid4()), "color": "magenta", "numeric_val": random.randint(-100, 100)},
    ]

    data_right = [
        {"first_col": "row 1", "id": str(uuid.uuid4()), "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 2", "id": str(uuid.uuid4()), "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 3", "id": str(uuid.uuid4()), "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 4", "id": str(uuid.uuid4()), "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 5", "id": str(uuid.uuid4()), "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 6", "id": str(uuid.uuid4()), "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 7", "id": str(uuid.uuid4()), "numeric_val": random.randint(-100, 100)},
    ]

    left_df = spark.createDataFrame([Row(**data_row) for data_row in data_left])
    right_df = spark.createDataFrame([Row(**data_row) for data_row in data_right])

    with raises(ValueError) as exc_info:
        diff(left_df, right_df)
    assert exc_info.match("do not contain the same columns")


def test_diff_one_change_single_col_compared(spark: SparkSession):
    """Diff two different DataFrames that have different values, specify only the changed column to compare,
    and ensure it finds the change"""
    data_left = [
        {"first_col": "row 1", "id": str(uuid.uuid4()), "color": "blue", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 2", "id": str(uuid.uuid4()), "color": "green", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 3", "id": str(uuid.uuid4()), "color": "pink", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 4", "id": str(uuid.uuid4()), "color": "yellow", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 5", "id": str(uuid.uuid4()), "color": "red", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 6", "id": str(uuid.uuid4()), "color": "orange", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 7", "id": str(uuid.uuid4()), "color": "magenta", "numeric_val": random.randint(-100, 100)},
    ]

    data_right = deepcopy(data_left)
    changed_row_id = data_right[0]["id"]
    data_right[0]["numeric_val"] = data_right[0]["numeric_val"] + 1

    left_df = spark.createDataFrame([Row(**data_row) for data_row in data_left])
    right_df = spark.createDataFrame([Row(**data_row) for data_row in data_right])

    diff_df = diff(left_df, right_df, compare_cols=["numeric_val"])

    diff_row_count = diff_df.count()
    assert diff_row_count == 1
    assert len(diff_df.columns) == 5
    assert [f.name for f in diff_df.schema.fields] == [
        "diff",
        "id",
        "id",
        "numeric_val",
        "numeric_val",
    ]
    diff_df_data = diff_df.collect()
    assert diff_df_data[0][0] == "C"
    assert diff_df_data[0].diff == "C"
    assert diff_df_data[0][1] == changed_row_id
    assert diff_df_data[0][2] == changed_row_id
    assert diff_df_data[0][4] == diff_df_data[0][3] + 1


def test_diff_one_change_changed_column_not_compared(spark: SparkSession):
    """Diff two different DataFrames that have different values, specify only the unchanged columns to compare,
    and ensure it finds no changes"""
    data_left = [
        {"first_col": "row 1", "id": str(uuid.uuid4()), "color": "blue", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 2", "id": str(uuid.uuid4()), "color": "green", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 3", "id": str(uuid.uuid4()), "color": "pink", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 4", "id": str(uuid.uuid4()), "color": "yellow", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 5", "id": str(uuid.uuid4()), "color": "red", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 6", "id": str(uuid.uuid4()), "color": "orange", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 7", "id": str(uuid.uuid4()), "color": "magenta", "numeric_val": random.randint(-100, 100)},
    ]

    data_right = deepcopy(data_left)
    data_right[0]["numeric_val"] = data_right[0]["numeric_val"] * 2

    left_df = spark.createDataFrame([Row(**data_row) for data_row in data_left])
    right_df = spark.createDataFrame([Row(**data_row) for data_row in data_right])

    diff_df = diff(left_df, right_df, compare_cols=["color"])

    diff_row_count = diff_df.count()
    assert diff_row_count == 0

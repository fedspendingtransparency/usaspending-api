"""Creating a Test Model and table spec to be agnostic to the actual data"""

from django.db import models


class TestModel(models.Model):
    id = models.IntegerField(primary_key=True, help_text="surrogate primary key defined in Broker")
    test_timestamp = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "test_table"


TEST_MODEL_COLUMNS = {
    "id": {"delta": "LONG NOT NULL", "postgres": "BIGINT NOT NULL"},
    "test_timestamp": {"delta": "TIMESTAMP", "postgres": "TIMESTAMP WITH TIME ZONE"},
}

TEST_TABLE_POSTGRES = "CREATE TABLE test_table(id BIGINT NOT NULL, test_timestamp TIMESTAMP WITH TIME ZONE)"
TEST_TABLE_DELTA = rf"""
    CREATE OR REPLACE TABLE {{DESTINATION_TABLE}} (
        {", ".join([f'{key} {val["delta"]}' for key, val in TEST_MODEL_COLUMNS.items()])}
    )
    USING DELTA
    LOCATION 's3a://{{SPARK_S3_BUCKET}}/{{DELTA_LAKE_S3_PATH}}/{{DESTINATION_DATABASE}}/{{DESTINATION_TABLE}}'
"""

TEST_TABLE_SPEC = {
    "test_table": {
        "model": TestModel,
        "is_from_broker": False,
        "source_table": "test_table",
        "source_database": "temp",
        "destination_database": "temp",
        "swap_table": None,
        "swap_schema": None,
        "partition_column": "id",
        "partition_column_type": "numeric",
        "is_partition_column_unique": True,
        "delta_table_create_sql": TEST_TABLE_DELTA,
        "source_schema": None,
        "custom_schema": "",
        "column_names": ["id", "test_timestamp"],
        "tsvectors": None,
    }
}

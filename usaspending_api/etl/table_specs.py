from dataclasses import dataclass
from typing import Any, Callable, Literal

from django.db import models
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


@dataclass(kw_only=True)
class TableSpec:
    destination_database: Literal["arc", "int", "raw", "rpt", "test"]
    delta_table_create_sql: str | StructType
    column_names: list[str] | None = None
    model: models.Model | None = None
    is_from_broker: bool = False
    source_table: str | None = None
    source_database: Literal["public", "int", "raw", "rpt"] | None = None
    swap_table: str | None = None
    swap_schema: str | None = None
    partition_column: str | None = None
    partition_column_type: Literal["date", "numeric"] | None = None
    is_partition_column_unique: bool = False
    source_schema: dict[str, str] | None = None
    custom_schema: str | None = None
    delta_table_create_options: dict[str, str | bool] | None = None
    delta_table_create_partitions: list[str] | None = None
    tsvectors: dict[str, list[str]] | None = None


@dataclass(kw_only=True)
class QueryTableSpec(TableSpec):
    source_query: (
        str
        | Callable[[SparkSession, str, str], None]
        | list[str]
        | list[Callable[[SparkSession, str, str], None]]
        | None
    ) = None
    source_query_incremental: (
        str
        | Callable[[SparkSession, str, str], None]
        | list[str]
        | list[Callable[[SparkSession, str, str], None]]
        | None
    ) = None
    postgres_seq_name: str | None = None
    postgres_partition_spec: dict[str, Any] | None = None


@dataclass(kw_only=True)
class ArchiveTableSpec(TableSpec):
    destination_table: str
    archive_date_field: str

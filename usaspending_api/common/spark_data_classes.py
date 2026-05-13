from dataclasses import dataclass
from typing import Callable

from pyspark.sql.types import StructType


@dataclass(slots=True)
class TableSpec:
    """
    Delta table metadata class
    """

    destination_database: str
    delta_table_create_sql: str | StructType

    destination_table_name: str | None = None
    source_table: str | None = None
    source_database: str | None = None
    model: str | None = None
    partition_column: str | None = None
    partition_column_type: str | None = None
    is_partition_column_unique: bool = False
    delta_table_create_options: dict | None = None
    delta_table_create_partitions: list[str] | None = None
    source_schema: list | dict | None = None
    custom_schema: str | None = None
    column_names: list[str] | None = None
    is_from_broker: bool = False
    source_query: str | list[str] | Callable | None = None
    source_query_incremental: str | Callable | None = None
    user_defined_functions: list[dict] | None = None
    archive_data_field: str = "update_date"
    postgres_seq_name: str | None = None
    postgres_partition_spec: dict | None = None
    tsvectors: list[str] | dict | None = None
    swap_table: str | None = None
    swap_schema: str | None = None
    primary_key: str | None = None

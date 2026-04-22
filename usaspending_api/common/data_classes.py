from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Union

from typing_extensions import Literal


@dataclass
class Pagination:
    page: int
    limit: int
    lower_limit: int
    upper_limit: int
    sort_key: Optional[str] = None
    sort_order: Optional[str] = None
    secondary_sort_key: Optional[str] = None

    @property
    def _sort_order_field_prefix(self) -> str:
        if self.sort_order == "desc":
            return "-"
        return ""

    @property
    def order_by(self) -> str:
        return f"{self._sort_order_field_prefix}{self.sort_key}"

    @property
    def robust_order_by_fields(self) -> tuple[str] | tuple[str, str]:
        return (
            (self.order_by,)
            if self.secondary_sort_key is None
            else (self.order_by, f"{self._sort_order_field_prefix}{self.secondary_sort_key}")
        )


@dataclass
class TransactionColumn:
    dest_name: str
    source: Optional[str]
    delta_type: str
    handling: Literal[
        "cast", "leave_null", "literal", "normal", "parse_string_datetime_to_date", "string_datetime_remove_timestamp"
    ] = "normal"
    # Columns can optionally have a transformation defined on the source column.
    #   This transformation must be a scalar function or operation and PSQL compatible.
    #   The SQL must not alias the output of the scalar function as that's done automatically.
    #   The scalar transformation should include a named placeholder, called "input", in the string which will allow
    #   calling code to format the string with a input. You should expect the scalar transformation
    #   to be applied on this input. For example, a valid scalar_transformation string is
    #   "CASE {input} WHEN 'UNITED STATES' THEN 'USA' ELSE {input} END"
    scalar_transformation: str = None


@dataclass(slots=True)
class TableSpec:
    """
    Delta table metadata class
    """

    destination_database: str
    delta_table_create_sql: Union[str, Any]   # pyspark.sql.types.StructType

    destination_table_name: Optional[str] = None
    source_table: Optional[str] = None
    source_database: Optional[str] = None
    model: Optional[str] = None
    partition_column: Optional[str] = None
    partition_column_type: Optional[str] = None
    is_partition_column_unique: bool = False
    delta_table_create_options: Optional[dict] = None
    delta_table_create_partitions: Optional[List[str]] = None
    source_schema: Optional[Union[List, dict]] = None
    custom_schema: Optional[str] = None
    column_names: Optional[List[str]] = None
    is_from_broker: bool = False
    source_query: Optional[Union[str, List[str], Callable]] = None
    source_query_incremental: Optional[Union[str, Callable]] = None
    user_defined_functions: Optional[List[dict]] = None
    archive_data_field: str = "update_date"
    postgres_seq_name: Optional[str] = None
    postgres_partition_spec: Optional[dict] = None
    tsvectors: Optional[Union[List[str], dict]] = None
    swap_table: Optional[str] = None
    swap_schema: Optional[str] = None

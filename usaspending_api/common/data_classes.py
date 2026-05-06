from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Optional

if TYPE_CHECKING:
    from pyspark.sql.types import StructType
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

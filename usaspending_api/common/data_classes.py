from dataclasses import dataclass
from typing import Optional
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
    def _sort_order_field_prefix(self):
        if self.sort_order == "desc":
            return "-"
        return ""

    @property
    def order_by(self):
        return f"{self._sort_order_field_prefix}{self.sort_key}"

    @property
    def robust_order_by_fields(self):
        return (self.order_by, f"{self._sort_order_field_prefix}{self.secondary_sort_key}")


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

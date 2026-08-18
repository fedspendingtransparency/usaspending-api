import csv
import io
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable

from usaspending_api.config import CONFIG
from usaspending_api.etl.models import CDFVersionTracking
from usaspending_api.settings import IS_LOCAL

logger = logging.getLogger(__name__)
CDF_METADATA_COLUMNS = ("_change_type", "_commit_version", "_commit_timestamp")
UPSERT_CHANGE_TYPES = ("insert", "update_postimage")


@dataclass
class CDFChangeSet:
    cdf: pa.Table
    latest_version: int
    latest_commit_timestamp: datetime


def _format_pg_array_literal(items: list) -> str | None:
    """Convert a Python list to a Postgres array literal."""

    if items is None:
        return None

    parts = []
    for element in items:
        if element is None:
            parts.append("NULL")
        else:
            escaped = str(element).replace("\\", "\\\\").replace('"', '\\"')
            parts.append(escaped)
    return "{" + ",".join(parts) + "}"


def arrow_to_pg_csv_buffer(table: pa.Table, column_order: list[str]) -> io.StringIO:
    """Convert a PyArrow table to an in-memory CSV buffer compatible with Postgres COPY.
    Columns are reordered to match `column_order`. The list of columns are rendered as Postgres array
        literals (e.g. `{"a", "b"}`).

    Args:
        table: Source PyArrow table to be converted.
        column_order: Column order of columns in the in-memory CSV buffer.

    Returns:
        In-memory CSV buffer
    """

    table = table.select(column_order)

    list_columns = [
        field.name for field in table.schema if pa.types.is_list(field.type) or pa.types.is_large_list(field.type)
    ]

    for col in list_columns:
        col_array = table.column(col).to_pylist()
        formatted_array = [
            _format_pg_array_literal(item) if item is not None else None for item in col_array
        ]
        table = table.set_column(
            table.schema.get_field_index(col),
            col,
            pa.array(formatted_array, type=pa.string())
        )

    integer_columns = []

    for field in table.schema:
        if pa.types.is_integer(field.type):
            integer_columns.append(field.name)

        if pa.types.is_timestamp(field.type) or pa.types.is_date(field.type):
            col = table.column(field.name)
            string_values = []

            for chunk in col.chunks:
                for i in range(len(chunk)):
                    scalar = chunk[i]
                    if not scalar.is_valid:
                        string_values.append(None)
                    else:
                        try:
                            val = scalar.as_py()
                            string_values.append(str(val))
                        except (OverflowError, ValueError):
                            string_values.append(None)

            table = table.set_column(
                table.schema.get_field_index(field.name),
                field.name,
                pa.array(string_values, type=pa.string())
            )

    df = table.to_pandas()

    for col_name in integer_columns:
        if col_name in df.columns:
            df[col_name] = df[col_name].astype('Int64')

    buffer = io.StringIO()
    df.to_csv(buffer, header=False, index=False, na_rep="", quoting=csv.QUOTE_MINIMAL)
    buffer.seek(0)

    return buffer


def ids_to_csv_buffer(ids: Iterable) -> io.StringIO:
    """Convert an iterable of scalar IDs to a single column CSV buffer."""

    buffer = io.StringIO()
    writer = csv.writer(buffer)

    for id in ids:
        writer.writerow([id])

    buffer.seek(0)

    return buffer


def get_last_processed_version(table_schema: str, table_name: str) -> int | None:
    """Return the last processed CDF version for the given table, or `None` if never processed.

    Args:
        table_schema: The schema that the table is in.
            Example: "rpt"
        table_name: The name of the Delta table.
            Example: "transaction_search"
    """

    try:
        return CDFVersionTracking.objects.values_list(
            "last_processed_version", flat=True
        ).get(table_schema=table_schema, table_name=table_name)
    except CDFVersionTracking.DoesNotExist:
        return None


def update_last_processed_version(table_schema: str, table_name: str, version: int, commit_timestamp: datetime) -> None:
    """Update the CDF tracking information for the given table"""

    CDFVersionTracking.objects.update_or_create(
        table_schema=table_schema,
        table_name=table_name,
        defaults={
            "last_processed_version": version,
            "last_commit_timestamp": commit_timestamp,
        }
    )


def build_delta_table_s3_uri(destination_database: str, destination_table: str) -> str:
    return f"s3://{CONFIG.SPARK_S3_BUCKET}/{CONFIG.DELTA_LAKE_S3_PATH}/{destination_database}/{destination_table}"


def _get_storage_options() -> dict | None:
    """Build AWS connection information"""

    if CONFIG.USE_AWS:
        return None

    if IS_LOCAL and not CONFIG.AWS_S3_ENDPOINT.startswith("http://"):
        aws_s3_endpoint = f"http://{CONFIG.AWS_S3_ENDPOINT}"
    else:
        aws_s3_endpoint = CONFIG.AWS_S3_ENDPOINT

    return {
        "AWS_ACCESS_KEY_ID": CONFIG.AWS_ACCESS_KEY.get_secret_value(),
        "AWS_SECRET_ACCESS_KEY": CONFIG.AWS_SECRET_KEY.get_secret_value(),
        "AWS_REGION": CONFIG.AWS_REGION,
        "AWS_ENDPOINT_URL": aws_s3_endpoint,
        "AWS_ALLOW_HTTP": "true" if IS_LOCAL else "false"
    }


def read_cdf_changes(delta_table_uri: str, starting_version: int) -> CDFChangeSet | None:
    """Read CDF entries from `starting_version` to the latest committed version. Returns `None` if there are no new
        commits to process or if the CDF is empty.
    """

    # Debug
    logger.info(f"Delta table URI: {delta_table_uri}")
    logger.info(f"Starting version: {starting_version}")

    dt = DeltaTable(delta_table_uri, storage_options=_get_storage_options())
    latest_version = dt.version()
    logger.info(f"Latest Delta table version: {latest_version}")

    if starting_version >= latest_version:
        logger.info(
            f"No new CDF changes to process: starting_version={starting_version}, latest_version={latest_version}"
        )
        return None

    logger.info(f"Loading CDF from {delta_table_uri} for versions {starting_version + 1}..{latest_version}")
    cdf = dt.load_cdf(starting_version=starting_version + 1, ending_version=latest_version).read_all()

    if cdf.num_rows == 0:
        return None

    max_timestamp = pc.max(pa.chunked_array(cdf.column("_commit_timestamp"))).as_py()
    logger.info(f"Loaded {cdf.num_rows:,} CDF rows. latest_version={latest_version}")

    return CDFChangeSet(cdf=cdf, latest_version=latest_version, latest_commit_timestamp=max_timestamp)


def split_cdf_by_change_type(cdf: pa.Table, pk_column: str) -> tuple[list, pa.Table]:
    """Return (deleted_ids, upsert_rows) from the CDF Arrow table.

    `deleted_ids` includes every PK that appears in the CDF (any change_type). We delete these PKs before inserting
        upserts so the delete-then-insert pattern avoids PK conflicts, and so retries after a partial failure remain
        safe.

    `upsert_rows` contains rows whose final (latest _commmit_version) change_type is `insert` or `upsert_postimage`.
        Rows whose final states is a delete are excluded so insert-then-delete within the CDF window does not re-insert
        a row. CDF metadata columns are stripped from the returned Arrow table.
    """

    # Cast all `string_view` columns to `string`
    new_schema = pa.schema([
        pa.field(field.name, pa.string() if pa.types.is_string_view(field.type) else field.type)
        for field in cdf.schema
    ])
    cdf = cdf.cast(new_schema)

    deleted_ids = pc.unique(pa.chunked_array(cdf.column(pk_column))).to_pylist()
    non_preimage = cdf.filter(pc.not_equal(cdf.column("_change_type"), "update_preimage"))

    if non_preimage.num_rows == 0:
        upsert_rows = non_preimage
    else:
        sorted_table = non_preimage.sort_by([(pk_column, "ascending"), ("_commit_version", "ascending")])
        pk_array = sorted_table.column(pk_column).combine_chunks()
        pk_current = pk_array[:-1]
        pk_next = pk_array[1:]
        changes = pc.not_equal(pk_current, pk_next)
        keep_mask = pa.concat_arrays([changes, pa.array([True])])
        deduplicated = sorted_table.filter(keep_mask)
        upsert_rows = deduplicated.filter(
            pc.is_in(deduplicated.column("_change_type"), value_set=pa.array(UPSERT_CHANGE_TYPES))
        )

    cols_to_drop = [c for c in CDF_METADATA_COLUMNS if c in upsert_rows.column_names]
    if cols_to_drop:
        upsert_rows = upsert_rows.drop_columns(cols_to_drop)

    return deleted_ids, upsert_rows

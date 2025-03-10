"""
WARNING WARNING WARNING
!!!!!!!!!!!!!!!!!!!!!!!
This module must be managed very carefully and stay lean.

The main functions: copy_csv_from_s3_to_pg and copy_csvs_from_s3_to_pg
are used in distributed/parallel/multiprocess execution (by Spark) and is
pickled via cloudpickle. As such it must not have any presumed setup code that would have run (like Django setup,
logging configuration, etc.) and must encapsulate all of those dependencies (like logging config) on its own.

Adding new imports to this module may inadvertently introduce a dependency that can't be pickled.

As it stands, even if new imports are added to the modules it already imports, it could lead to a problem.
"""

import boto3
import gzip
import logging
import psycopg2
import tempfile
import time

from typing import Iterable, List

from botocore.client import BaseClient

from usaspending_api.common.helpers.s3_helpers import download_s3_object
from usaspending_api.common.logging import AbbrevNamespaceUTCFormatter, ensure_logging
from usaspending_api.config import CONFIG
from usaspending_api.settings import LOGGING

logger = logging.getLogger(__name__)


def _get_boto3_s3_client() -> BaseClient:
    if not CONFIG.USE_AWS:
        boto3_session = boto3.session.Session(
            region_name=CONFIG.AWS_REGION,
            aws_access_key_id=CONFIG.AWS_ACCESS_KEY.get_secret_value(),
            aws_secret_access_key=CONFIG.AWS_SECRET_KEY.get_secret_value(),
        )
        s3_client = boto3_session.client(
            service_name="s3",
            region_name=CONFIG.AWS_REGION,
            endpoint_url=f"http://{CONFIG.AWS_S3_ENDPOINT}",
        )
    else:
        s3_client = boto3.client(
            service_name="s3",
            region_name=CONFIG.AWS_REGION,
            endpoint_url=f"https://{CONFIG.AWS_S3_ENDPOINT}",
        )
    return s3_client


def _download_and_copy(
    configured_logger: logging.Logger,
    cursor: psycopg2._psycopg.cursor,
    s3_client: BaseClient,
    s3_bucket_name: str,
    s3_obj_key: str,
    target_pg_table: str,
    ordered_col_names: List[str],
    gzipped: bool,
    partition_prefix: str = "",
):
    """Download a CSV file from S3 then COPY it into a Postgres table using the SQL bulk COPY command. The CSV should
    not include a header row with column names
    """
    start = time.time()
    configured_logger.info(f"{partition_prefix}Starting write of {s3_obj_key}")

    with tempfile.NamedTemporaryFile(delete=True) as temp_file:
        download_s3_object(s3_bucket_name, s3_obj_key, temp_file.name, s3_client=s3_client)

        try:
            if gzipped:
                with gzip.open(temp_file.name, "rb") as csv_file:
                    cursor.copy_expert(
                        sql=f"COPY {target_pg_table} ({','.join(ordered_col_names)}) FROM STDIN (FORMAT CSV)",
                        file=csv_file,
                    )
            else:
                with open(temp_file.name, "r", encoding="utf-8") as csv_file:
                    cursor.copy_expert(
                        sql=f"COPY {target_pg_table} ({','.join(ordered_col_names)}) FROM STDIN (FORMAT CSV)",
                        file=csv_file,
                    )

            elapsed = time.time() - start
            rows_copied = cursor.rowcount
            configured_logger.info(
                f"{partition_prefix}Finished writing {rows_copied} row(s) in {elapsed:.3f}s for {s3_obj_key}"
            )
            yield rows_copied

        except Exception as exc:
            configured_logger.error(f"{partition_prefix}ERROR writing {s3_obj_key}")
            configured_logger.exception(exc)
            raise exc


def copy_csv_from_s3_to_pg(
    s3_bucket_name: str,
    s3_obj_key: str,
    db_dsn: str,
    target_pg_table: str,
    ordered_col_names: List[str],
    gzipped: bool = True,
    work_mem_override: int = None,
):
    """Download a CSV file from S3 then stream it into a Postgres table using the SQL bulk COPY command

    WARNING: See note above in module docstring about this function being pickle-able, and maintaining a lean set of
    outward dependencies on other modules/code that require setup/config.
    """
    ensure_logging(logging_config_dict=LOGGING, formatter_class=AbbrevNamespaceUTCFormatter, logger_to_use=logger)
    try:
        with psycopg2.connect(dsn=db_dsn) as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                if work_mem_override:
                    cursor.execute("SET work_mem TO %s", (work_mem_override,))
                s3_client = _get_boto3_s3_client()
                results_generator = _download_and_copy(
                    configured_logger=logger,
                    cursor=cursor,
                    s3_client=s3_client,
                    s3_bucket_name=s3_bucket_name,
                    s3_obj_key=s3_obj_key,
                    target_pg_table=target_pg_table,
                    gzipped=gzipped,
                    ordered_col_names=ordered_col_names,
                )
                return list(results_generator)[0]
    except Exception as exc:
        logger.error(f"ERROR writing {s3_obj_key}")
        logger.exception(exc)
        raise exc


def copy_csvs_from_s3_to_pg(
    batch_num: int,
    s3_bucket_name: str,
    s3_obj_keys: Iterable[str],
    db_dsn: str,
    target_pg_table: str,
    ordered_col_names: List[str],
    gzipped: bool = True,
    work_mem_override: int = None,
):
    """An optimized form of ``copy_csv_from_s3_to_pg`` that can save on runtime by instantiating the psycopg2 DB
    connection and s3_client only once per partition, where a partition could represent processing several files
    """
    ensure_logging(logging_config_dict=LOGGING, formatter_class=AbbrevNamespaceUTCFormatter, logger_to_use=logger)
    s3_obj_keys = list(s3_obj_keys)  # convert from Iterator (generator) to a concrete List
    batch_size = len(s3_obj_keys)
    batch_start = time.time()
    partition_prefix = f"Partition#{batch_num}: "
    logger.info(f"{partition_prefix}Starting write of a batch of {batch_size} on partition {batch_num}")
    try:
        with psycopg2.connect(dsn=db_dsn) as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                if work_mem_override:
                    cursor.execute("SET work_mem TO %s", (work_mem_override,))
                s3_client = _get_boto3_s3_client()
                for s3_obj_key in s3_obj_keys:
                    yield from _download_and_copy(
                        configured_logger=logger,
                        cursor=cursor,
                        s3_client=s3_client,
                        s3_bucket_name=s3_bucket_name,
                        s3_obj_key=s3_obj_key,
                        target_pg_table=target_pg_table,
                        ordered_col_names=ordered_col_names,
                        gzipped=gzipped,
                        partition_prefix=partition_prefix,
                    )
                batch_elapsed = time.time() - batch_start
                logger.info(
                    f"{partition_prefix}Finished writing batch of {batch_size} "
                    f"{'file' if batch_size == 1 else 'files'} on partition {batch_num} in {batch_elapsed:.3f}s"
                )
    except Exception as exc_batch:
        logger.error(f"{partition_prefix}ERROR writing batch/partition number {batch_num}")
        logger.exception(exc_batch)
        raise exc_batch

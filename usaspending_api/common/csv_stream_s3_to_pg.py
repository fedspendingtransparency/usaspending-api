"""
WARNING WARNING WARNING
!!!!!!!!!!!!!!!!!!!!!!!
This module must be managed very carefully and stay lean.

The main functions: copy_csv_from_s3_to_pg and copy_csvs_from_s3_to_pg
are used in distributed/parallel/multiprocess execution (by Spark) and is
picked via cloudpickle. As such it must not have any presumed setup code that would have run (like Django setup,
logging configuration, etc.) and must encapsulate all of those dependencies (like logging config) on its own.

Adding new imports to this module may inadvertenly introduce a dependency that can't be picked.

As it stands, even if new imports are added to the modules it already imports, it could lead to a problem.
"""
import boto3
import time
import psycopg2
import codecs
import gzip
import logging

from contextlib import closing
from typing import Iterable, List

from botocore.client import BaseClient

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


def _stream_and_copy(
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
    start = time.time()
    configured_logger.info(f"{partition_prefix}Starting write of {s3_obj_key}")
    try:
        s3_obj = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_obj_key)
        # Getting Body gives a botocore.response.StreamingBody object back to allow "streaming" its contents
        s3_obj_body = s3_obj["Body"]
        with closing(s3_obj_body):  # make sure to close the stream when done
            if gzipped:
                with gzip.open(s3_obj_body, "rb") as csv_binary:
                    cursor.copy_expert(
                        sql=f"COPY {target_pg_table} ({','.join(ordered_col_names)}) FROM STDIN (FORMAT CSV)",
                        file=csv_binary,
                    )
            else:
                with codecs.getreader("utf-8")(s3_obj_body) as csv_stream_reader:
                    cursor.copy_expert(
                        sql=f"COPY {target_pg_table} ({','.join(ordered_col_names)}) FROM STDIN (FORMAT CSV)",
                        file=csv_stream_reader,
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
    """Stream a CSV file from S3 into a Postgres table using the SQL bulk COPY command

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
                results_generator = _stream_and_copy(
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
                    yield from _stream_and_copy(
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

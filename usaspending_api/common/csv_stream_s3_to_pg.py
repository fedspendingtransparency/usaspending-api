"""
WARNING WARNING WARNING
!!!!!!!!!!!!!!!!!!!!!!!
This module must be managed very carefully and stay lean.

The main function: copy_csv_from_s3_to_pg is used in distributed/parallel/multiprocess execution (by Spark) and is
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

from usaspending_api.common.logging import AbbrevNamespaceUTCFormatter, ensure_logging
from usaspending_api.config import CONFIG
from usaspending_api.settings import LOGGING

logger = logging.getLogger(__name__)


def copy_csv_from_s3_to_pg(
    s3_bucket_name: str,
    s3_obj_key: str,
    db_dsn: str,
    target_pg_table: str,
    gzipped: bool = True,
):
    """Stream a CSV file from S3 into a Postgres table using the SQL bulk COPY command

    WARNING: See note above in module docstring about this function being pickle-able, and maintaining a lean set of
    outward dependencies on other modules/code that require setup/config.
    """
    ensure_logging(logging_config_dict=LOGGING, formatter_class=AbbrevNamespaceUTCFormatter, logger_to_use=logger)
    start = time.time()
    logger.info(f"Starting write of: {s3_obj_key}")
    try:
        with psycopg2.connect(dsn=db_dsn) as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
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
                s3_obj = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_obj_key)
                # Getting Body gives a botocore.response.StreamingBody object back to allow "streaming" its contents
                s3_obj_body = s3_obj["Body"]
                with closing(s3_obj_body):  # make sure to close the stream when done
                    if gzipped:
                        with gzip.open(s3_obj_body, "rb") as csv_binary:
                            cursor.copy_expert(
                                sql=f"COPY {target_pg_table} FROM STDIN (FORMAT CSV)",
                                file=csv_binary,
                            )
                    else:
                        with codecs.getreader("utf-8")(s3_obj_body) as csv_stream_reader:
                            cursor.copy_expert(
                                sql=f"COPY {target_pg_table} FROM STDIN (FORMAT CSV)",
                                file=csv_stream_reader,
                            )
                elapsed = time.time() - start
                rows_copied = cursor.rowcount
                logger.info(f"Finished writing {s3_obj_key} with {rows_copied} row(s) in {elapsed:.3f}s")
                return rows_copied
    except Exception as exc:
        logger.error(f"ERROR writing {s3_obj_key}")
        logger.exception(exc)
        raise exc

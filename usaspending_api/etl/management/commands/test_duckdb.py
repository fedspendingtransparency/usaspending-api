import duckdb
import os

from django.core.management.base import BaseCommand

from usaspending_api.config import CONFIG
from usaspending_api import settings

SPARK_S3_BUCKET = CONFIG.SPARK_S3_BUCKET
DELTA_LAKE_S3_PATH = CONFIG.DELTA_LAKE_S3_PATH

S3_DELTA_PATH = f"s3://{SPARK_S3_BUCKET}/{DELTA_LAKE_S3_PATH}/rpt/award_search"

DELTA_EXTENSION_PATH = os.path.abspath("/duckdb_plugins/delta.duckdb_extension")
HTTPFS_EXTENSION_PATH = os.path.abspath("/duckdb_plugins/httpfs.duckdb_extension")

class Command(BaseCommand):

    def add_arguments(self, parser):

        parser.add_argument(
            "--is-local",
            action="store_true"
        )


    def handle(self, *args, **options):

        # Read arguments
        is_local = options["is_local"]

        # Establish DuckDB connection and install plugins
        conn = duckdb.connect()
        conn.query(f"LOAD '{DELTA_EXTENSION_PATH}'")
        conn.query(f"LOAD '{HTTPFS_EXTENSION_PATH}'")


        if is_local:
            conn.execute(f"SET s3_region='us-east-1'")
            conn.execute(f"SET s3_endpoint='http://minio:10001';")
            conn.execute(f"SET s3_use_ssl=false;")
            conn.execute(f"SET s3_access_key_id='usaspending';")
            conn.execute(f"SET s3_secret_access_key='usaspender';")
            conn.execute(f"SET s3_url_style='path';")

        query = f"SELECT * FROM delta_scan('{S3_DELTA_PATH}');"
        df = conn.execute(query).fetchdf()

        print(df)
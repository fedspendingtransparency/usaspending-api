import os

import duckdb
from django.core.management.base import BaseCommand

from usaspending_api.config import CONFIG

SPARK_S3_BUCKET = CONFIG.SPARK_S3_BUCKET
DELTA_LAKE_S3_PATH = CONFIG.DELTA_LAKE_S3_PATH

S3_DELTA_PATH = f"s3://{SPARK_S3_BUCKET}/{DELTA_LAKE_S3_PATH}/rpt/recipient_profile"

DUCKDB_EXTENSIONS = ["delta", "httpfs", "aws"]


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("--is-local", action="store_true")

    def handle(self, *args, **options):
        # Read arguments
        is_local = options["is_local"]

        # Establish DuckDB connection and install plugins
        conn = duckdb.connect()

        # Try to load the required extensions and install them only if they are missing
        for extension in DUCKDB_EXTENSIONS:
            try:
                print(f"Loading extension: {extension}")
                conn.load_extension(extension)
            except duckdb.IOException:
                try:
                    print(f"Failed to load extension '{extension}', so trying to install it.")
                    conn.install_extension(extension)
                    print(f"Successfully installed: {extension}. Trying to load it again.\n")
                    conn.load_extension(extension)
                except duckdb.IOException:
                    print(f"Failed to install extension: {extension}. Exiting.")
                    exit(1)
            print(f"Successfully loaded: {extension}\n")

        if is_local:
            minio_host = os.getenv("MINIO_HOST", "minio")
            minio_port = os.getenv("MINIO_PORT", "10001")
            minio_user = os.getenv("MINIO_ROOT_USER", "usaspending")
            minio_pass = os.getenv("MINIO_ROOT_PASSWORD", "usaspender")

            conn.execute(f"""
            CREATE SECRET delta_secret (
                TYPE s3,
                KEY_ID '{minio_user}',
                SECRET '{minio_pass}',
                REGION 'us-east-1',
                ENDPOINT '{minio_host}:{minio_port}',
                URL_STYLE 'path',
                USE_SSL 'false'
            );
            """)
        else:
            conn.execute("""
            CREATE SECRET delta_secret (
                TYPE s3,
                REGION 'us-gov-west-1',
                PROVIDER 'credential_chain',
                ENDPOINT 's3.us-gov-west-1.amazonaws.com'
            );
            """)

        # conn.execute("SELECT * FROM read_parquet('s3://dti-da-usaspending-spark-qat/data/delta/rpt/award_search/part-00000-b0a97813-ade0-4f32-9a13-89f23472850c.c000.snappy.parquet');")
        # print("Successfully read from Parquet file")

        print("Credentials:")
        print(conn.query("FROM duckdb_secrets();"))

        print("Reading CSV from S3 bucket")
        result = conn.read_csv("s3://dti-da-public-files-nonprod/broker_reference_data/agency_codes.csv").fetchall()
        print(f"Found {len(result)} rows in agency_codes.csv\n")

        columns = [
            duckdb.ColumnExpression("recipient_name"),
            duckdb.ColumnExpression("recipient_unique_id"),
        ]
        recipient_profile_query = conn.from_query(f"FROM delta_scan('{S3_DELTA_PATH}');").select(*columns).limit(5)
        print(f"Attempting to read from S3 Location: {S3_DELTA_PATH}")
        print(f"Generated query: {recipient_profile_query.sql_query()}")

        print("Results using .show():")
        recipient_profile_query.show()

        print("\nResults using print():")
        print(recipient_profile_query)

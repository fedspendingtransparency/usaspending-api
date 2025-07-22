import os

import duckdb
from django.core.management.base import BaseCommand

from usaspending_api.config import CONFIG

SPARK_S3_BUCKET = CONFIG.SPARK_S3_BUCKET
DELTA_LAKE_S3_PATH = CONFIG.DELTA_LAKE_S3_PATH

S3_DELTA_PATH = f"s3://{SPARK_S3_BUCKET}/{DELTA_LAKE_S3_PATH}/rpt/award_search"

DUCKDB_EXTENSIONS = ["delta", "httpfs", "aws", "postgres"]


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

        conn.execute(f"""
        CREATE SECRET postgres_secret (
            TYPE postgres,
            HOST {os.getenv("USASPENDING_DB_HOST", "localhost")},
            PORT 5432,
            DATABASE {os.getenv("USASPENDING_DB_NAME", "data_store_api")},
            USER {os.getenv("USASPENDING_DB_USER", "usaspending")},
            PASSWORD {os.getenv("USASPENDING_DB_PASSWORD", "usaspender")}
        );
        """)

        # query = conn.from_query(f"FROM delta_scan('{S3_DELTA_PATH}');").select("*").order("award_amount desc")
        # print(f"Attempting to read from S3 Location: {S3_DELTA_PATH}")
        # print("SQL results:")
        # print(query)

        print(f"DB_SOURCE: {os.getenv('DB_SOURCE')}")
        print(f"DB_R1: {os.getenv('DB_R1')}")
        print(f"DATABASE_URL: {os.getenv('DATABASE_URL')}")

        # print("Counting the occurrences of each DEFC in the FABA table")
        # conn.execute("ATTACH '' AS usas_pg (TYPE postgres, SECRET postgres_secret);")
        # conn.sql("""
        # SELECT
        #     COUNT(financial_accounts_by_awards_id) AS faba_count,
        #     disaster_emergency_fund_code AS defc
        # FROM usas_pg.public.financial_accounts_by_awards
        # GROUP BY defc
        # ORDER BY faba_count DESC;
        # """).show()

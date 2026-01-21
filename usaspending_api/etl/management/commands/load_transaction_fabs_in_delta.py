import logging

from django.core.management import BaseCommand
from usaspending_api.config import CONFIG
from usaspending_api.etl.transaction_delta_loaders.context_managers import prepare_spark
from usaspending_api.etl.transaction_delta_loaders.loaders import FABSDeltaTransactionLoader

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = """
        This command reads transaction data from source / bronze tables in delta and creates the delta silver tables.
    """

    spark_s3_bucket: str

    @staticmethod
    def add_arguments(parser):
        parser.add_argument(
            "--spark-s3-bucket",
            type=str,
            required=False,
            default=CONFIG.SPARK_S3_BUCKET,
            help="The destination bucket in S3 for creating the tables.",
        )

    @staticmethod
    def handle(*args, **options):
        with prepare_spark() as spark:
            loader = FABSDeltaTransactionLoader(spark=spark, spark_s3_bucket=options["spark_s3_bucket"])
            loader.load_transactions()

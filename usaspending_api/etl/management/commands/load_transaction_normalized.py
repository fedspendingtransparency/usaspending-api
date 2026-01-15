import logging

from django.core.management import BaseCommand, call_command

from usaspending_api.config import CONFIG
from usaspending_api.etl.transaction_delta_loaders.loaders import (
    FABSNormalizedDeltaTransactionLoader,
    FPDSNormalizedDeltaTransactionLoader,
)

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = """
        This command reads transaction data from source / bronze tables in delta and creates the delta silver tables.
    """

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
        fabs_loader = FABSNormalizedDeltaTransactionLoader(spark_s3_bucket=options["spark_s3_bucket"])
        fpds_loader = FPDSNormalizedDeltaTransactionLoader(spark_s3_bucket=options["spark_s3_bucket"])
        fabs_loader.load_transactions()
        fpds_loader.load_transactions()

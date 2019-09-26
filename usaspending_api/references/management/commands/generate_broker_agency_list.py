import logging
import os
import boto3
import pandas as pd

from django.core.management.base import BaseCommand
from django.conf import settings


class Command(BaseCommand):
    help = "Retrieves agency list file from broker's s3 bucket and writes csv to application"

    logger = logging.getLogger("console")

    def handle(self, *args, **options):
        """Connects to broker s3 bucket and writes agency list csv file in application"""

        self.logger.info("Connecting to S3 bucked to retrive broker agency list")

        s3connection = boto3.client("s3", region_name=settings.USASPENDING_AWS_REGION)
        agency_list_url = s3connection.generate_presigned_url(
            "get_object", Params={"Bucket": settings.BROKER_AGENCY_BUCKET_NAME, "Key": "agency_list.csv"}
        )
        broker_agency_list = pd.read_csv(agency_list_url, dtype=str)

        self.logger.info("Agency file sucessfully retrieved from S3")

        # Renaming subtier code column to match authoritative agency list
        broker_agency_list = broker_agency_list.rename(columns={"SUB TIER CODE": "SUBTIER CODE"})

        self.logger.info("Writing broker agency list to local data directory")

        broker_agency_list.to_csv(
            os.path.join(settings.BASE_DIR, "usaspending_api", "data", "agency_list_broker_s3.csv"),
            mode="w",
            index=False,
        )

        self.logger.info("Complete writing broker agency list")

import logging
import os
import boto
import pandas as pd

from django.core.management.base import BaseCommand
from django.conf import settings


class Command(BaseCommand):
    help = "Retrieves agency list file from broker's s3 bucket and writes csv to application"

    logger = logging.getLogger('console')

    def handle(self, *args, **options):
        """Connects to broker s3 bucket and writes agency list csv file in application"""

        self.logger.info('Connecting to S3 bucked to retrive broker agency list')

        s3connection = boto.s3.connect_to_region(settings.BULK_DOWNLOAD_AWS_REGION)
        s3bucket = s3connection.lookup(settings.BROKER_AGENCY_BUCKET_NAME)
        agency_list = s3bucket.get_key("agency_list.csv").generate_url(expires_in=600)
        broker_agency_list = pd.read_csv(agency_list, dtype=str)

        self.logger.info('Agency file sucessfully retrieved from S3')

        # Renaming subtier code column to match authoritative agency list
        broker_agency_list = broker_agency_list.rename(columns={'SUB TIER CODE': 'SUBTIER CODE'})

        self.logger.info('Writing broker agency list to local data directory')

        broker_agency_list.to_csv(os.path.join(settings.BASE_DIR, 'usaspending_api', 'data',
                                               'agency_list_broker_s3.csv'),
                                  mode='w', index=False)

        self.logger.info('Complete writing broker agency list')

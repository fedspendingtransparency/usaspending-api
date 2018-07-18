import logging
import boto

from datetime import datetime
from django.conf import settings


logger = logging.getLogger(__name__)


class S3Handler:
    """
    This class acts a wrapper for S3 URL Signing
    """

    def __init__(self, name, region=settings.USASPENDING_AWS_REGION):
        """
        Creates the object for signing URLS

        arguments:
        name -- (String) Name of the S3 bucket

        """
        self.bucketRoute = name
        self.region = region

    def get_simple_url(self, file_name):
        """
        Gets URL for read
        """

        s3connection = boto.s3.connect_to_region(self.region)
        generated = "https://{}/{}/{}".format(s3connection.server_name(), self.bucketRoute, file_name)
        return generated

    @staticmethod
    def get_timestamped_filename(filename):
        """
        Gets a Timestamped file name to prevent conflicts on S3 Uploading
        """
        sans_extension, extension = filename.split('.')
        timestamp = datetime.strftime(datetime.now(), '%Y%m%d%H%M%S')
        return '{}_{}.{}'.format(sans_extension, timestamp, extension)

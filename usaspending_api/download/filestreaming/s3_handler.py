import logging

from django.conf import settings


logger = logging.getLogger(__name__)


class S3Handler:
    """
    This class acts as a wrapper for S3 URL Signing
    """

    def __init__(self, bucket_name, redirect_dir, region=None, environment=None):
        """
        Creates the object for signing URLS

        arguments:
            bucket_name: (str) Name of the S3 bucket
            redirect_dir: (str) Name of the S3 folder (useful for production)

        """
        self.bucketRoute = bucket_name
        self.redirect_dir = redirect_dir
        self.region = region or settings.USASPENDING_AWS_REGION
        self.environment = environment or settings.DOWNLOAD_ENV

    def get_simple_url(self, file_name):
        """
        Gets URL for read
        """
        bucket_url = f"{settings.FILES_SERVER_BASE_URL}/{self.redirect_dir}/"
        generated = f"{bucket_url}{file_name}"
        return generated

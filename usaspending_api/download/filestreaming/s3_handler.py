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
        subdomain = "files"
        if self.environment != "production":
            subdomain = "files-nonprod"

        bucket_url = "https://{}.usaspending.gov/{}/".format(subdomain, self.redirect_dir)
        env_dir = ""
        if self.redirect_dir == settings.BULK_DOWNLOAD_S3_REDIRECT_DIR and self.environment != "production":
            # currently only downloads have a bucket per environment
            env_dir = "{}/".format(self.environment)
        generated = "{}{}{}".format(bucket_url, env_dir, file_name)
        return generated

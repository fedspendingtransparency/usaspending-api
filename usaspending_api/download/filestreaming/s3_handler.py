import logging

from datetime import datetime
from django.conf import settings


logger = logging.getLogger(__name__)


class S3Handler:
    """
    This class acts a wrapper for S3 URL Signing
    """

    def __init__(self, bucket_name, redirect_dir, region=settings.USASPENDING_AWS_REGION,
                 environment=settings.DOWNLOAD_ENV):
        """
        Creates the object for signing URLS

        arguments:
        name -- (String) Name of the S3 bucket

        """
        self.bucketRoute = bucket_name
        self.redirect_dir = redirect_dir
        self.region = region
        self.environment = environment

    def get_simple_url(self, file_name):
        """
        Gets URL for read
        """
        subdomain = 'files'
        if self.environment != 'production':
            subdomain = 'files-nonprod'

        bucket_url = "https://{}.usaspending.gov/{}/".format(subdomain, self.redirect_dir)
        env_dir = ''
        if self.redirect_dir == settings.BUCK_DOWNLOAD_S3_REDIRECT_DIR and self.environment != 'production':
            # currently only downloads have a bucket per environment
            env_dir = '{}/'.format(self.environment)
        generated = '{}{}{}'.format(bucket_url, env_dir, file_name)
        return generated

    @staticmethod
    def get_timestamped_filename(filename):
        """
        Gets a Timestamped file name to prevent conflicts on S3 Uploading
        """
        sans_extension, extension = filename.split('.')
        timestamp = datetime.strftime(datetime.now(), '%Y%m%d%H%M%S')
        return '{}_{}.{}'.format(sans_extension, timestamp, extension)

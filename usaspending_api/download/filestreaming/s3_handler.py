import logging
import time
from urllib.parse import urlparse, urlunparse
import boto
from boto import sts

from django.conf import settings


logger = logging.getLogger(__name__)


def strip_parameters(raw_url):
    'Returns URL without query parameters'

    return (urlparse(raw_url)[:3] + ('', )*3)


class S3Handler:
    """
    This class acts a wrapper for S3 URL Signing
    """
    BASE_URL = "https://s3.amazonaws.com/"
    ENABLE_S3 = True
    URL_LIFETIME = 2000
    STS_LIFETIME = 2000
    S3_ROLE = ""

    def __init__(self, name=None):
        """
        Creates the object for signing URLS

        arguments:
        name -- (String) Name of the S3 bucket

        """
        if name is None:
            self.bucketRoute = settings.CSV_S3_BUCKET_NAME
        else:
            self.bucketRoute = name

        S3Handler.S3_ROLE = settings.CSV_S3_ROLE
        S3Handler.REGION = settings.CSV_AWS_REGION

    def _sign_url(self, path, file_name, bucket_route, method="PUT"):
        """
        Creates the object for signing URLS

        arguments:
            path - (String) path to folder
            fileName - (String) File name of file to be uploaded to S3.
            method - method to create signed url for
        returns signed url (String)

        """
        if S3Handler.ENABLE_S3:
            s3connection = boto.s3.connect_to_region(S3Handler.REGION)
            if method == "PUT":
                generated = s3connection.generate_url(S3Handler.URL_LIFETIME, method,
                                                 bucket_route, "/" + path + "/" + file_name,
                                                 headers={'Content-Type': 'application/octet-stream'})
                return strip_parameters(generated)
            generated = s3connection.generate_url(S3Handler.URL_LIFETIME, method,
                                                  bucket_route, "/" + path + "/" + file_name)
            return strip_parameters(generated)
        return S3Handler.BASE_URL + "/" + self.bucketRoute + "/" + path + "/" + file_name

    def get_signed_url(self, path, file_name, bucket_route=None, method="PUT"):
        """
        Signs a URL for PUT requests

        arguments:
        file_name -- (String) File name of file to be uploaded to S3.

        returns signed url (String)
        """
        bucket_route = self.bucketRoute if bucket_route is None else bucket_route

        if method == "PUT":
            self.s3FileName = S3Handler.get_timestamped_filename(file_name)
        else:
            self.s3FileName = file_name
        return self._sign_url(path, self.s3FileName, bucket_route, method)

    @staticmethod
    def get_timestamped_filename(filename):
        """
        Gets a Timestamped file name to prevent conflicts on S3 Uploading
        """
        seconds = str(time.time() - 1500000000).replace('.', '_')
        return '{}_{}'.format(seconds, filename)

    @staticmethod
    def get_temporary_credentials(user):
        """
        Gets token that allows for S3 Uploads for seconds set in STS_LIFETIME
        """
        sts_connection = sts.connect_to_region(S3Handler.REGION)
        role = sts_connection.assume_role(S3Handler.S3_ROLE, "FileUpload" + str(user),
                                          duration_seconds=S3Handler.STS_LIFETIME)
        credentials = {
            'AccessKeyId': role.credentials.access_key,
            'SecretAccessKey': role.credentials.secret_key,
            'SessionToken': role.credentials.session_token,
            'Expiration': role.credentials.expiration
        }
        return credentials

    @staticmethod
    def get_file_size(filename):
        """ Returns file size in number of bytes for specified filename, or False if file doesn't exist """

        # Get key
        try:
            S3Handler.REGION
        except AttributeError:
            S3Handler.REGION = settings.CSV_AWS_REGION
        s3connection = boto.s3.connect_to_region(S3Handler.REGION)
        bucket = s3connection.get_bucket(settings.CSV_S3_BUCKET_NAME)
        key = bucket.get_key(filename)
        if key is None:
            logger.warning("File doesn't exist on AWS: %s", filename)
            return 0
        else:
            return key.size

    def get_file_urls(self, bucket_name, path):
        try:
            S3Handler.REGION
        except AttributeError:
            S3Handler.REGION = settings.CSV_AWS_REGION

        s3connection = boto.s3.connect_to_region(S3Handler.REGION)
        bucket = s3connection.get_bucket(bucket_name)

        urls = {}

        for key in bucket.list(prefix=path):
            if key.name != path:
                file_name = key.name[len(path):]
                url = self.get_signed_url(path=path, file_name=file_name, bucket_route=bucket_name, method="GET")
                urls[file_name] = url

        return urls

    @staticmethod
    def create_file_path(upload_name):
        try:
            S3Handler.REGION
        except AttributeError:
            S3Handler.REGION = settings.CSV_AWS_REGION

        bucket = settings.CSV_S3_BUCKET_NAME
        conn = boto.s3.connect_to_region(S3Handler.REGION).get_bucket(bucket).new_key(upload_name)
        return conn

    @staticmethod
    def copy_file(original_bucket, new_bucket, original_path, new_path):
        try:
            S3Handler.REGION
        except AttributeError:
            S3Handler.REGION = settings.CSV_AWS_REGION

        s3connection = boto.s3.connect_to_region(S3Handler.REGION)
        original_key = s3connection.get_bucket(original_bucket).get_key(original_path)
        original_key.copy(new_bucket, new_path)

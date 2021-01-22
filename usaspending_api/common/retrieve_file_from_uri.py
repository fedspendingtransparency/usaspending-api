import boto3
import requests
import tempfile
import urllib

from shutil import copyfile
from django.conf import settings


VALID_SCHEMES = ("http", "https", "s3", "file", "")
SCHEMA_HELP_TEXT = (
    "Internet RFC on Relative Uniform Resource Locators "
    + "Format: scheme://netloc/path;parameters?query#fragment "
    + "List of supported schemes: "
    + ", ".join(["{}://".format(s) for s in VALID_SCHEMES if s])
)


class RetrieveFileFromUri:
    def __init__(self, ruri):
        self.ruri = ruri  # Relative Uniform Resource Locator
        self.parsed_url_obj = urllib.parse.urlparse(ruri)
        if self.parsed_url_obj.scheme not in VALID_SCHEMES:
            msg = "Scheme '{}' isn't supported. Try one of these: {}"
            raise NotImplementedError(msg.format(self.parsed_url_obj.scheme, VALID_SCHEMES))

    def get_file_object(self, text=False):
        """
        return a file object (aka file handler) to either:
            the local file,
            a temporary file that was loaded from the pulled external file
        Recommendation is to use this method as a context manager
        """
        if self.parsed_url_obj.scheme == "s3":
            return self._handle_s3(text)
        elif self.parsed_url_obj.scheme.startswith("http"):
            return self._handle_http(text)
        elif self.parsed_url_obj.scheme in ("file", ""):
            return self._handle_file(text)
        else:
            raise NotImplementedError("No handler for scheme: {}!".format(self.parsed_url_obj.scheme))

    def copy(self, dest_file_path):
        """
        create a copy of the file and place at "dest_file_path" which
        currently must be a filesystem path (not s3 or http).
        """
        if self.parsed_url_obj.scheme == "s3":
            file_path = self.parsed_url_obj.path[1:]  # remove leading '/' character
            boto3_s3 = boto3.resource("s3", region_name=settings.USASPENDING_AWS_REGION)
            s3_bucket = boto3_s3.Bucket(self.parsed_url_obj.netloc)
            s3_bucket.download_file(file_path, dest_file_path)
        elif self.parsed_url_obj.scheme.startswith("http"):
            urllib.request.urlretrieve(self.ruri, dest_file_path)
        elif self.parsed_url_obj.scheme in ("file", ""):
            copyfile(self.ruri, dest_file_path)
        else:
            raise NotImplementedError("No handler for scheme: {}!".format(self.parsed_url_obj.scheme))

    def copy_to_temporary_file(self):
        """Sometimes it is super helpful to just have a nice, concrete, local file to work with."""
        with tempfile.NamedTemporaryFile() as tf:
            path = tf.name
        self.copy(path)
        return path

    def _handle_s3(self, text):
        file_path = self.parsed_url_obj.path[1:]  # remove leading '/' character
        boto3_s3 = boto3.resource("s3", region_name=settings.USASPENDING_AWS_REGION)
        s3_bucket = boto3_s3.Bucket(self.parsed_url_obj.netloc)

        f = tempfile.SpooledTemporaryFile()  # Must be in binary mode (default)
        s3_bucket.download_fileobj(file_path, f)

        if text:
            byte_str = f._file.getvalue()
            f = tempfile.SpooledTemporaryFile(mode="r")
            f.write(byte_str.decode())

        f.seek(0)  # go to beginning of file for reading
        return f

    def _handle_http(self, text):
        r = requests.get(self.ruri, allow_redirects=True)
        f = tempfile.SpooledTemporaryFile(mode="w" if text else "w+b")
        f.write(r.text if text else r.content)
        f.seek(0)  # go to beginning of file for reading
        return f

    def _handle_file(self, text):
        if self.parsed_url_obj == "file":
            file_path = self.parsed_url_obj.netloc
        else:  # if no schema provided, treat it as a relative file path
            file_path = self.parsed_url_obj.path

        return open(file_path, "r" if text else "rb")

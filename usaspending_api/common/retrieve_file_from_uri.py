import boto3
import requests
import tempfile
import urllib

from usaspending_api import settings

VALID_SCHEMES = ("http", "https", "s3", "file", "")
SCHEMA_HELP_TEXT = (
    "Internet RFC on Relative Uniform Resource Locators " +
    "Format: scheme://netloc/path;parameters?query#fragment " +
    "List of supported schemes: " +
    ", ".join(["{}://".format(s) for s in VALID_SCHEMES if s])
)


class RetrieveFileFromUri:
    def __init__(self, ruri, binary_data=True):
        self.uri = ruri  # Relative Uniform Resource Locator
        self.mode = "rb" if binary_data else "r"
        self._validate_url()

    def _validate_url(self):
        self.parsed_url_obj = urllib.parse.urlparse(self.uri)
        self._test_approved_scheme()

    def _test_approved_scheme(self):
        if self.parsed_url_obj.scheme not in VALID_SCHEMES:
            msg = "Scheme '{}' isn't supported. Try one of these: {}"
            raise NotImplementedError(msg.format(self.parsed_url_obj.scheme, VALID_SCHEMES))

    def get_file_object(self):
        """
            return a file object (aka file handler) to either:
                the local file,
                a temporary file that was loaded from the pulled exernal file
            Recommendation is to use this method as a context manager
        """
        if self.parsed_url_obj.scheme == "s3":
            return self._handle_s3()
        elif self.parsed_url_obj.scheme.startswith("http"):
            return self._handle_http()
        elif self.parsed_url_obj.scheme in ("file", ""):
            return self._handle_file()
        else:
            raise NotImplementedError("No handler for scheme: {}!".format(self.parsed_url_obj.scheme))

    def _handle_s3(self):
        file_path = self.parsed_url_obj.path[1:]  # remove leading '/' character
        boto3_s3 = boto3.resource("s3", region_name=settings.USASPENDING_AWS_REGION)
        s3_bucket = boto3_s3.Bucket(self.parsed_url_obj.netloc)

        f = tempfile.SpooledTemporaryFile()  # Must be in binary mode (default)
        s3_bucket.download_fileobj(file_path, f)
        f.seek(0)  # go to beginning of file for reading
        return f

    def _handle_http(self):
        r = requests.get(self.uri, allow_redirects=True)
        f = tempfile.SpooledTemporaryFile()
        f.write(r.content)
        f.seek(0)  # go to beginning of file for reading
        return f

    def _handle_file(self):
        if self.parsed_url_obj == "file":
            file_path = self.parsed_url_obj.netloc
        else:  # if no schema provided, treat it as a relative file path
            file_path = self.parsed_url_obj.path

        return open(file_path, self.mode)

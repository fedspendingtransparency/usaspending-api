import requests
import tempfile
import urllib


VALID_SCHEMES = ("http", "https", "s3", "file", "")
DISPLAY_ALL_SCHEMAS = ",".join(["{}://".format(s) for s in VALID_SCHEMES])


class FileFromUrl:
    def __init__(self, rfc_url, logger=None):
        self.url = rfc_url
        self.logger = logger
        self._validate_url()

    def _validate_url(self):
        self.parsed_url_obj = urllib.parse.urlparse(self.url)
        self._test_approved_scheme()

    def _test_approved_scheme(self):
        if self.parsed_url_obj.scheme not in VALID_SCHEMES:
            msg = "Scheme '{}' isn't an accepted type. Try one of these: {}"
            raise NotImplementedError(msg.format(self.parsed_url_obj.scheme, VALID_SCHEMES))

    def fetch_data_generator(self):
        if self.parsed_url_obj.scheme == "s3":
            return self._handle_s3()
        elif self.parsed_url_obj.scheme.startswith("http"):
            return self._handle_http()
        elif self.parsed_url_obj.scheme in ("file", ""):
            return self._handle_file()
        else:
            raise NotImplementedError("No handler for scheme!")

    def _handle_s3(self):
        raise NotImplementedError("No handler for s3 scheme (yet)")

    def _handle_http(self):
        r = requests.get(self.url, allow_redirects=True)
        with tempfile.SpooledTemporaryFile() as f:
            f.write(r.content)
            f.seek(0)
            for row in f:
                print(row)
                yield row

    def _handle_file(self):
        if self.parsed_url_obj == "file":
            file_path = self.parsed_url_obj.netloc
        else:  # no schema provided, treat it as a relative file path
            file_path = self.parsed_url_obj.path
        with open(file_path, "rb") as f:
            for row in f:
                yield row

import pytest

from usaspending_api.common.retrieve_file_from_uri import RetrieveFileFromUri


# Example.com exists for this very purpose.  I don't know about its uptime or
# anything.  If downtime proves to be an issue, we can switch to something else.
URL = "http://example.com/"


def test_retrieve_from_file():

    with RetrieveFileFromUri(__file__).get_file_object() as f:
        c = f.read()
        assert type(c) is bytes
        assert len(c) > 0

    with RetrieveFileFromUri(__file__).get_file_object(True) as f:
        c = f.read()
        assert type(c) is str
        assert len(c) > 0


def test_retrieve_from_http():

    with RetrieveFileFromUri(URL).get_file_object() as f:
        c = f.read()
        assert type(c) is bytes
        assert len(c) > 0

    with RetrieveFileFromUri(URL).get_file_object(True) as f:
        c = f.read()
        assert type(c) is str
        assert len(c) > 0


def test_retrieve_from_s3():
    # Can't currently think of a good way to test this.  We'd need a public,
    # always available S3 file.

    # Can test the not implemented bit, though.
    with pytest.raises(NotImplementedError):
        with RetrieveFileFromUri("s3://whatever/file.txt").get_file_object(True):
            pass


def test_file_copy(temp_file_path):

    RetrieveFileFromUri(__file__).copy(temp_file_path)

    with RetrieveFileFromUri(__file__).get_file_object(True) as f:
        c1 = f.read()

    with open(__file__) as f:
        c2 = f.read()

    assert c1 == c2


def test_http_copy(temp_file_path):

    RetrieveFileFromUri(URL).copy(temp_file_path)

    with RetrieveFileFromUri(__file__).get_file_object(True) as f:
        c = f.read()
        assert type(c) is str
        assert len(c) > 0


def test_s3_copy(temp_file_path):

    # Again, need an actual file to test against.

    with pytest.raises(NotImplementedError):
        with RetrieveFileFromUri("s3://whatever/file.txt").copy(temp_file_path):
            pass

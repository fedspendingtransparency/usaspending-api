from urllib.request import urlopen
from urllib.error import URLError
import socket


def is_aws():
    """Check if an instance is running on AWS, by way of getting a 200 response from the EC2 metadata service"""
    result = False
    meta = "http://169.254.169.254/latest/meta-data/"
    try:
        result = urlopen(meta, timeout=2).status == 200
    except (ConnectionError, TimeoutError, URLError, socket.timeout):
        return result
    return result


def get_aws_credentials(
    access_key: str = None,
    secret_key: str = None,
    profile: str = None,
) -> "botocore.credential.Credential":  # noqa, quoting to defer boto3+botocore import until totally needed
    """Use boto3.Session(...) to derive credentials from any of given values or other credential providers that boto3
    uses

    For example, if no args are provided, but the AWS_PROFILE environment variable is populated, it will use the
    credentials from that profile.
    """
    # Deferring import of external boto3 library until necessary.
    # Some configurations may run in a remote Spark cluster, and this reduces dependency on 3rd-party python
    # libraries, unless needed
    import boto3

    if profile == "":
        profile = None

    aws_creds = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        profile_name=profile,
    ).get_credentials()
    return aws_creds

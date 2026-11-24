import os
import socket
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from urllib.error import URLError
from urllib.request import urlopen

import aioboto3
from botocore.config import Config
from botocore.credentials import Credentials
from botocore.exceptions import ClientError
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_random_exponential

THROTTLING_ERROR_CODES = frozenset({"ThrottlingException", "TooManyRequestsException", "ServiceUnavailableException"})


def is_throttling_error(exc: BaseException) -> bool:
    """True if the exception is a botocore ClientError signaling AWS throttling."""
    return isinstance(exc, ClientError) and exc.response.get("Error", {}).get("Code") in THROTTLING_ERROR_CODES


# Retries throttled AWS calls (sync or async functions) with jittered exponential backoff, then re-raises the original
# error. Jitter matters here: parallel callers throttled at the same moment would otherwise retry in lockstep.
# Timeouts are deliberately not retried: callers may be waiting on a user-facing request, so fail fast instead.
retry_on_throttling = retry(
    retry=retry_if_exception(is_throttling_error),
    wait=wait_random_exponential(multiplier=1, max=20),
    stop=stop_after_attempt(5),
    reraise=True,
)

# Disables botocore's built-in retries (which would otherwise re-send a timed-out request up to 5 times, ~5 minutes at
# a 60s read timeout, and multiply with the tenacity retries above). Use with `retry_on_throttling` for throttling.
NO_BOTOCORE_RETRIES_CONFIG = Config(retries={"total_max_attempts": 1})


def is_aws() -> bool:
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
) -> Credentials:
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


@asynccontextmanager
async def async_aws_client(service_name: str, config: Config | None = None) -> AsyncGenerator:
    session = aioboto3.Session()
    async with session.client(
        service_name=service_name,
        region_name=os.environ.get("AWS_REGION", "us-gov-west-1"),
        config=config,
    ) as client:
        yield client

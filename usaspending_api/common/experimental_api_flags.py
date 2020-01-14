"""
Some API endpoints accept "X-Experimental-API" as a HTTP Header to control experimental API functionality.
The constants expected are defined in this file to insure consistency across different endpoints that may / will
need to have experimental functionality alongside the expected functionality as the changes are made permanent
in Production.

EXPERIMENTAL_API_HEADER - HTTP Header expected to access experimental functionality.

_ExperimentalHeaderValue - Enum representing the different possible Experimental headers
    ELASTICSEARCH - Access Elasticsearch functionality on specific endpoints.
    DISABLE_REQUEST_CACHE - Disable the API Request Cache where the decorator is used.
"""
from enum import Enum

from rest_framework.request import Request

EXPERIMENTAL_API_HEADER = "HTTP_X_EXPERIMENTAL_API"


class ExperimentalHeaderValue(Enum):
    ELASTICSEARCH = "elasticsearch"
    DISABLE_API_CACHE_CHECK = "disable-cache-check"


def _has_experimental_header(request: Request, expected_header: ExperimentalHeaderValue) -> bool:
    """
    When making an API Request an HTTP Header can be used multiple times with different values. This will cause
    the values to be passed over as 'HTTP_X_EXPERIMENTAL_API': 'value1,value2,value3'.

    Returns True if the 'expected_header' is one of the 'HTTP_X_EXPERIMENTAL_API' HTTP Headers.
    """
    experimental_headers = request.META.get(EXPERIMENTAL_API_HEADER, "").split(",")
    return expected_header.value in experimental_headers


def is_experimental_elasticsearch_api(request: Request) -> bool:
    """
    Returns True if the 'elasticsearch' header is part of 'HTTP_X_EXPERIMENTAL_API' HTTP Header; false otherwise
    """
    return _has_experimental_header(request, ExperimentalHeaderValue.ELASTICSEARCH)


def should_skip_api_cache_check(request: Request) -> bool:
    """
    Returns True if the 'disable-cache-check' header is part of 'HTTP_X_EXPERIMENTAL_API' HTTP Header; false otherwise
    """
    return _has_experimental_header(request, ExperimentalHeaderValue.DISABLE_API_CACHE_CHECK)

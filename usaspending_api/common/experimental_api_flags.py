"""
Some API endpoints accept "X-Experimental-API" as a HTTP Header to control experimental API functionality.
The constants expected are defined in this file to insure consistency across different endpoints that may / will
need to have experimental functionality alongside the expected functionality as the changes are made permanent
in Production.

EXPERIMENTAL_API_HEADER - HTTP Header expected to access experimental functionality.
ELASTICSEARCH_HEADER_VALUE - Value expected to access Elasticsearch functionality on specific endpoints.
"""
from rest_framework.request import Request

EXPERIMENTAL_API_HEADER = "HTTP_X_EXPERIMENTAL_API"
ELASTICSEARCH_HEADER_VALUE = "elasticsearch"


def is_experimental_elasticsearch_api(request: Request) -> bool:
    """
    Returns True or False depending on if the expected_header_value matches what is sent with the request.
    """
    return request.META.get(EXPERIMENTAL_API_HEADER) == ELASTICSEARCH_HEADER_VALUE

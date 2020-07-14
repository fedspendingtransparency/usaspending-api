from rest_framework.exceptions import APIException
from rest_framework import status


class NoDataFoundException(APIException):
    """Exception when no data were returned in query when there should always be data"""

    status_code = status.HTTP_204_NO_CONTENT
    default_detail = "Unmet data expectations: response contains no data"
    default_code = "no_data"


class InvalidParameterException(APIException):
    """Exception for invalid API request parameters."""

    status_code = status.HTTP_400_BAD_REQUEST
    default_detail = "Request contained an invalid parameter"
    default_code = "invalid_request"


class ForbiddenException(APIException):
    """Exception for when the request was valid, but the server is refusing to respond to it."""

    status_code = status.HTTP_403_FORBIDDEN
    default_detail = "Request was valid but server is refusing to respond due to constraints"
    default_code = "forbidden"


class EndpointRemovedException(APIException):
    """Exception for invalid API request parameters."""

    status_code = status.HTTP_410_GONE
    default_detail = (
        "Endpoint has been removed. Please refer to https://api.usaspending.gov/docs/endpoints for "
        "currently supported endpoints, or https://github.com/fedspendingtransparency/usaspending-api to "
        "report an issue."
    )
    default_code = "removed_endpoint"


class UnprocessableEntityException(APIException):
    """Exception for valid API request parameters but there are unmet constraints on the values.

    https://tools.ietf.org/html/rfc4918
    """

    status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
    default_detail = "Request parameter is valid but unable to process due to constraints"
    default_code = "invalid_request"


class ElasticsearchConnectionException(APIException):
    """Exception for invalid request parameters."""

    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    default_detail = "Unable to reach the Elasticsearch Cluster"
    default_code = "service_unavailable"


class NotImplementedException(APIException):
    """Exception to block an endpoint or specific logic path which will soon be available."""

    status_code = status.HTTP_501_NOT_IMPLEMENTED
    default_detail = "Functionality Not (yet) Implemented"
    default_code = "invalid_request"


class EndpointTimeoutException(APIException):
    """Exception for timeout for an API endpoint."""

    status_code = status.HTTP_504_GATEWAY_TIMEOUT
    default_detail = "Endpoint has timed out"
    default_code = "timeout"

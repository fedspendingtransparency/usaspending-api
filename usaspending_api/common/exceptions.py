from rest_framework.exceptions import APIException
from rest_framework import status


class NoDataFoundException(APIException):
    """Exception when no data were returned in query when there should always be data"""

    status_code = status.HTTP_204_NO_CONTENT
    default_detail = "Unmet data expectations: response contains no data"
    default_code = "no_data"


class InvalidParameterException(APIException):
    """Exception for invalid request parameters."""

    status_code = status.HTTP_400_BAD_REQUEST
    default_detail = "Request contained an invalid parameter"
    default_code = "invalid_request"


class UnprocessableEntityException(APIException):
    """https://tools.ietf.org/html/rfc4918"""

    status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
    default_detail = "Request parameter is valid but unable to process due to constraints"
    default_code = "invalid_request"


class ElasticsearchConnectionException(APIException):
    """Exception for invalid request parameters."""

    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    default_detail = "Unable to reach the Elasticsearch Cluster"
    default_code = "service_unavailable"


class NotImplementedException(APIException):
    status_code = status.HTTP_501_NOT_IMPLEMENTED
    default_detail = "Functionality Not (yet) Implemented"
    default_code = "invalid_request"


class EndpointTimeoutException(APIException):
    """Exception for timeout for an API endpoint."""

    status_code = status.HTTP_504_GATEWAY_TIMEOUT
    default_detail = "Endpoint has timed out"
    default_code = "timeout"

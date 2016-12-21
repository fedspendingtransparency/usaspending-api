from rest_framework.exceptions import APIException
from rest_framework import status


class InvalidParameterException(APIException):
    """Exception for invalid request parameters."""
    status_code = status.HTTP_400_BAD_REQUEST
    default_detail = 'Request contained an invalid parameter'
    default_code = 'invalid_request'
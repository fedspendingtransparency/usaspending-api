from opensearchpy import OpenSearchException
from requests.exceptions import ReadTimeout
from rest_framework.response import Response
from rest_framework.views import exception_handler
from urllib3.exceptions import ReadTimeoutError


def custom_exception_handler(exc: Exception, context: dict) -> Response:
    # Call REST framework's default exception handler first,
    # to get the standard error response.
    response = exception_handler(exc, context)
    if isinstance(exc, ReadTimeoutError):
        response = Response(status=504)
    if isinstance(exc, OpenSearchException):
        # Do ES error handling here
        if isinstance(exc, ReadTimeout):
            response = Response(status=504)
        else:
            response = Response(status=503)
    return response

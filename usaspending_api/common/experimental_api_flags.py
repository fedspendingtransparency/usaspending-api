"""
Some API endpoints accept "X-Experimental-API" as a HTTP Header to control experimental API functionality.
The constants expected are defined in this file to insure consistency across different endpoints that may / will
need to have experimental functionality alongside the expected functionality as the changes are made permanent
in Production.

EXPERIMENTAL_API_HEADER - HTTP Header expected to access experimental functionality.
ELASTICSEARCH_HEADER_VALUE - Value expected to access Elasticsearch functionality on specific endpoints.
"""

import json
import logging
import requests
from typing import Union
from rest_framework.request import Request
from django.http import HttpRequest
from django.http.request import HttpHeaders

logger = logging.getLogger(__name__)

EXPERIMENTAL_API_HEADER = "HTTP_X_EXPERIMENTAL_API"
ELASTICSEARCH_HEADER_VALUE = "elasticsearch"


def is_experimental_elasticsearch_api(request: Request) -> bool:
    """
    Returns True or False depending on if the expected_header_value matches what is sent with the request.
    """
    return request.META.get(EXPERIMENTAL_API_HEADER) == ELASTICSEARCH_HEADER_VALUE


def mirror_request_to_elasticsearch(request: Union[HttpRequest, Request]):
    """Duplicate request and send-again against this server, with the ES header attached to mirror
    non-elasticsearch load against elasticsearch for load testing
    """
    url = request.build_absolute_uri()
    data = json.dumps(request.data)
    headers = {
        **request.headers,
        HttpHeaders.parse_header_name(EXPERIMENTAL_API_HEADER): ELASTICSEARCH_HEADER_VALUE,
    }

    logger.warning("Mirroring inbound request with elasticsearch experimental header.")

    # NOTE: Purposely desiring an immediate timeout, with ignoring of that timeout error,
    # since this is a fire-and-forget way to siphon off duplicate load-testing traffic to the server,
    # without disrupting the primary request
    try:
        if request.method == "GET":
            requests.get(url, data, headers=headers, timeout=0.01)
        elif request.method == "POST":
            requests.post(url, data, headers=headers, timeout=0.01)
        else:
            pass
    # TODO: Preemptive timeout still seems to cause the request to be recorded as 500 with:
    # ConnectionResetError: [Errno 54] Connection reset by peer.
    # See if this can be avoided in a different way than forcing an early timeout
    except (requests.exceptions.Timeout, ConnectionResetError):
        pass
    except Exception as exc:
        logger.exception("Mirrored request using Elasticsearch failed", exc_info=exc)

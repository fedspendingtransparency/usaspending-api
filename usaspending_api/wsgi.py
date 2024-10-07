"""
WSGI config for usaspending_api project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/2.2/howto/deployment/wsgi/
"""

import os

from opentelemetry.instrumentation.wsgi import OpenTelemetryMiddleware
from django.core.wsgi import get_wsgi_application


def request_hook(span, environ):
    print("\n")
    print("Request hook executed\n")
    if span and span.is_recording():
        headers_to_capture = [
            "CONTENT_LENGTH",
            "CONTENT_TYPE",
            "HOST",
            "ORIGIN",
            "REFERER",
            "UA-IS-BOT",
            "USER_AGENT",
            "X_FORWARDED_FOR",
            "X_REQUESTED_WITH",
            "ALLOW",
            "CACHE_TRACE",
            "IS-DYNAMICALLY-RENDERED",
            "KEY",
            "STRICT-TRANSPORT-SECURITY",
        ]
        for header in headers_to_capture:
            header_value = environ.get(f'HTTP_{header.replace("-", "_").upper()}')
            if header_value:
                span.set_attribute(f"http.request.header.{header.lower().replace('_', '-')}", header_value)


def response_hook(span, environ, status, response_headers):
    print("Response hook executed\n")
    if span and span.is_recording():
        headers_to_capture = [
            "content-length",
            "content-type",
            "host",
            "origin",
            "referer",
            "ua-is-bot",
            "user-agent",
            "x-forwarded-for",
            "x-requested-with",
            "allow",
            "cache-trace",
            "is-dynamically-rendered",
            "key",
            "strict-transport-security",
        ]
        for header in headers_to_capture:
            for response_header in response_headers:
                if response_header[0].lower() == header:
                    span.set_attribute(f"http.response.header.{header}", response_header[1])


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "usaspending_api.settings")

application = get_wsgi_application()
application = OpenTelemetryMiddleware(application, request_hook=request_hook, response_hook=response_hook)

"""
WSGI config for usaspending_api project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/2.2/howto/deployment/wsgi/
"""

# Standard library imports
import os

# Django imports
from django.core.wsgi import get_wsgi_application

# OpenTelemetry imports
from opentelemetry import trace
from opentelemetry.instrumentation.django import DjangoInstrumentor
from opentelemetry.instrumentation.wsgi import OpenTelemetryMiddleware

# Local imports
from usaspending_api.common.logging import configure_logging
from usaspending_api.settings import IS_LOCAL, TRACE_ENV

# Constants
HEADERS_TO_CAPTURE = [
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


def request_hook(span, environ):

    if span and span.is_recording():
        for header in HEADERS_TO_CAPTURE:
            header_env_name = f"HTTP_{header.replace('-', '_').upper()}"
            header_value = environ.get(header_env_name)
            if header_value:
                span.set_attribute(f"http.request.header.{header}", header_value)

    if IS_LOCAL and os.getenv("TOGGLE_OTEL_CONSOLE_LOGGING") == "True":
        print("\nRequest hook executed\n")


def response_hook(span, environ, status, response_headers):

    if span and span.is_recording():
        for header in HEADERS_TO_CAPTURE:
            for response_header in response_headers:
                if response_header[0].lower() == header:
                    span.set_attribute(f"http.response.header.{header}", response_header[1])

    if IS_LOCAL and os.getenv("TOGGLE_OTEL_CONSOLE_LOGGING") == "True":
        print("\nResponse hook executed\n")


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "usaspending_api.settings")

############################################################
# ==== [Open Telemetry Configuration] ====
# Django Instrumentation
DjangoInstrumentor().instrument()

configure_logging(service_name="usaspending-api-" + TRACE_ENV)

# Optionally, set other OpenTelemetry configurations
service_name = os.getenv("OTEL_SERVICE_NAME", "usaspending-api")
os.environ["OTEL_RESOURCE_ATTRIBUTES"] = f"service.name={service_name}"

# Define additional settings for OpenTelemetry integration
TRACER = trace.get_tracer_provider().get_tracer(__name__)
OTEL_EXPORTER_OTLP_TRACES_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://localhost:4318/v1/traces")

############################################################

application = get_wsgi_application()
application = OpenTelemetryMiddleware(application, request_hook=request_hook, response_hook=response_hook)

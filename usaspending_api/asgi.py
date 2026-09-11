"""
WSGI config for usaspending_api project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/2.2/howto/deployment/wsgi/
"""

import logging
import os
from typing import Any

from asgiref.typing import Scope
from django.core.asgi import get_asgi_application
from opentelemetry import trace
from opentelemetry.instrumentation.asgi import OpenTelemetryMiddleware, asgi_getter
from opentelemetry.instrumentation.django import DjangoInstrumentor
from opentelemetry.trace import Span

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

logger = logging.getLogger(__name__)


def _add_headers_from_scope(span: Span, scope: Scope, attribute_prefix: str) -> None:
    if span and span.is_recording():
        for header in HEADERS_TO_CAPTURE:
            header_value = asgi_getter.get(scope, header)
            if header_value:
                span.set_attribute(f"{attribute_prefix}.{header}", header_value)


def client_request_hook(span: Span, scope: Scope, message: dict[str, Any]) -> None:
    _add_headers_from_scope(span, scope, "http.request.header")

    if IS_LOCAL and os.getenv("TOGGLE_OTEL_CONSOLE_LOGGING") == "True":
        logger.info("\nClient request hook executed\n")


def client_response_hook(span: Span, scope: Scope, message: dict[str, Any]):
    _add_headers_from_scope(span, scope, "http.response.header")

    if IS_LOCAL and os.getenv("TOGGLE_OTEL_CONSOLE_LOGGING") == "True":
        logger.info("\nClient response hook executed\n")


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

application = get_asgi_application()
application = OpenTelemetryMiddleware(
    application, client_request_hook=client_request_hook, client_response_hook=client_response_hook
)

"""
Module for Application Performance Monitoring and distributed tracing tools and helpers.

Specifically leveraging the Grafana Open Telemetry tracing client.
"""
from opentelemetry import trace
from opentelemetry.trace import SpanKind
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.trace.status import Status, StatusCode
from typing import Optional, Callable
import logging
import os

_logger = logging.getLogger(__name__)

# Set up the OpenTelemetry tracer provider
trace.set_tracer_provider(TracerProvider())

# Set up the OTLP exporter
otlp_exporter = OTLPSpanExporter(
    endpoint=os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "0.0.0.0:4317"),
)
span_processor = BatchSpanProcessor(otlp_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)

# Optionally, add a console exporter for debugging
console_exporter = ConsoleSpanExporter()
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(console_exporter))

tracer = trace.get_tracer_provider().get_tracer(__name__)


class OpenTelemetryEagerlyDropTraceFilter:
    """
    A trace filter that eagerly drops a trace by marking it as ERROR if a sentinel value is present.
    """

    EAGERLY_DROP_TRACE_KEY = "EAGERLY_DROP_TRACE"

    @classmethod
    def activate(cls):
        # No equivalent function to append filters in OpenTelemetry, add logic where necessary
        pass

    @classmethod
    def drop(cls, span: trace.Span):
        span.set_status(Status(StatusCode.ERROR))
        span.set_attribute(cls.EAGERLY_DROP_TRACE_KEY, True)


class SubprocessTrace:
    """
    A context manager class to handle entry and exit things that need to be done to get spans published
    that are part of an OpenTelemetry trace which continues into a subprocess.
    """

    def __init__(
        self,
        name: str,
        service: str = None,
        resource: str = None,
        span_type: SpanKind = SpanKind.INTERNAL,
        can_drop_sample: bool = True,
        **tags,
    ) -> None:
        self.name = name
        self.service = service
        self.resource = resource
        self.span_type = span_type
        self.can_drop_sample = can_drop_sample
        self.tags = tags
        self.span: Optional[trace.Span] = None

    def __enter__(self) -> trace.Span:
        self.span = tracer.start_span(name=self.name, kind=self.span_type)
        for key, value in self.tags.items():
            self.span.set_attribute(key, value)
        if not self.can_drop_sample:
            # OpenTelemetry does not have a direct equivalent for analytics sample rate
            pass
        return self.span

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.span is not None:
            if exc_type is not None:
                self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))
            self.span.end()


class OpenTelemetryLoggingTraceFilter:
    """Debugging utility filter that can log trace spans"""

    _log = logging.getLogger(f"{__name__}.OpenTelemetryLoggingTraceFilter")

    @classmethod
    def activate(cls):
        pass

    def process_trace(self, trace):
        logged = False
        trace_id = "???"
        for span in trace:
            trace_id = span.context.trace_id or "???"
            if not span.get_attribute(OpenTelemetryEagerlyDropTraceFilter.EAGERLY_DROP_TRACE_KEY):
                logged = True
                self._log.info(f"----[SPAN#{trace_id}]" + "-" * 40 + f"\n{span}")
        if logged:
            self._log.info(f"====[END TRACE#{trace_id}]" + "=" * 35)
        return trace

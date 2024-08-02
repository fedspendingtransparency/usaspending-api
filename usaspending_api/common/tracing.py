"""
Module for Application Performance Monitoring and distributed tracing tools and helpers.

Specifically leveraging the Grafana Open Telemetry tracing client.
"""
from opentelemetry import trace
from opentelemetry.trace import SpanKind
from opentelemetry.sdk.trace import TracerProvider, ReadableSpan, SpanProcessor

# from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
# from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.trace.status import Status, StatusCode
from typing import Optional, Callable
import logging

# import os


_logger = logging.getLogger(__name__)

# Set up the OpenTelemetry tracer provider
trace.set_tracer_provider(TracerProvider())
tracer = trace.get_tracer_provider().get_tracer(__name__)

# otlp exporter to send spans to url
# otlp_exporter = OTLPSpanExporter(
#     endpoint=os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "0.0.0.0:4317"),
# )
# span_processor = BatchSpanProcessor(otlp_exporter)
# trace.get_tracer_provider().add_span_processor(span_processor)

# Console exporter for debugging
# console_exporter = ConsoleSpanExporter()
# console_span_processor = BatchSpanProcessor(console_exporter)
# trace.get_tracer_provider().add_span_processor(console_span_processor)


class LoggingSpanProcessor(SpanProcessor):
    def on_end(self, span: ReadableSpan) -> None:
        trace_id = span.context.trace_id
        span_id = span.context.span_id
        logger = logging.getLogger(__name__)
        logger.info(f"Span ended: trace_id={trace_id}, span_id={span_id}, {span.name}_attributes={span.attributes}")

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True


logging_span_processor = LoggingSpanProcessor()
trace.get_tracer_provider().add_span_processor(logging_span_processor)


def _activate_trace_filter(filter_class: Callable) -> None:
    if not hasattr(tracer, "_filters"):
        _logger.warning("Datadog tracer client no longer has attribute '_filters' on which to append a span filter")
    else:
        if tracer._filters:
            tracer._filters.append(filter_class())
        else:
            tracer._filters = [filter_class()]


class OpenTelemetryEagerlyDropTraceFilter:
    """
    A trace filter that eagerly drops a trace, by filtering it out before sending it on to the Datadog Server API.
    It uses the `self.EAGERLY_DROP_TRACE_KEY` as a sentinel value. If present within any span's tags, the whole
    trace that the span is part of will be filtered out before being sent to the server.
    """

    EAGERLY_DROP_TRACE_KEY = "EAGERLY_DROP_TRACE"

    @classmethod
    def activate(cls):
        _activate_trace_filter(cls)

    @classmethod
    def drop(cls, span: trace.Span):
        span.set_status(Status(StatusCode.ERROR))
        span.set_attribute(cls.EAGERLY_DROP_TRACE_KEY, True)


# class DatadogEagerlyDropTraceFilter:
#     """
#     A trace filter that eagerly drops a trace, by filtering it out before sending it on to the Datadog Server API.
#     It uses the `self.EAGERLY_DROP_TRACE_KEY` as a sentinel value. If present within any span's tags, the whole
#     trace that the span is part of will be filtered out before being sent to the server.
#     """

#     EAGERLY_DROP_TRACE_KEY = "EAGERLY_DROP_TRACE"

#     @classmethod
#     def activate(cls):
#         _activate_trace_filter(cls)

#     @classmethod
#     def drop(cls, span: Span):
#         tracer.get_call_context().sampling_priority = USER_REJECT
#         span.set_tag(cls.EAGERLY_DROP_TRACE_KEY, True)

#     def process_trace(self, trace):
#         """Drop trace if any span tag has tag with key 'EAGERLY_DROP_TRACE'"""
#         return None if any(span.get_tag(self.EAGERLY_DROP_TRACE_KEY) for span in trace) else trace


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
        span_type: SpanKind = None,
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
        self.span = tracer.start_span(
            name=self.name, service=self.service, resource=self.resource, span_type=self.span_type
        )
        for key, value in self.tags.items():
            self.span.set_attribute(key, value)
        return self.span

    # ddtrace implementation
    # def __enter__(self) -> Span:
    #     self.span = tracer.trace(name=self.name, service=self.service, resource=self.resource, span_type=self.span_type)
    #     self.span.set_tags(self.tags)
    #     if not self.can_drop_sample:
    #         # Set True to add trace to App Analytics:
    #         # - https://docs.datadoghq.com/tracing/app_analytics/?tab=python#custom-instrumentation
    #         self.span.set_tag(ANALYTICS_SAMPLE_RATE_KEY, 1.0)
    #     return self.span

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.span is not None:
            if exc_type is not None:
                self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))
            self.span.end()

    # ddtrace implementation
    # def __exit__(self, exc_type, exc_val, exc_tb):
    #     """This context exit handler ensures that traces get sent to server regardless of how fast this subprocess runs.

    #     Workaround for: https://github.com/DataDog/dd-trace-py/issues/1184
    #     """
    #     try:
    #         # This will call span.finish() which must be done before the queue is flushed in order to enqueue the
    #         # span data that is to be flushed (sent to the server)
    #         self.span.__exit__(exc_type, exc_val, exc_tb)
    #     finally:
    #         writer_type = type(tracer.writer)
    #         if writer_type is AgentWriter:
    #             tracer.writer.flush_queue()
    #         else:
    #             _logger.warning(
    #                 f"Unexpected Datadog tracer.writer type of {writer_type} found. Not flushing trace spans."
    #             )


class OpenTelemetryLoggingTraceFilter:
    """Debugging utility filter that can log trace spans"""

    _log = logging.getLogger(f"{__name__}.OpenTelemetryLoggingTraceFilter")

    @classmethod
    def activate(cls):
        _activate_trace_filter(cls)

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


# ddtrace implementation
# class DatadogLoggingTraceFilter:
#     """Debugging utility filter that can log trace spans"""

#     _log = logging.getLogger(f"{__name__}.DatadogLoggingTraceFilter")

#     @classmethod
#     def activate(cls):
#         _activate_trace_filter(cls)

#     def process_trace(self, trace):
#         logged = False
#         trace_id = "???"
#         for span in trace:
#             trace_id = span.trace_id or "???"
#             if not span.get_tag(DatadogEagerlyDropTraceFilter.EAGERLY_DROP_TRACE_KEY):
#                 logged = True
#                 self._log.info(f"----[SPAN#{trace_id}]" + "-" * 40 + f"\n{span.pprint()}")
#         if logged:
#             self._log.info(f"====[END TRACE#{trace_id}]" + "=" * 35)
#         return trace

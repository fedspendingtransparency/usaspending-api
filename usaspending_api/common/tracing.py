"""
Module for Application Performance Monitoring and distributed tracing tools and helpers.

Specifically leveraging the Datadog tracing client.
"""
import logging

from ddtrace import tracer
from ddtrace.constants import ANALYTICS_SAMPLE_RATE_KEY
from ddtrace.ext import SpanTypes
from ddtrace.ext.priority import USER_REJECT
from ddtrace.internal.writer import AgentWriter
from ddtrace.span import Span
from typing import Optional

_logger = logging.getLogger(__name__)


class DatadogEagerlyDropTraceFilter:
    """
    A trace filter that eagerly drops a trace, by filtering it out before sending it on to the Datadog Server API.
    It uses the `self.EAGERLY_DROP_TRACE_KEY` as a sentinel value. If present within any span's tags, the whole
    trace that the span is part of will be filtered out before being sent to the server.
    """

    EAGERLY_DROP_TRACE_KEY = "EAGERLY_DROP_TRACE"

    @staticmethod
    def activate():
        if not hasattr(tracer, "_filters"):
            _logger.warning("Datadog tracer client no longer has attribute '_filters' on which to append a span filter")
        else:
            if tracer._filters:
                tracer._filters.append(DatadogEagerlyDropTraceFilter())
            else:
                tracer._filters = [DatadogEagerlyDropTraceFilter()]

    @classmethod
    def drop(cls, span: Span):
        tracer.get_call_context().sampling_priority = USER_REJECT
        span.set_tag(cls.EAGERLY_DROP_TRACE_KEY, True)

    def process_trace(self, trace):
        """Drop trace if any span tag has tag with key 'EAGERLY_DROP_TRACE'"""
        return None if any(span.get_tag(self.EAGERLY_DROP_TRACE_KEY) for span in trace) else trace


class SubprocessTrace:
    """A context manager class to handle of entry and exit things that need to be done to get spans published that are
    part of a Datadog trace which continues into a subprocess.

    This wraps some of the context management activity done in ``Span`` class, which is also a context manager.
    """

    def __init__(
        self,
        name: str,
        service: str = None,
        resource: str = None,
        span_type: SpanTypes = None,
        can_drop_sample: bool = True,
        **tags,
    ) -> None:
        self.name = name
        self.service = service
        self.resource = resource
        self.span_type = span_type
        self.can_drop_sample = can_drop_sample
        self.tags = tags
        self.span: Optional[Span] = None

    def __enter__(self) -> Span:
        self.span = tracer.trace(name=self.name, service=self.service, resource=self.resource, span_type=self.span_type)
        self.span.set_tags(self.tags)
        if not self.can_drop_sample:
            # Set True to add trace to App Analytics:
            # - https://docs.datadoghq.com/tracing/app_analytics/?tab=python#custom-instrumentation
            self.span.set_tag(ANALYTICS_SAMPLE_RATE_KEY, 1.0)
        return self.span

    def __exit__(self, exc_type, exc_val, exc_tb):
        """This context exit handler ensures that traces get sent to server regardless of how fast this subprocess runs.

        Workaround for: https://github.com/DataDog/dd-trace-py/issues/1184
        """
        try:
            # This will call span.finish() which must be done before the queue is flushed in order to enqueue the
            # span data that is to be flushed (sent to the server)
            self.span.__exit__(exc_type, exc_val, exc_tb)
        finally:
            writer_type = type(tracer.writer)
            if writer_type is AgentWriter:
                tracer.writer.flush_queue()
            else:
                _logger.warning(
                    f"Unexpected Datadog tracer.writer type of {writer_type} found. Not flushing trace spans."
                )


class DatadogLoggingTraceFilter:
    """Debugging utility filter that can log trace spans"""

    _logger = logging.getLogger(__name__)

    def process_trace(self, trace):
        logged = False
        trace_id = "???"
        for span in trace:
            trace_id = span.trace_id or "???"
            if not span.get_tag(DatadogEagerlyDropTraceFilter.EAGERLY_DROP_TRACE_KEY):
                logged = True
                self._logger.info(f"----[SPAN#{trace_id}]" + "-" * 40 + f"\n{span.pprint()}")
        if logged:
            self._logger.info(f"====[END TRACE#{trace_id}]" + "=" * 35)
        return trace

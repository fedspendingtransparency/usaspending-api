import inspect

import logging

import multiprocessing as mp
import pytest
import json

from _pytest.logging import LogCaptureFixture

from opentelemetry import trace
from opentelemetry.trace import SpanKind
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    SimpleSpanProcessor,
    ConsoleSpanExporter,
    SpanExportResult,
)

from usaspending_api.common.tracing import SubprocessTrace


class JsonLoggingSpanExporter(ConsoleSpanExporter):
    def formatter(self, span):
        span_data = {
            "name": span.name,
            "context": {
                "trace_id": hex(span.context.trace_id),
                "span_id": hex(span.context.span_id),
                "trace_state": str(span.context.trace_state),
            },
            "kind": str(span.kind),
            "start_time": span.start_time,
            "end_time": span.end_time,
            "status": str(span.status.status_code),
            "attributes": dict(span.attributes),
            "resource": dict(span.resource.attributes),
        }
        return json.dumps(span_data, indent=4)  # Pretty-print JSON with indentation

    def export(self, spans):
        for span in spans:
            logger.info(self.formatter(span))
        return SpanExportResult.SUCCESS


# Initialize the tracer provider and exporter for testing
provider = TracerProvider(resource=Resource.create({"service_name": "usaspending-api"}))
trace.set_tracer_provider(provider)

exporter = JsonLoggingSpanExporter()
span_processor = SimpleSpanProcessor(exporter)
trace.get_tracer_provider().add_span_processor(span_processor)
tracer = trace.get_tracer_provider().get_tracer(__name__)

# Instrument logging
LoggingInstrumentor().instrument(set_logging_format=True)
log_format = "%(message)s\n"
logging.basicConfig(format=log_format)
logger = logging.getLogger(__name__)
logger.propagate = True  # Ensure logs propagate


@pytest.fixture
def caplog(caplog):
    """A decorator (pattern) fixture around the pytest caplog fixture that adds the ability to temporarily alter
    loggers with propagate=False to True for duration of the test, so their output is propagated to the caplog log
    handler"""

    restore = []
    for logger in logging.Logger.manager.loggerDict.values():
        try:
            if not logger.propagate:
                logger.propagate = True
                restore += [logger]
        except AttributeError:
            pass
    yield caplog
    for logger in restore:
        logger.propagate = False


def test_logging_trace_spans_basic(caplog: LogCaptureFixture):
    caplog.set_level(logging.INFO, logger.name)

    test = f"{inspect.stack()[0][3]}"

    with tracer.start_as_current_span(name=f"{test}_operation", kind=SpanKind.INTERNAL) as span:
        span_attributes = {"service.name": f"{test}_service", "resource.name": f"{test}_resource", "span.type": "TEST"}
        for k, v in span_attributes.items():
            span.set_attribute(k, v)

        trace_id = span.get_span_context().trace_id
        span_id = span.get_span_context().span_id
        logger.info(f"Test log message with trace id: {trace_id}")
        log_span_id = f"The corresponding span id: {span_id}"
        logger.warning(log_span_id)

    assert f"trace id: {trace_id}" in caplog.text, "trace_id not found in logging output"
    assert f"span id: {span_id}" in caplog.text, "span_id not found in logging output"


def test_logging_trace_spans(caplog: LogCaptureFixture):
    caplog.set_level(logging.INFO)
    test = f"{inspect.stack()[0][3]}"

    with tracer.start_as_current_span(name=f"{test}_operation", kind=SpanKind.INTERNAL) as span:
        span_attributes = {"service.name": f"{test}_service", "resource.name": f"{test}_resource", "span.type": "TEST"}
        for k, v in span_attributes.items():
            span.set_attribute(k, v)

        trace_id = span.get_span_context().trace_id
        span_id = span.get_span_context().span_id
        logger.info(f"Test log message with trace id: {trace_id}")
        log_span_id = f"The corresponding span id: {span_id}"
        logger.warning(log_span_id)
        # do things
        x = 2**5
        thirty_two_squares = [m for m in map(lambda y: y**2, range(x))]
        assert thirty_two_squares[-1] == 961
    log_output = caplog.text
    assert f"trace id: {trace_id}" in log_output, "trace_id not found in logging output"
    assert f"span id: {span_id}" in log_output, "span_id not found in logging output"
    assert f"{test}_resource" in log_output, "traced resource not found in logging output"


def test_subprocess_basic(caplog: LogCaptureFixture):
    caplog.set_level(logging.INFO)
    test = f"{inspect.stack()[0][3]}"
    with SubprocessTrace(
        name=f"{test}_operation", kind=SpanKind.INTERNAL, service=f"{test}_service"
    ) as subprocess_trace:
        subprocess_trace.set_attributes(
            {"resource": f"{test}_resource", "span_type": "Internal", "message": "A test message"}
        )
        trace_id = subprocess_trace.get_span_context().trace_id
        span_id = subprocess_trace.get_span_context().span_id
        logger.info(f"Test log message with trace id: {trace_id}")
        log_span_id = f"The corresponding span id: {span_id}"
        logger.warning(log_span_id)

        # do things
        x = 2**5
        thirty_two_squares = [m for m in map(lambda y: y**2, range(x))]
        assert thirty_two_squares[-1] == 961

    log_output = caplog.text
    assert f"trace id: {trace_id}" in log_output, "trace_id not found in logging output"
    assert f"span id: {span_id}" in log_output, "span_id not found in logging output"
    assert f"{test}_resource" in log_output, "traced resource not found in logging output"


def test_subprocess_tracing(caplog: LogCaptureFixture, capsys):
    """Verify that spans created in subprocesses are written to the queue and then flushed to the server,
    when wrapped in the SubprocessTracer"""

    # Enable log output for this logger for duration of this test
    caplog.set_level(logging.INFO)
    test = f"{inspect.stack()[0][3]}"

    # And also send its output through a multiprocessing queue to surface logs from the subprocess
    log_queue = mp.Queue()

    with tracer.start_as_current_span(name=f"{test}_operation", kind=SpanKind.INTERNAL) as span:
        span_attributes = {"service.name": f"{test}_service", "resource.name": f"{test}_resource", "span.type": "TEST"}
        for k, v in span_attributes.items():
            span.set_attribute(k, v)

        trace_id = span.get_span_context().trace_id
        span_id = span.get_span_context().span_id
        test_msg = f"a test message was logged during {test}"
        logger.warning(test_msg)

        logger.info(f"The corresponding trace id: {trace_id}")
        logger.info(f"The corresponding span id: {span_id}")

        # Create and start a subprocess, passing the queue to capture logs
        ctx = mp.get_context("fork")
        worker = ctx.Process(
            name=f"{test}_subproc",
            target=_do_things_in_subproc,
            args=(caplog, log_queue, trace_id),  # Pass the main trace ID to the subprocess
        )
        worker.start()
        worker.join(timeout=100)

        if worker.is_alive():
            worker.terminate()
            try:
                log_records = []
                draining = True
                while draining:
                    while not log_queue.empty():
                        log_record = log_queue.get(block=True, timeout=5)
                        log_records.append(log_record)

                    if log_queue.empty():
                        draining = False

                for log_record in log_records:
                    caplog.handler.flush(log_record)
            except Exception:
                print("Error draining captured log queue when handling subproc TimeoutError")
                pass
            raise mp.TimeoutError(f"subprocess {worker.name} did not complete in timeout")

    subproc_trace_id, subproc_span_id = log_queue.get(block=True, timeout=10)
    logger.info(f"The corresponding trace id: {subproc_trace_id}")
    logger.info(f"The corresponding span id: {subproc_span_id}")

    assert test_msg in caplog.text, "caplog.text did not seem to capture logging output during test"

    # Parent Trace ID - this should be the same for the parent span and the child span
    assert subproc_trace_id == trace_id  # subprocess tracing should be a continuation of the trace in parent process
    assert f"trace id: {trace_id}" in caplog.text, "trace marker not found in logging output"

    # Parent Span ID
    assert f"span id: {span_id}" in caplog.text, "span marker not found in logging output"
    assert f"{test}_resource" in caplog.text, "traced resource not found in logging output"

    # Child Span ID aka subproc span
    assert f"span id: {subproc_span_id}" in caplog.text, "span marker not found in logging output"


def _do_things_in_subproc(caplog: LogCaptureFixture, log_queue: mp.Queue, parent_trace_id):
    # Ensure a StreamHandler is attached in the subprocess
    if not logger.handlers:
        stream_handler = logging.StreamHandler()
        logger.addHandler(stream_handler)
        logger.setLevel(logging.INFO)

    caplog.set_level(logging.INFO)
    test = f"{inspect.stack()[0][3]}"
    message = "This is a subprocess within a subprocess"
    with SubprocessTrace(
        name=f"{test}_operation",
        service=f"{test}_service",
        kind=SpanKind.INTERNAL,
        parent_trace_id=parent_trace_id,  # Propagate the parent trace ID to the subprocess
    ) as subprocess_2:
        subprocess_2.set_attributes(
            {
                "resource": f"{test}_subproc_resource",
                "span_type": "Internal",
                "subproc_test_msg": message,
            }
        )
        trace_id = subprocess_2.get_span_context().trace_id
        span_id = subprocess_2.get_span_context().span_id

        logger.info(message)
        logger.info(f"The corresponding trace id: {trace_id}")
        logger.info(f"The corresponding span id: {span_id}")

        # do things
        x = 2**5
        thirty_two_squares = [m for m in map(lambda y: y**2, range(x))]
        assert thirty_two_squares[-1] == 961
        logger.info("DONE doing things in subproc 2")
        # Return the trace and span ID to the main process

        # Ensure that spans are flushed and properly exported
        subprocess_2.end()
        log_queue.put((trace_id, span_id))

import inspect

import ddtrace
import logging
import multiprocessing as mp
import pytest

from logging.handlers import QueueHandler
from _pytest.logging import LogCaptureFixture
from ddtrace.ext import SpanTypes

from usaspending_api.common.tracing import DatadogEagerlyDropTraceFilter, DatadogLoggingTraceFilter, SubprocessTrace


@pytest.fixture
def datadog_tracer() -> ddtrace.Tracer:
    """Fixture to temporarily enable the Datadog Tracer during a test"""
    tracer_state = ddtrace.tracer.enabled
    ddtrace.tracer.enabled = True
    yield ddtrace.tracer
    ddtrace.tracer.enabled = tracer_state


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


def test_logging_trace_spans(datadog_tracer: ddtrace.Tracer, caplog: LogCaptureFixture):
    """Test the DatadogLoggingTraceFilter can actually capture trace span data in log output"""

    # Enable log output for this logger for duration of this test
    caplog.set_level(logging.DEBUG, DatadogLoggingTraceFilter._log.name)
    test = f"{inspect.stack()[0][3]}"
    DatadogLoggingTraceFilter.activate()
    with ddtrace.tracer.trace(
        name=f"{test}_operation",
        service=f"{test}_service",
        resource=f"{test}_resource",
        span_type=SpanTypes.TEST,
    ) as span:
        trace_id = span.trace_id
        logger = logging.getLogger(f"{test}_logger")
        test_msg = f"a test message was logged during {test}"
        logger.warning(test_msg)
        # do things
        x = 2 ** 5
        thirty_two_squares = [m for m in map(lambda y: y ** 2, range(x))]
        assert thirty_two_squares[-1] == 961
    assert test_msg in caplog.text, "caplog.text did not seem to capture logging output during test"
    assert f"SPAN#{trace_id}" in caplog.text, "span marker not found in logging output"
    assert f"TRACE#{trace_id}" in caplog.text, "trace marker not found in logging output"
    assert f"resource {test}_resource" in caplog.text, "traced resource not found in logging output"


def test_drop_key_on_trace_spans(datadog_tracer: ddtrace.Tracer, caplog: LogCaptureFixture):
    """Test that traces that have any span with the key that marks them for dropping, are not logged, but those that
    do not have this marker, are still logged"""

    # Enable log output for this logger for duration of this test
    caplog.set_level(logging.DEBUG, DatadogLoggingTraceFilter._log.name)
    test = f"{inspect.stack()[0][3]}"
    DatadogLoggingTraceFilter.activate()
    DatadogEagerlyDropTraceFilter.activate()
    with ddtrace.tracer.trace(
        name=f"{test}_operation",
        service=f"{test}_service",
        resource=f"{test}_resource",
        span_type=SpanTypes.TEST,
    ) as span:
        trace_id1 = span.trace_id
        logger = logging.getLogger(f"{test}_logger")
        test_msg = f"a test message was logged during {test}"
        logger.warning(test_msg)
        # do things
        x = 2 ** 5
        thirty_two_squares = [m for m in map(lambda y: y ** 2, range(x))]
        assert thirty_two_squares[-1] == 961

        # Drop this span so it is not sent to the server, and not logged by the trace logger
        DatadogEagerlyDropTraceFilter.drop(span)

    # Do another trace, that is NOT dropped
    with ddtrace.tracer.trace(
        name=f"{test}_operation2",
        service=f"{test}_service2",
        resource=f"{test}_resource2",
        span_type=SpanTypes.TEST,
    ) as span2:
        trace_id2 = span2.trace_id
        logger = logging.getLogger(f"{test}_logger")
        test_msg2 = f"a second test message was logged during {test}"
        logger.warning(test_msg2)
        # do things
        x = 2 ** 7

    assert test_msg in caplog.text, "caplog.text did not seem to capture logging output during test"
    assert f"SPAN#{trace_id1}" not in caplog.text, "span marker still logged when should have been dropped"
    assert f"TRACE#{trace_id1}" not in caplog.text, "trace marker still logged when should have been dropped"
    assert f"resource {test}_resource" in caplog.text, "traced resource still logged when should have been dropped"
    assert test_msg2 in caplog.text
    assert f"SPAN#{trace_id2}" in caplog.text, "span marker not found in logging output"
    assert f"TRACE#{trace_id2}" in caplog.text, "trace marker not found in logging output"
    assert f"resource {test}_resource2" in caplog.text, "traced resource not found in logging output"
    assert DatadogEagerlyDropTraceFilter.EAGERLY_DROP_TRACE_KEY not in caplog.text


def test_subprocess_trace(datadog_tracer: ddtrace.Tracer, caplog: LogCaptureFixture):
    """Verify that spans created in subprocesses are written to the queue and then flushed to the server,
    when wrapped in the SubprocessTracer"""

    # Enable log output for this logger for duration of this test
    caplog.set_level(logging.DEBUG, DatadogLoggingTraceFilter._log.name)
    test = f"{inspect.stack()[0][3]}"
    # And also send its output through a multiprocessing queue to surface logs from the subprocess
    log_queue = mp.Queue()
    DatadogLoggingTraceFilter._log.addHandler(QueueHandler(log_queue))
    DatadogLoggingTraceFilter.activate()

    subproc_test_msg = f"a test message was logged in a subprocess of {test}"
    state = mp.Queue()
    stop_sentinel = "-->STOP<--"

    with ddtrace.tracer.trace(
        name=f"{test}_operation",
        service=f"{test}_service",
        resource=f"{test}_resource",
        span_type=SpanTypes.TEST,
    ) as span:
        trace_id = span.trace_id
        logger = logging.getLogger(f"{test}_logger")
        test_msg = f"a test message was logged during {test}"
        logger.warning(test_msg)
        ctx = mp.get_context("fork")
        worker = ctx.Process(
            name=f"{test}_subproc",
            target=_do_things_in_subproc,
            args=(
                subproc_test_msg,
                state,
            ),
        )
        worker.start()
        worker.join(timeout=10)
        DatadogLoggingTraceFilter._log.warning(stop_sentinel)

    subproc_trace_id, subproc_span_id = state.get(block=True, timeout=10)
    assert test_msg in caplog.text, "caplog.text did not seem to capture logging output during test"
    assert f"SPAN#{trace_id}" in caplog.text, "span marker not found in logging output"
    assert f"TRACE#{trace_id}" in caplog.text, "trace marker not found in logging output"
    assert f"resource {test}_resource" in caplog.text, "traced resource not found in logging output"
    assert subproc_trace_id == trace_id  # subprocess tracing should be a continuation of the trace in parent process

    # Drain the queue and redirect DatadogLoggingTraceFilter log output to the caplog handler
    log_records = []
    draining = True
    while draining:
        while not log_queue.empty():
            log_record = log_queue.get(block=True, timeout=5)
            log_records.append(log_record)
        log_msgs = [r.getMessage() for r in log_records]
        if stop_sentinel in log_msgs:  # check for sentinel, signaling end of queued records
            draining = False
    for log_record in log_records:
        if log_record.getMessage() != stop_sentinel:
            caplog.handler.handle(log_record)

    assert f"{subproc_span_id}" in caplog.text, "subproc span id not found in logging output"
    assert (
        f"resource {_do_things_in_subproc.__name__}_resource" in caplog.text
    ), "subproc traced resource not found in logging output"


def _do_things_in_subproc(subproc_test_msg, q: mp.Queue):
    test = f"{inspect.stack()[0][3]}"
    with SubprocessTrace(
        name=f"{test}_operation",
        service=f"{test}_service",
        resource=f"{test}_resource",
        span_type=SpanTypes.TEST,
        subproc_test_msg=subproc_test_msg,
    ) as span:
        span_ids = (
            span.trace_id,
            span.span_id,
        )
        q.put(span_ids, block=True, timeout=5)
        logging.getLogger(f"{test}_logger").warning(subproc_test_msg)
        # do things
        x = 2 ** 5
        thirty_two_squares = [m for m in map(lambda y: y ** 2, range(x))]
        assert thirty_two_squares[-1] == 961
        logging.getLogger(f"{test}_logger").warning("DONE doing things in subproc")

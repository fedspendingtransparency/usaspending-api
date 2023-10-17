import inspect
from queue import Empty

import boto3
import logging
import multiprocessing as mp
import os
import signal
import psutil as ps

from random import randint

import pytest
from botocore.config import Config
from botocore.exceptions import EndpointConnectionError, ClientError, NoCredentialsError, NoRegionError
from psutil import TimeoutExpired

from usaspending_api.common.sqs.sqs_handler import (
    get_sqs_queue,
    FakeSQSMessage,
    UNITTEST_FAKE_QUEUE_NAME,
)
from usaspending_api.common.sqs.sqs_work_dispatcher import (
    SQSWorkDispatcher,
    QueueWorkerProcessError,
    QueueWorkDispatcherError,
)
from time import sleep

from usaspending_api.conftest_helpers import get_unittest_fake_sqs_queue


@pytest.fixture()
def _patch_get_sqs_queue(fake_sqs_queue, monkeypatch):
    """
    Patch the use of get_sqs_queue HERE, in tests in this module (defined by ``__name__``), where the function is
    imported as a new name in this module.

    Chaining to fixture ``fake_sqs_queue`` will also take care of fake-queue purging and cleanup.
    """
    monkeypatch.setattr(f"{__name__}.get_sqs_queue", get_unittest_fake_sqs_queue)
    should_be_fake = get_sqs_queue()
    # Check that the patch worked, and the fake unit test queue is returned
    assert should_be_fake.url.split("/")[-1] == UNITTEST_FAKE_QUEUE_NAME


@pytest.mark.signal_handling  # see mark doc in pyproject.toml
def test_fake_sqs_queue_fixture(fake_sqs_queue):
    """Ensure the fixture used in this and other tests yields a properly constructed instance of a fake queue for test

    Args:
        fake_sqs_queue: The fake queue yielded by the fixture this test depends on
    """
    should_be_fake = fake_sqs_queue
    # Check that the patch worked, and the fake unit test queue is returned
    assert should_be_fake.url.split("/")[-1] == UNITTEST_FAKE_QUEUE_NAME
    assert len(should_be_fake._FAKE_AWS_ACCT) == 32
    assert should_be_fake.url.split("/")[-2] == should_be_fake._FAKE_AWS_ACCT
    assert should_be_fake._QUEUE_DATA_FILE.split("/")[-1].split("_")[0] == should_be_fake._FAKE_AWS_ACCT


@pytest.mark.signal_handling  # see mark doc in pyproject.toml
def test_dispatch_with_default_numeric_message_body_succeeds(fake_sqs_queue):
    """SQSWorkDispatcher can execute work on a numeric message body successfully

    - Given a numeric message body
    - When on a SQSWorkDispatcher.dispatch() is called
    - Then the default message_transformer provides it as an argument to the job
    """
    queue = fake_sqs_queue
    queue.send_message(MessageBody=1234)

    dispatcher = SQSWorkDispatcher(
        queue, worker_process_name="Test Worker Process", long_poll_seconds=1, monitor_sleep_time=1
    )

    def do_some_work(task_id):
        assert task_id == 1234  # assert the message body is passed in as arg by default

        # The "work" we're doing is just putting something else on the queue
        queue_in_use = fake_sqs_queue
        queue_in_use.send_message(MessageBody=9999)

    dispatcher.dispatch(do_some_work)
    dispatcher._worker_process.join(5)  # wait at most 5 sec for the work to complete

    # Make sure the "work" was done
    messages = queue.receive_messages(WaitTimeSeconds=1, MaxNumberOfMessages=10)
    assert len(messages) == 1
    assert messages[0].body == 9999

    # Worker process should have a successful (0) exitcode
    assert dispatcher._worker_process.exitcode == 0


@pytest.mark.signal_handling  # see mark doc in pyproject.toml
def test_dispatch_with_single_dict_item_message_transformer_succeeds(fake_sqs_queue):
    """SQSWorkDispatcher can execute work on a numeric message body provided from the message
    transformer as a dict

    - Given a numeric message body
    - When on a SQSWorkDispatcher.dispatch() is called
    - And a message_transformer wraps that message body into a dict,
        keyed by the name of the single param of the job
    - Then the dispatcher provides the dict item's value to the job when invoked
    """
    queue = fake_sqs_queue
    queue.send_message(MessageBody=1234)

    dispatcher = SQSWorkDispatcher(
        queue, worker_process_name="Test Worker Process", long_poll_seconds=1, monitor_sleep_time=1
    )

    def do_some_work(task_id):
        assert task_id == 1234  # assert the message body is passed in as arg by default

        # The "work" we're doing is just putting something else on the queue
        queue_in_use = fake_sqs_queue
        queue_in_use.send_message(MessageBody=9999)

    dispatcher.dispatch(do_some_work, message_transformer=lambda x: {"task_id": x.body})
    dispatcher._worker_process.join(5)  # wait at most 5 sec for the work to complete

    # Make sure the "work" was done
    messages = queue.receive_messages(WaitTimeSeconds=1, MaxNumberOfMessages=10)
    assert len(messages) == 1
    assert messages[0].body == 9999

    # Worker process should have a successful (0) exitcode
    assert dispatcher._worker_process.exitcode == 0


@pytest.mark.signal_handling  # see mark doc in pyproject.toml
def test_dispatch_with_multi_arg_message_transformer_succeeds(fake_sqs_queue):
    """SQSWorkDispatcher can execute work when a message_transformer provides tuple-based args to use

    - Given a message_transformer that returns a tuple as the arguments
    - When on a SQSWorkDispatcher.dispatch() is called
    - Then the given job will run with those unnamed (positional) args
    """
    queue = fake_sqs_queue
    queue.send_message(MessageBody=1234)

    dispatcher = SQSWorkDispatcher(
        queue, worker_process_name="Test Worker Process", long_poll_seconds=1, monitor_sleep_time=1
    )

    def do_some_work(task_id, task_id_times_two):
        assert task_id == 1234
        assert task_id_times_two == 2468

        # The "work" we're doing is just putting something else on the queue
        queue_in_use = fake_sqs_queue
        queue_in_use.send_message(MessageBody=9999)

    dispatcher.dispatch(do_some_work, message_transformer=lambda x: (x.body, x.body * 2))
    dispatcher._worker_process.join(5)  # wait at most 5 sec for the work to complete

    # Make sure the "work" was done
    messages = queue.receive_messages(WaitTimeSeconds=1, MaxNumberOfMessages=10)
    assert len(messages) == 1
    assert messages[0].body == 9999

    # Worker process should have a successful (0) exitcode
    assert dispatcher._worker_process.exitcode == 0


@pytest.mark.signal_handling  # see mark doc in pyproject.toml
def test_dispatch_with_multi_kwarg_message_transformer_succeeds(fake_sqs_queue):
    """SQSWorkDispatcher can execute work when a message_transformer provides dict-based args to use

    - Given a message_transformer that returns a tuple as the arguments
    - When on a SQSWorkDispatcher.dispatch() is called
    - Then the given job will run with those unnamed (positional) args
    """
    queue = fake_sqs_queue
    queue.send_message(MessageBody=1234)

    dispatcher = SQSWorkDispatcher(
        queue, worker_process_name="Test Worker Process", long_poll_seconds=1, monitor_sleep_time=1
    )

    def do_some_work(task_id, task_id_times_two):
        assert task_id == 1234
        assert task_id_times_two == 2468

        # The "work" we're doing is just putting something else on the queue
        queue_in_use = fake_sqs_queue
        queue_in_use.send_message(MessageBody=9999)

    dispatcher.dispatch(
        do_some_work, message_transformer=lambda x: {"task_id": x.body, "task_id_times_two": x.body * 2}
    )
    dispatcher._worker_process.join(5)  # wait at most 5 sec for the work to complete

    # Make sure the "work" was done
    messages = queue.receive_messages(WaitTimeSeconds=1, MaxNumberOfMessages=10)
    assert len(messages) == 1
    assert messages[0].body == 9999

    # Worker process should have a successful (0) exitcode
    assert dispatcher._worker_process.exitcode == 0


@pytest.mark.signal_handling  # see mark doc in pyproject.toml
def test_additional_job_args_can_be_passed(fake_sqs_queue):
    """Additional args can be passed to the job to execute

    This should combine the singular element arg from the message transformer with the additional
    tuple-based args given as positional args
    """
    queue = fake_sqs_queue
    queue.send_message(MessageBody=1234)

    dispatcher = SQSWorkDispatcher(
        queue, worker_process_name="Test Worker Process", long_poll_seconds=1, monitor_sleep_time=1
    )

    def do_some_work(task_id, category):
        assert 1234 == task_id  # assert the message body is passed in as arg by default
        assert "easy work" == category

        # The "work" we're doing is just putting something else on the queue
        queue_in_use = fake_sqs_queue
        queue_in_use.send_message(MessageBody=9999)

    dispatcher.dispatch(do_some_work, "easy work")

    dispatcher._worker_process.join(5)  # wait at most 5 sec for the work to complete

    # Make sure the "work" was done
    messages = queue.receive_messages(WaitTimeSeconds=1, MaxNumberOfMessages=10)
    assert len(messages) == 1
    assert messages[0].body == 9999

    # Worker process should have a successful (0) exitcode
    assert dispatcher._worker_process.exitcode == 0


@pytest.mark.signal_handling  # see mark doc in pyproject.toml
def test_additional_job_kwargs_can_be_passed(fake_sqs_queue):
    """Additional args can be passed to the job to execute

    This should combine the singular element arg from the message transformer with the additional
    dictionary-based args given as keyword arguments
    """
    queue = fake_sqs_queue
    queue.send_message(MessageBody=1234)

    dispatcher = SQSWorkDispatcher(
        queue, worker_process_name="Test Worker Process", long_poll_seconds=1, monitor_sleep_time=1
    )

    def do_some_work(task_id, category):
        assert 1234 == task_id  # assert the message body is passed in as arg by default
        assert "easy work" == category

        # The "work" we're doing is just putting something else on the queue
        queue_in_use = fake_sqs_queue
        queue_in_use.send_message(MessageBody=9999)

    dispatcher.dispatch(do_some_work, category="easy work")

    dispatcher._worker_process.join(5)  # wait at most 5 sec for the work to complete

    # Make sure the "work" was done
    messages = queue.receive_messages(WaitTimeSeconds=1, MaxNumberOfMessages=10)
    assert len(messages) == 1
    assert messages[0].body == 9999

    # Worker process should have a successful (0) exitcode
    assert dispatcher._worker_process.exitcode == 0


@pytest.mark.signal_handling  # see mark doc in pyproject.toml
def test_additional_job_kwargs_can_be_passed_alongside_dict_args_from_message_transformer(fake_sqs_queue):
    """Additional args can be passed to the job to execute.

    This should combine the dictionary-based args from the message transformer with the additional
    dictionary-based args given as keyword arguments
    """
    queue = fake_sqs_queue
    queue.send_message(MessageBody=1234)

    dispatcher = SQSWorkDispatcher(
        queue, worker_process_name="Test Worker Process", long_poll_seconds=1, monitor_sleep_time=1
    )

    def do_some_work(task_id, category):
        assert 1234 == task_id  # assert the message body is passed in as arg by default
        assert "easy work" == category

        # The "work" we're doing is just putting something else on the queue
        queue_in_use = fake_sqs_queue
        queue_in_use.send_message(MessageBody=9999)

    dispatcher.dispatch(do_some_work, message_transformer=lambda x: {"task_id": x.body}, category="easy work")

    dispatcher._worker_process.join(5)  # wait at most 5 sec for the work to complete

    # Make sure the "work" was done
    messages = queue.receive_messages(WaitTimeSeconds=1, MaxNumberOfMessages=10)
    assert len(messages) == 1
    assert messages[0].body == 9999

    # Worker process should have a successful (0) exitcode
    assert dispatcher._worker_process.exitcode == 0


@pytest.mark.signal_handling  # see mark doc in pyproject.toml
def test_dispatching_by_message_attribute_succeeds_with_job_args(fake_sqs_queue):
    """SQSWorkDispatcher can read a message attribute to determine which function to call

    - Given a message with a user-defined message attribute
    - When on a SQSWorkDispatcher.dispatch_by_message_attribute() is called
    - And a message_transformer is given to route execution based on that message attribute
    - Then the correct function is executed
    """
    queue = fake_sqs_queue
    message_attr = {"work_type": {"DataType": "String", "StringValue": "a"}}
    queue.send_message(MessageBody=1234, MessageAttributes=message_attr)

    dispatcher = SQSWorkDispatcher(
        queue, worker_process_name="Test Worker Process", long_poll_seconds=1, monitor_sleep_time=1
    )

    def one_work(task_id):
        assert task_id == 1234  # assert the message body is passed in as arg by default

        # The "work" we're doing is just putting "a" on the queue
        queue_in_use = fake_sqs_queue
        queue_in_use.send_message(MessageBody=1)

    def two_work(task_id):
        assert task_id == 1234  # assert the message body is passed in as arg by default

        # The "work" we're doing is just putting "b" on the queue
        queue_in_use = fake_sqs_queue
        queue_in_use.send_message(MessageBody=2)

    def work_one_or_two(message):
        msg_attr = message.message_attributes
        if msg_attr and msg_attr.get("work_type", {}).get("StringValue") == "a":
            return {"_job": one_work, "_job_args": (message.body,)}
        else:
            return {"_job": two_work, "_job_args": (message.body,)}

    dispatcher.dispatch_by_message_attribute(work_one_or_two)
    dispatcher._worker_process.join(5)  # wait at most 5 sec for the work to complete

    # Make sure the "a_work" was done
    messages = queue.receive_messages(WaitTimeSeconds=1, MaxNumberOfMessages=10)
    assert len(messages) == 1
    assert messages[0].body == 1

    # Worker process should have a successful (0) exitcode
    assert dispatcher._worker_process.exitcode == 0


@pytest.mark.signal_handling  # see mark doc in pyproject.toml
def test_dispatching_by_message_attribute_succeeds_with_job_kwargs(fake_sqs_queue):
    """SQSWorkDispatcher can read a message attribute to determine which function to call

    - Given a message with a user-defined message attribute
    - When on a SQSWorkDispatcher.dispatch_by_message_attribute() is called
    - And a message_transformer is given to route execution based on that message attribute
    - Then the correct function is executed
    - And it can get its keyword arguments for execution from the job_kwargs item of the
        message_transformer's returned dictionary
    """
    queue = fake_sqs_queue
    message_attr = {"work_type": {"DataType": "String", "StringValue": "a"}}
    queue.send_message(MessageBody=1234, MessageAttributes=message_attr)

    dispatcher = SQSWorkDispatcher(
        queue, worker_process_name="Test Worker Process", long_poll_seconds=1, monitor_sleep_time=1
    )

    def one_work(task_id):
        assert task_id == 1234  # assert the message body is passed in as arg by default

        # The "work" we're doing is just putting "a" on the queue
        queue_in_use = fake_sqs_queue
        queue_in_use.send_message(MessageBody=1)

    def two_work(task_id):
        assert task_id == 1234  # assert the message body is passed in as arg by default

        # The "work" we're doing is just putting "b" on the queue
        queue_in_use = fake_sqs_queue
        queue_in_use.send_message(MessageBody=2)

    def work_one_or_two(message):
        msg_attr = message.message_attributes
        if msg_attr and msg_attr.get("work_type", {}).get("StringValue") == "a":
            return {"_job": one_work, "task_id": message.body}
        else:
            return {"_job": two_work, "task_id": message.body}

    dispatcher.dispatch_by_message_attribute(work_one_or_two)
    dispatcher._worker_process.join(5)  # wait at most 5 sec for the work to complete

    # Make sure the "a_work" was done
    messages = queue.receive_messages(WaitTimeSeconds=1, MaxNumberOfMessages=10)
    assert len(messages) == 1
    assert messages[0].body == 1

    # Worker process should have a successful (0) exitcode
    assert dispatcher._worker_process.exitcode == 0


@pytest.mark.signal_handling  # see mark doc in pyproject.toml
def test_dispatching_by_message_attribute_succeeds_with_job_args_and_job_kwargs(fake_sqs_queue):
    """SQSWorkDispatcher can read a message attribute to determine which function to call

    - Given a message with a user-defined message attribute
    - When on a SQSWorkDispatcher.dispatch_by_message_attribute() is called
    - And a message_transformer is given to route execution based on that message attribute
    - Then the correct function is executed
    - And it can get its args from the job_args item and keyword arguments for execution from the job_kwargs
        item of the message_transformer's returned dictionary
    """
    queue = fake_sqs_queue
    message_attr = {"work_type": {"DataType": "String", "StringValue": "a"}}
    queue.send_message(MessageBody=1234, MessageAttributes=message_attr)

    dispatcher = SQSWorkDispatcher(
        queue, worker_process_name="Test Worker Process", long_poll_seconds=1, monitor_sleep_time=1
    )

    def one_work(task_id, category):
        assert task_id == 1234  # assert the message body is passed in as arg by default
        assert category == "one work"

        # The "work" we're doing is just putting 1 on the queue
        queue_in_use = fake_sqs_queue
        queue_in_use.send_message(MessageBody=1)

    def two_work(task_id, category):
        assert task_id == 1234  # assert the message body is passed in as arg by default
        assert category == "two work"

        # The "work" we're doing is just putting 2 on the queue
        queue_in_use = fake_sqs_queue
        queue_in_use.send_message(MessageBody=2)

    def work_one_or_two(message):
        msg_attr = message.message_attributes
        if msg_attr and msg_attr.get("work_type", {}).get("StringValue") == "a":
            return {"_job": one_work, "_job_args": (message.body,), "category": "one work"}
        else:
            return {"_job": one_work, "_job_args": (message.body,), "category": "two work"}

    dispatcher.dispatch_by_message_attribute(work_one_or_two)
    dispatcher._worker_process.join(5)  # wait at most 5 sec for the work to complete

    # Make sure the "a_work" was done
    messages = queue.receive_messages(WaitTimeSeconds=1, MaxNumberOfMessages=10)
    assert len(messages) == 1
    assert messages[0].body == 1

    # Worker process should have a successful (0) exitcode
    assert dispatcher._worker_process.exitcode == 0


@pytest.mark.signal_handling  # see mark doc in pyproject.toml
def test_dispatching_by_message_attribute_succeeds_with_job_args_and_job_kwargs_and_additional(fake_sqs_queue):
    """SQSWorkDispatcher can read a message attribute to determine which function to call

    - Given a message with a user-defined message attribute
    - When on a SQSWorkDispatcher.dispatch_by_message_attribute() is called
    - And a message_transformer is given to route execution based on that message attribute
    - Then the correct function is executed
    - And it can get its args from the job_args item and keyword arguments for execution from the job_kwargs
        item of the message_transformer's returned dictionary
    - And can append to those additional args and additional kwargs
    """
    queue = fake_sqs_queue
    message_attr = {"work_type": {"DataType": "String", "StringValue": "a"}}
    queue.send_message(MessageBody=1234, MessageAttributes=message_attr)

    dispatcher = SQSWorkDispatcher(
        queue, worker_process_name="Test Worker Process", long_poll_seconds=1, monitor_sleep_time=1
    )

    def one_work(task_id, category, extra1, extra2, kwarg1, xkwarg1, xkwarg2=None, xkwarg3="override_me"):
        assert task_id == 1234  # assert the message body is passed in as arg by default
        assert category == "one work"
        assert kwarg1 == "my_kwarg_1"
        assert extra1 == "my_extra_arg_1"
        assert extra2 == "my_extra_arg_2"
        assert xkwarg1 == "my_extra_kwarg_1"
        assert xkwarg2 == "my_extra_kwarg_2"
        assert xkwarg3 == "my_extra_kwarg_3"

        # The "work" we're doing is just putting 1 on the queue
        queue_in_use = fake_sqs_queue
        queue_in_use.send_message(MessageBody=1)

    def two_work(task_id, category, extra1, extra2, kwarg1, xkwarg1, xkwarg2=None, xkwarg3="override_me"):
        assert task_id == 1234  # assert the message body is passed in as arg by default
        assert category == "two work"
        assert kwarg1 == "my_kwarg_1"
        assert extra1 == "my_extra_arg_1"
        assert extra2 == "my_extra_arg_2"
        assert xkwarg1 == "my_extra_kwarg_1"
        assert xkwarg2 == "my_extra_kwarg_2"
        assert xkwarg3 == "my_extra_kwarg_3"

        # The "work" we're doing is just putting 2 on the queue
        queue_in_use = fake_sqs_queue
        queue_in_use.send_message(MessageBody=2)

    def work_one_or_two(message):
        msg_attr = message.message_attributes
        if msg_attr and msg_attr.get("work_type", {}).get("StringValue") == "a":
            return {"_job": one_work, "_job_args": (message.body, "one work"), "kwarg1": "my_kwarg_1"}
        else:
            return {"_job": one_work, "_job_args": (message.body, "two work"), "kwarg1": "my_kwarg_1"}

    dispatcher.dispatch_by_message_attribute(
        work_one_or_two,
        "my_extra_arg_1",
        "my_extra_arg_2",
        xkwarg1="my_extra_kwarg_1",
        xkwarg2="my_extra_kwarg_2",
        xkwarg3="my_extra_kwarg_3",
    )
    dispatcher._worker_process.join(5)  # wait at most 5 sec for the work to complete

    # Make sure the "a_work" was done
    messages = queue.receive_messages(WaitTimeSeconds=1, MaxNumberOfMessages=10)
    assert len(messages) == 1
    assert messages[0].body == 1

    # Worker process should have a successful (0) exitcode
    assert dispatcher._worker_process.exitcode == 0


@pytest.mark.signal_handling  # see mark doc in pyproject.toml
def test_faulty_queue_connection_raises_correct_exception(fake_sqs_queue):
    """When a queue cannot be connected to, it raises the appropriate exception"""
    try:
        region_name = "us-gov-west-1"
        # note: connection max retries config not in botocore v1.5.x
        client_config = Config(region_name=region_name, connect_timeout=1, read_timeout=1)
        sqs = boto3.resource("sqs", config=client_config, endpoint_url=f"https://sqs.{region_name}.amazonaws.com")
        queue = sqs.Queue("75f4f422-3866-4e4f-9dc9-5364e3de3eaf")
        dispatcher = SQSWorkDispatcher(
            queue, worker_process_name="Test Worker Process", long_poll_seconds=1, monitor_sleep_time=1
        )
        dispatcher.dispatch(lambda x: x * 2)
    except (SystemExit, Exception) as e:
        assert isinstance(e, SystemExit)
        assert e.__cause__ is not None
        assert (
            isinstance(e.__cause__, EndpointConnectionError)
            or isinstance(e.__cause__, ClientError)
            or isinstance(e.__cause__, NoRegionError)
            or isinstance(e.__cause__, NoCredentialsError)
        )


@pytest.mark.signal_handling  # see mark doc in pyproject.toml
def test_failed_job_detected(fake_sqs_queue):
    """SQSWorkDispatcher handles failed work within the child process

    - Given a numeric message body
    - When on a SQSWorkDispatcher.dispatch() is called
    - And the function to execute in the child process fails
    - Then a QueueWorkerProcessError exception is raised
    - And the exit code of the worker process is > 0
    """
    with pytest.raises(QueueWorkerProcessError):
        queue = fake_sqs_queue
        queue.send_message(MessageBody=1234)

        dispatcher = SQSWorkDispatcher(
            queue, worker_process_name="Test Worker Process", long_poll_seconds=1, monitor_sleep_time=1
        )

        def fail_at_work(task_id):
            raise Exception("failing at this particular job...")

        dispatcher.dispatch(fail_at_work)
        dispatcher._worker_process.join(5)  # wait at most 5 sec for the work to complete

    # Worker process should have a failed (> 0)  exitcode
    assert dispatcher._worker_process.exitcode > 0


@pytest.mark.signal_handling  # see mark doc in pyproject.toml
def test_separate_signal_handlers_for_child_process(fake_sqs_queue):
    """Demonstrate (via log output) that a forked child process will inherit signal-handling of the parent
    process, but that can be overridden, while maintaining the original signal handling of the parent.

    NOTE: Test is not provable with asserts. Reading STDOUT proves it, but a place to store shared state among
    processes other than STDOUT was not found to be asserted on.
    """

    def fire_alarm():
        print("firing alarm from PID {}".format(os.getpid()))
        signal.setitimer(signal.ITIMER_REAL, 0.01)  # fire alarm in .01 sec
        sleep(0.015)  # Wait for timer to fire signal.SIGALRM

    fired = None

    def handle_sig(sig, frame):
        nonlocal fired
        fired = [os.getpid(), sig]
        print("handled signal from PID {} with fired = {}".format(os.getpid(), fired))

    signal.signal(signal.SIGALRM, handle_sig)
    fire_alarm()
    assert fired is not None
    assert fired[0] == os.getpid(), "PID of handled signal != this process's PID"
    assert fired[1] == signal.SIGALRM, "Signal handled was not signal.SIGALRM ({})".format(signal.SIGALRM)

    child_proc = mp.Process(target=fire_alarm, daemon=True)
    child_proc.start()
    child_proc.join(1)

    def signal_reset_wrapper(wrapped_func):
        print("resetting signals in PID {} before calling {}".format(os.getpid(), wrapped_func))
        signal.signal(signal.SIGALRM, signal.SIG_DFL)  # reset first
        wrapped_func()

    child_proc_with_cleared_signals = mp.Process(target=signal_reset_wrapper, args=(fire_alarm,), daemon=True)
    child_proc_with_cleared_signals.start()
    child_proc.join(1)

    fire_alarm()  # prove that clearing in one child process, left the handler intact in the parent process


@pytest.mark.signal_handling  # see mark doc in pyproject.toml
def test_terminated_job_triggers_exit_signal_handling_with_retry(fake_sqs_queue):
    """The child worker process is terminated, and exits indicating the exit signal of the termination. The
    parent monitors this, and initiates exit-handling. Because the dispatcher allows retries, this message
    should be made receivable again on the queue.
    """
    logger = logging.getLogger(__name__ + "." + inspect.stack()[0][3])
    logger.setLevel(logging.DEBUG)

    msg_body = randint(1111, 9998)
    queue = fake_sqs_queue
    queue.send_message(MessageBody=msg_body)

    dispatcher = SQSWorkDispatcher(
        queue, worker_process_name="Test Worker Process", long_poll_seconds=0, monitor_sleep_time=0.05
    )
    dispatcher.sqs_queue_instance.max_receive_count = 2  # allow retries

    wq = mp.Queue()  # work tracking queue
    tq = mp.Queue()  # termination queue

    terminator = mp.Process(name="worker_terminator", target=_worker_terminator, args=(tq, 0.05, logger), daemon=True)
    terminator.start()  # start terminator
    # Start dispatcher with work, and with the inter-process Queue so it can pass along its PID
    # Passing its PID on this Queue will let the terminator know the worker to terminate
    dispatcher.dispatch(_work_to_be_terminated, termination_queue=tq, work_tracking_queue=wq)
    dispatcher._worker_process.join(2)  # wait at most 2 sec for the work to complete
    terminator.join(1)  # ensure terminator completes within 3 seconds. Don't let it run away.

    try:
        # Worker process should have an exitcode less than zero
        assert dispatcher._worker_process.exitcode < 0
        assert -signal.SIGTERM == dispatcher._worker_process.exitcode
        # Message should NOT have been deleted from the queue, but available for receive again
        msgs = queue.receive_messages(WaitTimeSeconds=0)
        assert msgs is not None
        assert len(msgs) == 1, "Should be only 1 message received from queue"
        assert msgs[0].body == msg_body, "Should be the same message available for retry on the queue"
        # If the job function was called, it should have put the task_id (msg_body in this case) into the
        # work_tracking_queue as its first queued item
        work = wq.get(True, 1)
        assert work == msg_body, "Was expecting to find worker task_id (msg_body) tracked in the queue"
    finally:
        _fail_runaway_processes(logger, worker=dispatcher._worker_process, terminator=terminator)


@pytest.mark.signal_handling  # see mark doc in pyproject.toml
def test_terminated_job_triggers_exit_signal_handling_to_dlq(fake_sqs_queue):
    """The child worker process is terminated, and exits indicating the exit signal of the termination. The
    parent monitors this, and initiates exit-handling. Because the dispatcher does not allow retries, the
    message is copied to the dead letter queue, and deleted from the queue.
    """
    logger = logging.getLogger(__name__ + "." + inspect.stack()[0][3])
    logger.setLevel(logging.DEBUG)

    msg_body = randint(1111, 9998)
    queue = fake_sqs_queue
    queue.send_message(MessageBody=msg_body)

    dispatcher = SQSWorkDispatcher(
        queue, worker_process_name="Test Worker Process", long_poll_seconds=0, monitor_sleep_time=0.05
    )

    wq = mp.Queue()  # work tracking queue
    tq = mp.Queue()  # termination queue

    terminator = mp.Process(name="worker_terminator", target=_worker_terminator, args=(tq, 0.05, logger), daemon=True)
    terminator.start()  # start terminator

    with pytest.raises(QueueWorkerProcessError) as err_ctx:
        # Start dispatcher with work, and with the inter-process Queue so it can pass along its PID
        # Passing its PID on this Queue will let the terminator know the worker to terminate
        dispatcher.dispatch(_work_to_be_terminated, termination_queue=tq, work_tracking_queue=wq)

    # Ensure to wait on the processes to end with join
    dispatcher._worker_process.join(2)  # wait at most 2 sec for the work to complete
    terminator.join(1)  # ensure terminator completes within 3 seconds. Don't let it run away.

    assert err_ctx.match("SIGTERM"), "QueueWorkerProcessError did not mention 'SIGTERM'."

    try:
        # Worker process should have an exitcode less than zero
        assert dispatcher._worker_process.exitcode < 0
        assert -signal.SIGTERM == dispatcher._worker_process.exitcode
        # Message SHOULD have been deleted from the queue
        msgs = queue.receive_messages(WaitTimeSeconds=0)
        assert msgs is not None
        assert len(msgs) == 0, "Should be NO messages received from queue"
        # If the job function was called, it should have put the task_id (msg_body in this case) into the
        # work_tracking_queue as its first queued item
        work = wq.get(True, 1)
        assert work == msg_body, "Was expecting to find worker task_id (msg_body) tracked in the queue"
        # TODO: Test that the "dead letter queue" has this message - maybe by asserting the log statement
    finally:
        _fail_runaway_processes(logger, worker=dispatcher._worker_process, terminator=terminator)


@pytest.mark.signal_handling  # see mark doc in pyproject.toml
def test_terminated_parent_dispatcher_exits_with_negative_signal_code(fake_sqs_queue):
    """After a parent dispatcher process receives an exit signal, and kills its child worker process, it itself
    exits. Verify that the exit code it exits with is the negative value of the signal received, consistent with
    how Python handles this.
    """
    logger = logging.getLogger(__name__ + "." + inspect.stack()[0][3])
    logger.setLevel(logging.DEBUG)

    msg_body = randint(1111, 9998)
    queue = fake_sqs_queue
    queue.send_message(MessageBody=msg_body)

    dispatcher = SQSWorkDispatcher(
        queue, worker_process_name="Test Worker Process", long_poll_seconds=0, monitor_sleep_time=0.05
    )
    dispatcher.sqs_queue_instance.max_receive_count = 2  # allow retries

    wq = mp.Queue()  # work tracking queue
    tq = mp.Queue()  # termination queue

    dispatch_kwargs = {"job": _work_to_be_terminated, "termination_queue": tq, "work_tracking_queue": wq}
    parent_dispatcher = mp.Process(target=dispatcher.dispatch, kwargs=dispatch_kwargs)
    parent_dispatcher.start()

    # block until worker is running and ready to be terminated, or fail in 3 seconds
    work_done = wq.get(True, 3)
    worker_pid = tq.get(True, 1)
    worker_proc = ps.Process(worker_pid)

    parent_pid = parent_dispatcher.pid
    parent_proc = ps.Process(parent_pid)

    assert work_done == msg_body
    parent_proc.terminate()  # kills with SIGTERM signal

    # Wait at most this many seconds for the worker and dispatcher to finish, let TimeoutExpired raise and fail.
    try:
        # Check the same process that was started is still running. Must also check equality in case pid recycled
        while worker_proc.is_running() and ps.pid_exists(worker_pid) and worker_proc == ps.Process(worker_pid):
            wait_while = 5
            logger.debug(
                f"Waiting {wait_while}s for worker to complete after parent received kill signal. "
                f"worker pid = {worker_pid}, worker status = {worker_proc.status()}"
            )
            worker_proc.wait(timeout=wait_while)
    except TimeoutExpired as tex:
        pytest.fail(f"TimeoutExpired waiting for worker with pid {worker_proc.pid} to terminate (complete work).", tex)
    try:
        # Check the same process that was started is still running. Must also check equality in case pid recycled
        while (
            parent_dispatcher.is_alive()
            and parent_proc.is_running()
            and ps.pid_exists(parent_pid)
            and parent_proc == ps.Process(parent_pid)
        ):
            wait_while = 3
            logger.debug(
                f"Waiting {wait_while}s for parent dispatcher to complete after kill signal. "
                f"parent_dispatcher pid = {parent_pid}, parent_dispatcher status = {parent_proc.status()}"
            )
            parent_dispatcher.join(timeout=wait_while)
    except TimeoutExpired as tex:
        pytest.fail(
            f"TimeoutExpired waiting for parent dispatcher with pid {parent_pid} to terminate (complete work).", tex
        )

    try:
        # Parent dispatcher process should have an exit code less than zero
        assert not parent_dispatcher.is_alive(), "Parent dispatcher was expected to be killed, but still alive"
        assert parent_dispatcher.exitcode is not None and parent_dispatcher.exitcode < 0
        assert -signal.SIGTERM == parent_dispatcher.exitcode
        # Message should NOT have been deleted from the queue, but available for receive again
        msgs = queue.receive_messages(WaitTimeSeconds=0)
        assert msgs is not None
        assert len(msgs) == 1, "Should be only 1 message received from queue"
        assert msgs[0].body == msg_body, "Should be the same message available for retry on the queue"
        # If the job function was called, it should have put the task_id (msg_body in this case) into the
        # work_tracking_queue as its first queued item
        assert work_done == msg_body, "Was expecting to find worker task_id (msg_body) tracked in the queue"
    finally:
        if worker_proc and worker_proc.is_running():
            logger.warning(
                "Dispatched worker process with PID {} did not complete in timeout. "
                "Killing it.".format(worker_proc.pid)
            )
            os.kill(worker_proc.pid, signal.SIGKILL)
            pytest.fail("Worker did not complete in timeout as expected. Test fails.")
        _fail_runaway_processes(logger, dispatcher=parent_dispatcher)


@pytest.mark.signal_handling  # see mark doc in pyproject.toml
def test_exit_handler_can_receive_queue_message_as_arg(fake_sqs_queue):
    """Verify that exit_handlers provided whose signatures allow keyword args can receive the queue message
    as a keyword arg
    """
    logger = logging.getLogger(__name__ + "." + inspect.stack()[0][3])
    logger.setLevel(logging.DEBUG)

    msg_body = randint(1111, 9998)
    queue = fake_sqs_queue
    queue.send_message(MessageBody=msg_body)

    dispatcher = SQSWorkDispatcher(
        queue, worker_process_name="Test Worker Process", long_poll_seconds=0, monitor_sleep_time=0.05
    )
    dispatcher.sqs_queue_instance.max_receive_count = 2  # allow retries

    def exit_handler_with_msg(
        task_id, termination_queue: mp.Queue, work_tracking_queue: mp.Queue, queue_message=None  # noqa
    ):  # noqa
        logger.debug(
            "CLEANUP: performing cleanup with "
            "exit_handler_with_msg({}, {}, {}".format(task_id, termination_queue, queue_message)
        )
        logger.debug("msg.body = {}".format(queue_message.body))
        work_tracking_queue.put("exit_handling:{}".format(queue_message.body))  # track what we handled

    termination_queue = mp.Queue()
    work_tracking_queue = mp.Queue()
    terminator = mp.Process(
        name="worker_terminator",
        target=_worker_terminator,
        args=(termination_queue, 0.05, logger),
        daemon=True,
    )
    terminator.start()  # start terminator
    # Start dispatcher with work, and with the inter-process Queue so it can pass along its PID
    # Passing its PID on this Queue will let the terminator know the worker to terminate
    dispatcher.dispatch(
        _work_to_be_terminated,
        termination_queue=termination_queue,
        work_tracking_queue=work_tracking_queue,
        exit_handler=exit_handler_with_msg,
    )
    dispatcher._worker_process.join(2)  # wait at most 2 sec for the work to complete
    terminator.join(1)  # ensure terminator completes within 3 seconds. Don't let it run away.

    try:
        # Worker process should have an exitcode less than zero
        assert dispatcher._worker_process.exitcode < 0
        assert -signal.SIGTERM == dispatcher._worker_process.exitcode
        # Message SHOULD have been deleted from the queue
        msgs = queue.receive_messages(WaitTimeSeconds=0)

        # Message should NOT have been deleted from the queue, but available for receive again
        msgs = queue.receive_messages(WaitTimeSeconds=0)
        assert msgs is not None
        assert len(msgs) == 1, "Should be only 1 message received from queue"
        assert msgs[0].body == msg_body, "Should be the same message available for retry on the queue"
        # If the job function was called, it should have put the task_id (msg_body in this case) into the
        # work_tracking_queue as its first queued item. FIFO queue, so it should be "first out"
        work = work_tracking_queue.get(True, 1)
        assert work == msg_body, "Was expecting to find worker task_id (msg_body) tracked in the queue"
        # If the exit_handler was called, and if it passed along args from the original worker, then it should
        # have put the message body into the "work_tracker_queue" after work was started and exit occurred
        exit_handling_work = work_tracking_queue.get(True, 1)
        assert "exit_handling:{}".format(msg_body) == exit_handling_work, (
            "Was expecting to find tracking in the work_tracking_queue of the message being handled "
            "by the exit_handler"
        )
    finally:
        _fail_runaway_processes(logger, worker=dispatcher._worker_process, terminator=terminator)


@pytest.mark.signal_handling  # see mark doc in pyproject.toml
def test_default_to_queue_long_poll_works(fake_sqs_queue):
    """Same as testing exit handling when allowing retries, but not setting the long_poll_seconds value,
    to leave it to the default setting.
    """
    logger = logging.getLogger(__name__ + "." + inspect.stack()[0][3])
    logger.setLevel(logging.DEBUG)

    msg_body = randint(1111, 9998)
    queue = fake_sqs_queue
    queue.send_message(MessageBody=msg_body)

    dispatcher = SQSWorkDispatcher(queue, worker_process_name="Test Worker Process", monitor_sleep_time=0.05)
    dispatcher.sqs_queue_instance.max_receive_count = 2  # allow retries

    wq = mp.Queue()  # work tracking queue
    tq = mp.Queue()  # termination queue

    terminator = mp.Process(name="worker_terminator", target=_worker_terminator, args=(tq, 0.05, logger), daemon=True)
    terminator.start()  # start terminator
    # Start dispatcher with work, and with the inter-process Queue so it can pass along its PID
    # Passing its PID on this Queue will let the terminator know the worker to terminate
    dispatcher.dispatch(_work_to_be_terminated, termination_queue=tq, work_tracking_queue=wq)
    dispatcher._worker_process.join(2)  # wait at most 2 sec for the work to complete
    terminator.join(1)  # ensure terminator completes within 3 seconds. Don't let it run away.

    try:
        # Worker process should have an exitcode less than zero
        assert dispatcher._worker_process.exitcode < 0
        assert -signal.SIGTERM == dispatcher._worker_process.exitcode
        # Message should NOT have been deleted from the queue, but available for receive again
        msgs = queue.receive_messages(WaitTimeSeconds=0)
        assert msgs is not None
        assert len(msgs) == 1, "Should be only 1 message received from queue"
        assert msgs[0].body == msg_body, "Should be the same message available for retry on the queue"
        # If the job function was called, it should have put the task_id (msg_body in this case) into the
        # work_tracking_queue as its first queued item
        work = wq.get(True, 1)
        assert work == msg_body, "Was expecting to find worker task_id (msg_body) tracked in the queue"
    finally:
        _fail_runaway_processes(logger, worker=dispatcher._worker_process, terminator=terminator)


@pytest.mark.signal_handling  # see mark doc in pyproject.toml
def test_hanging_cleanup_of_signaled_child_fails_dispatcher_and_sends_to_dlq(fake_sqs_queue):
    """When detecting the child worker process received an exit signal, and the parent dispatcher
    process is handling cleanup and termination of the child worker process, if the cleanup hangs for longer
    than the allowed exit_handling_timeout, it will short-circuit, and retry a 2nd time. If that also hangs
    longer than exit_handling_timeout, the message will be put on the dead letter queue, deleted from the
    origin queue, and :exc:`QueueWorkDispatcherError` will be raised stating unable to do exit handling
    """
    logger = logging.getLogger(__name__ + "." + inspect.stack()[0][3])
    logger.setLevel(logging.DEBUG)

    msg_body = randint(1111, 9998)
    queue = fake_sqs_queue
    queue.send_message(MessageBody=msg_body)
    worker_sleep_interval = 0.05  # how long to "work"

    def hanging_cleanup(task_id, termination_queue: mp.Queue, work_tracking_queue: mp.Queue, queue_message):
        work_tracking_queue.put("cleanup_start_{}".format(queue_message.body), block=True, timeout=0.5)
        sleep(2.5)  # sleep for longer than the allowed time for exit handling
        # Should never get to this point, since it should be short-circuited by exit_handling_timeout
        work_tracking_queue.put("cleanup_end_{}".format(queue_message.body), block=True, timeout=0.5)

    cleanup_timeout = int(worker_sleep_interval + 1)
    dispatcher = SQSWorkDispatcher(
        queue,
        worker_process_name="Test Worker Process",
        long_poll_seconds=0,
        monitor_sleep_time=0.05,
        exit_handling_timeout=cleanup_timeout,
    )

    wq = mp.Queue()  # work tracking queue
    tq = mp.Queue()  # termination queue

    terminator = mp.Process(
        name="worker_terminator",
        target=_worker_terminator,
        args=(tq, worker_sleep_interval, logger),
        daemon=True,
    )
    terminator.start()  # start terminator

    with pytest.raises(QueueWorkDispatcherError) as err_ctx:
        # Start dispatcher with work, and with the inter-process Queue so it can pass along its PID
        # Passing its PID on this Queue will let the terminator know the worker to terminate
        dispatcher.dispatch(
            _work_to_be_terminated, termination_queue=tq, work_tracking_queue=wq, exit_handler=hanging_cleanup
        )

    # Ensure to wait on the processes to end with join
    dispatcher._worker_process.join(2)  # wait at most 2 sec for the work to complete
    terminator.join(1)  # ensure terminator completes within 3 seconds. Don't let it run away.
    timeout_error_fragment = (
        "Could not perform cleanup during exiting "
        "of job in allotted _exit_handling_timeout ({}s)".format(cleanup_timeout)
    )
    assert timeout_error_fragment in str(
        err_ctx.value
    ), "QueueWorkDispatcherError did not mention exit_handling timeouts"
    assert err_ctx.match("after 2 tries"), "QueueWorkDispatcherError did not mention '2 tries'"

    try:
        # Worker process should have an exitcode less than zero
        assert dispatcher._worker_process.exitcode < 0
        assert -signal.SIGTERM == dispatcher._worker_process.exitcode
        # Message SHOULD have been deleted from the queue (after going to DLQ)
        msgs = queue.receive_messages(WaitTimeSeconds=0)
        assert msgs is not None
        assert len(msgs) == 0, "Should be NO messages received from queue"
        # If the job function was called, it should have put the task_id (msg_body in this case) into the
        # work_tracking_queue as its first queued item
        work = wq.get_nowait()
        assert work == msg_body, "Was expecting to find worker task_id (msg_body) tracked in the queue"
        cleanup_attempt_1 = wq.get(True, 1)
        assert cleanup_attempt_1 == "cleanup_start_{}".format(
            msg_body
        ), "Was expecting to find a trace of cleanup attempt 1 tracked in the work queue"
        cleanup_attempt_2 = wq.get(True, 1)
        assert cleanup_attempt_2 == "cleanup_start_{}".format(
            msg_body
        ), "Was expecting to find a trace of cleanup attempt 2 tracked in the work queue"
        assert wq.empty(), "There should be no more work tracked on the work Queue"
        # TODO: Test that the "dead letter queue" has this message - maybe by asserting the log statement
    finally:
        _fail_runaway_processes(logger, worker=dispatcher._worker_process, terminator=terminator)


@pytest.mark.signal_handling  # see mark doc in pyproject.toml
def test_hanging_cleanup_of_signaled_parent_fails_dispatcher_and_sends_to_dlq(fake_sqs_queue):
    """When detecting the parent dispatcher process received an exit signal, and the parent dispatcher
    process is handling cleanup and termination of the child worker process, if the cleanup hangs for longer
    than the allowed exit_handling_timeout, it will short-circuit, and retry a 2nd time. If that also hangs
    longer than exit_handling_timeout, the message will be put on the dead letter queue, deleted from the

    Slight differences from :meth:`test_hanging_cleanup_fails_dispatcher_and_sends_to_dlq` is that the child
    worker process is not "dead" when doing these exit_handling cleanups.
    """
    logger = logging.getLogger(__name__ + "." + inspect.stack()[0][3])
    logger.setLevel(logging.DEBUG)

    msg_body = randint(1111, 9998)
    queue = fake_sqs_queue
    queue.send_message(MessageBody=msg_body)
    worker_sleep_interval = 0.05  # how long to "work"

    def hanging_cleanup(task_id, termination_queue: mp.Queue, work_tracking_queue: mp.Queue, queue_message):
        work_tracking_queue.put("cleanup_start_{}".format(queue_message.body), block=True, timeout=0.5)
        sleep(2.5)  # sleep for longer than the allowed time for exit handling
        # Should never get to this point, since it should be short-circuited by exit_handling_timeout
        work_tracking_queue.put("cleanup_end_{}".format(queue_message.body), block=True, timeout=0.5)

    cleanup_timeout = int(worker_sleep_interval + 1)
    dispatcher = SQSWorkDispatcher(
        queue,
        worker_process_name="Test Worker Process",
        long_poll_seconds=0,
        monitor_sleep_time=0.05,
        exit_handling_timeout=cleanup_timeout,
    )

    wq = mp.Queue()  # work tracking queue
    tq = mp.Queue()  # termination queue
    eq = mp.Queue()  # error queue

    def error_handling_dispatcher(work_dispatcher: SQSWorkDispatcher, error_queue: mp.Queue, **kwargs):
        try:
            work_dispatcher.dispatch(**kwargs)
        except Exception as exc:
            error_queue.put(exc, timeout=2)
            raise exc

    dispatch_kwargs = {
        "job": _work_to_be_terminated,
        "termination_queue": tq,
        "work_tracking_queue": wq,
        "exit_handler": hanging_cleanup,
    }
    parent_dispatcher = mp.Process(target=error_handling_dispatcher, args=(dispatcher, eq), kwargs=dispatch_kwargs)
    parent_dispatcher.start()

    # block until worker is running, or fail in 3 seconds
    work_done = wq.get(True, 3)
    assert work_done == msg_body

    parent_pid = parent_dispatcher.pid
    parent_proc = ps.Process(parent_pid)

    # block until received object indicating ready to be terminated, or fail in 1 second
    worker_pid = tq.get(True, 1)
    worker_proc = ps.Process(worker_pid)
    parent_proc.terminate()  # kills with SIGTERM signal

    # Wait at most this many seconds for the worker and dispatcher to finish, let TimeoutExpired raise and fail.
    try:
        # Check the same process that was started is still running. Must also check equality in case pid recycled
        while worker_proc.is_running() and ps.pid_exists(worker_pid) and worker_proc == ps.Process(worker_pid):
            wait_while = 5
            logger.debug(
                f"Waiting {wait_while}s for worker to complete after parent received kill signal. "
                f"worker pid = {worker_pid}, worker status = {worker_proc.status()}"
            )
            worker_proc.wait(timeout=wait_while)
    except TimeoutExpired as tex:
        pytest.fail(f"TimeoutExpired waiting for worker with pid {worker_proc.pid} to terminate (complete work).", tex)
    try:
        # Check the same process that was started is still running. Must also check equality in case pid recycled
        while (
            parent_dispatcher.is_alive()
            and parent_proc.is_running()
            and ps.pid_exists(parent_pid)
            and parent_proc == ps.Process(parent_pid)
        ):
            wait_while = 3
            logger.debug(
                f"Waiting {wait_while}s for parent dispatcher to complete after kill signal. "
                f"parent_dispatcher pid = {parent_pid}, parent_dispatcher status = {parent_proc.status()}"
            )
            parent_dispatcher.join(timeout=wait_while)
    except TimeoutExpired as tex:
        pytest.fail(
            f"TimeoutExpired waiting for parent dispatcher with pid {parent_pid} to terminate (complete work).", tex
        )

    assert not eq.empty(), "Should have been errors detected in parent dispatcher"
    exc = eq.get_nowait()
    assert isinstance(exc, QueueWorkDispatcherError), "Error was not of type QueueWorkDispatcherError"

    timeout_error_fragment = (
        "Could not perform cleanup during exiting "
        "of job in allotted _exit_handling_timeout ({}s)".format(cleanup_timeout)
    )
    assert timeout_error_fragment in str(exc), "QueueWorkDispatcherError did not mention exit_handling timeouts"
    assert "after 2 tries" in str(exc), "QueueWorkDispatcherError did not mention '2 tries'"

    try:
        # Parent dispatcher process should have an exit code of 1, since it will have raised an exception and
        # failed, rather than a graceful exit
        assert not parent_dispatcher.is_alive(), "Parent dispatcher is still alive but should have been killed"
        assert parent_dispatcher.exitcode is not None, "Parent dispatcher is not alive but has no exitcode"
        assert 1 == parent_dispatcher.exitcode, "Parent dispatcher exitcode was not 1"

        # Message SHOULD have been deleted from the queue (after going to DLQ)
        msgs = queue.receive_messages(WaitTimeSeconds=0)
        assert msgs is not None
        assert len(msgs) == 0, "Should be NO messages received from queue"
        # If the job function was called, it should have put the task_id (msg_body in this case) into the
        # work_tracking_queue as its first queued item
        assert work_done == msg_body, "Was expecting to find worker task_id (msg_body) tracked in the queue"
        cleanup_attempt_1 = wq.get(True, 1)
        assert cleanup_attempt_1 == "cleanup_start_{}".format(
            msg_body
        ), "Was expecting to find a trace of cleanup attempt 1 tracked in the work queue"
        cleanup_attempt_2 = wq.get(True, 1)
        assert cleanup_attempt_2 == "cleanup_start_{}".format(
            msg_body
        ), "Was expecting to find a trace of cleanup attempt 2 tracked in the work queue"
        assert wq.empty(), "There should be no more work tracked on the work Queue"
        # TODO: Test that the "dead letter queue" has this message - maybe by asserting the log statement
    finally:
        if worker_proc and worker_proc.is_running():
            logger.warning(
                "Dispatched worker process with PID {} did not complete in timeout. "
                "Killing it.".format(worker_proc.pid)
            )
            os.kill(worker_proc.pid, signal.SIGKILL)
            pytest.fail("Worker did not complete in timeout as expected. Test fails.")
        _fail_runaway_processes(logger, dispatcher=parent_dispatcher)


def _error_handling_dispatcher(work_dispatcher: SQSWorkDispatcher, error_queue: mp.Queue, **kwargs):
    """Wrapper around an SQSWorkDispatcher that catches errors raised and puts them on the given multiprocess error
    Queue, for inspection in tests"""
    try:
        work_dispatcher.dispatch(**kwargs)
    except Exception as exc:
        error_queue.put_nowait(exc)
        raise exc


def _hanging_cleanup_if_worker_alive(
    task_id, termination_queue: mp.Queue, work_tracking_queue: mp.Queue, queue_message, cleanup_timeout: float
):
    """An implementation of an exit handler, that will purposefully hang during the exit handling function (aka
    'cleanup') for longer than the exit handling timeout, in order to short-circuit cleanup, and try again with a
    killed worker"""
    cleanup_logger = logging.getLogger(__name__ + "." + inspect.stack()[0][3])
    cleanup_logger.setLevel(logging.DEBUG)
    cleanup_logger.debug("CLEANUP CLEANUP CLEANUP !!!!!!!!!!!!!!")

    work_tracking_queue.put("cleanup_start_{}".format(queue_message.body), block=True, timeout=0.5)

    sleep_before_cleanup = 0.25
    cleanup_logger.debug(
        "Sleeping for {} seconds to allow worker process state to stabilize before "
        "inspecting it during this cleanup.".format(sleep_before_cleanup)
    )
    sleep(sleep_before_cleanup)

    # Get the PID of the worker off the termination_queue, then put it back on, as if it wasn't removed
    worker_pid_during_cleanup = termination_queue.get(True, 1)
    termination_queue.put(worker_pid_during_cleanup, block=True, timeout=0.5)

    worker_during_cleanup = ps.Process(worker_pid_during_cleanup)

    # Log what psutil sees of this worker. 'zombie' is what we're looking for if it was killed
    cleanup_logger.debug(
        "psutil.pid_exists({}) = {}".format(worker_pid_during_cleanup, ps.pid_exists(worker_pid_during_cleanup))
    )
    cleanup_logger.debug(
        "worker_during_cleanup.is_running() = {} [PID={}]".format(
            worker_during_cleanup.is_running(), worker_pid_during_cleanup
        )
    )
    cleanup_logger.debug(
        "worker_during_cleanup.status() = {} [PID={}]".format(worker_during_cleanup.status(), worker_pid_during_cleanup)
    )
    if worker_during_cleanup.status() == ps.STATUS_ZOMBIE:
        # This case should only happen when the worker was dead/killed when this cleanup ran
        cleanup_logger.debug(
            "Found ZOMBIE worker during cleanup. "
            "Putting tracer about dead worker in the tracking queue...[PID={}]".format(worker_pid_during_cleanup)
        )
        payload = "cleanup_end_with_dead_worker_{}".format(queue_message.body)
        work_tracking_queue.put(payload, block=True, timeout=cleanup_timeout / 2)
        cleanup_logger.debug(
            "... Tracer about dead worker the tracking queue [PID={}]".format(worker_pid_during_cleanup)
        )
    else:  # hang cleanup if worker is running
        sleep(cleanup_timeout + 0.5)  # sleep for longer than the allowed time for exit handling

        # Should NEVER get to this point, since the timeout wrapper should interrupt during the above sleep
        cleanup_logger.error(
            "Timeout wrapper DID NOT interrupt sleeping cleanup (should have) [PID={}]".format(
                worker_pid_during_cleanup
            )
        )
        payload = "cleanup_end_with_live_worker_{}".format(queue_message.body)
        work_tracking_queue.put(payload, block=True, timeout=cleanup_timeout / 2)


@pytest.mark.signal_handling  # see mark doc in pyproject.toml
def test_cleanup_second_try_succeeds_after_killing_worker_with_dlq(fake_sqs_queue):
    """Simulate an exit_handler that interferes with work that was being done by the worker, to see that we can
    kill it and resume trying the exit handler.

    When detecting the parent dispatcher process received an exit signal, and the parent dispatcher
    process is handling cleanup and termination of the child worker process, if the cleanup hangs for longer
    than the allowed exit_handling_timeout, it will short-circuit, and retry a 2nd time. Before attempting
    that second time, it will kill the worker process. This test configured the exit_handler to hang if the
    worker is detected to still be alive, and proceeds with "cleanup" if it is detected as killed. It
    therefore simulates successful cleanup on the 2nd try.
    """
    logger = logging.getLogger(__name__ + "." + inspect.stack()[0][3])
    logger.setLevel(logging.DEBUG)

    msg_body = randint(1111, 9998)
    queue = fake_sqs_queue
    queue.send_message(MessageBody=msg_body)
    worker_sleep_interval = 0.05  # how long to "work"
    cleanup_timeout = worker_sleep_interval + 3  # how long to allow cleanup to run, max

    dispatcher = SQSWorkDispatcher(
        queue,
        worker_process_name="Test Worker Process",
        long_poll_seconds=0,
        monitor_sleep_time=0.05,
        exit_handling_timeout=int(cleanup_timeout),
    )

    wq = mp.Queue()  # work tracking queue
    tq = mp.Queue()  # termination queue
    eq = mp.Queue()  # error queue

    dispatch_kwargs = {
        "job": _work_to_be_terminated,
        "termination_queue": tq,
        "work_tracking_queue": wq,
        "cleanup_timeout": cleanup_timeout,
        "exit_handler": _hanging_cleanup_if_worker_alive,
    }
    parent_dispatcher = mp.Process(target=_error_handling_dispatcher, args=(dispatcher, eq), kwargs=dispatch_kwargs)
    parent_dispatcher.start()

    # block until worker is running, or fail in 3 seconds
    work_done = wq.get(True, 3)
    assert work_done == msg_body

    parent_pid = parent_dispatcher.pid
    parent_proc = ps.Process(parent_pid)

    # block until received object indicating ready to be terminated, or fail in 1 second
    worker_pid = tq.get(True, 1)
    worker_proc = ps.Process(worker_pid)

    # Put right back on termination queue as if not removed, so exit_handler can discover the worker process PID
    # to perform its conditional hang-or-cleanup logic
    tq.put(worker_pid)
    parent_proc.terminate()  # kills with SIGTERM signal

    # Wait at most this many seconds for the worker and dispatcher to finish, let TimeoutExpired raise and fail.
    try:
        # Check the same process that was started is still running. Must also check equality in case pid recycled
        while worker_proc.is_running() and ps.pid_exists(worker_pid) and worker_proc == ps.Process(worker_pid):
            wait_while = 5
            logger.debug(
                f"Waiting {wait_while}s for worker to complete after parent received kill signal. "
                f"worker pid = {worker_pid}, worker status = {worker_proc.status()}"
            )
            worker_proc.wait(timeout=wait_while)
    except TimeoutExpired as tex:
        pytest.fail(f"TimeoutExpired waiting for worker with pid {worker_proc.pid} to terminate (complete work).", tex)
    try:
        # Check the same process that was started is still running. Must also check equality in case pid recycled
        while (
            parent_dispatcher.is_alive()
            and parent_proc.is_running()
            and ps.pid_exists(parent_pid)
            and parent_proc == ps.Process(parent_pid)
        ):
            wait_while = 3
            logger.debug(
                f"Waiting {wait_while}s for parent dispatcher to complete after kill signal. "
                f"parent_dispatcher pid = {parent_pid}, parent_dispatcher status = {parent_proc.status()}"
            )
            parent_dispatcher.join(timeout=wait_while)
    except TimeoutExpired as tex:
        pytest.fail(
            f"TimeoutExpired waiting for parent dispatcher with pid {parent_pid} to terminate (complete work).", tex
        )

    assert eq.empty(), (
        f"Expecting error_queue to be empty but it contains errors detected from parent dispatcher: "
        f"{eq.get_nowait()}"
    )
    try:
        # Parent dispatcher process should have gracefully exited after receiving SIGTERM (exit code of -15)
        assert -signal.SIGTERM == parent_dispatcher.exitcode

        # Message SHOULD have been deleted from the queue (after going to DLQ)
        msgs = queue.receive_messages(WaitTimeSeconds=0)
        assert msgs is not None
        assert len(msgs) == 0, "Should be NO messages received from queue"
        # If the job function was called, it should have put the task_id (msg_body in this case) into the
        # work_tracking_queue as its first queued item
        assert work_done == msg_body, "Was expecting to find worker task_id (msg_body) tracked in the queue"
        fail1_msg = "Was expecting to find a trace of cleanup attempt 1 tracked in the work queue"
        try:
            cleanup_attempt_1 = wq.get(True, 1)
            assert cleanup_attempt_1 == "cleanup_start_{}".format(msg_body), fail1_msg
        except Empty as e:
            pytest.fail(fail1_msg + ", but queue was empty", e)

        fail2_msg = "Was expecting to find a trace of cleanup attempt 2 tracked in the work queue"
        try:
            cleanup_attempt_2 = wq.get(True, 1)
            assert cleanup_attempt_2 == "cleanup_start_{}".format(msg_body), fail2_msg
        except Empty as e:
            pytest.fail(fail2_msg + ", but queue was empty", e)

        failend_msg = "Was expecting to find a trace of cleanup reaching its end, after worker was killed (try 2)"
        try:
            cleanup_end = wq.get(True, 1)
            assert cleanup_end == "cleanup_end_with_dead_worker_{}".format(msg_body), failend_msg
        except Empty as e:
            pytest.fail(failend_msg + ", but queue was empty", e)

        assert wq.empty(), "There should be no more work tracked on the work Queue"
        # TODO: Test that the "dead letter queue" has this message - maybe by asserting the log statement
    finally:
        if worker_proc and worker_proc.is_running():
            logger.warning(
                "Dispatched worker process with PID {} did not complete in timeout. "
                "Killing it.".format(worker_proc.pid)
            )
            os.kill(worker_proc.pid, signal.SIGKILL)
            pytest.fail("Worker did not complete in timeout as expected. Test fails.")
        _fail_runaway_processes(logger, dispatcher=parent_dispatcher)


@pytest.mark.signal_handling  # see mark doc in pyproject.toml
def test_cleanup_second_try_succeeds_after_killing_worker_with_retry(fake_sqs_queue):
    """Same as :meth:`test_cleanup_second_try_succeeds_after_killing_worker_with_dlq`, but this queue allows
    retries. Changes asserts to ensure the message gets retried after cleanup.
    """
    logger = logging.getLogger(__name__ + "." + inspect.stack()[0][3])
    logger.setLevel(logging.DEBUG)

    msg_body = randint(1111, 9998)
    queue = fake_sqs_queue
    queue.send_message(MessageBody=msg_body)
    worker_sleep_interval = 0.05  # how long to "work"
    cleanup_timeout = worker_sleep_interval + 3.0  # how long to allow cleanup to run, max

    dispatcher = SQSWorkDispatcher(
        queue,
        worker_process_name="Test Worker Process",
        long_poll_seconds=0,
        monitor_sleep_time=0.05,
        exit_handling_timeout=int(cleanup_timeout),
    )
    dispatcher.sqs_queue_instance.max_receive_count = 2  # allow retries

    wq = mp.Queue()  # work tracking queue
    tq = mp.Queue()  # termination queue
    eq = mp.Queue()  # error queue

    dispatch_kwargs = {
        "job": _work_to_be_terminated,
        "termination_queue": tq,
        "work_tracking_queue": wq,
        "cleanup_timeout": cleanup_timeout,
        "exit_handler": _hanging_cleanup_if_worker_alive,
    }
    parent_dispatcher = mp.Process(target=_error_handling_dispatcher, args=(dispatcher, eq), kwargs=dispatch_kwargs)
    parent_dispatcher.start()

    # block until worker is running, or fail in 3 seconds
    work_done = wq.get()
    assert work_done == msg_body

    parent_pid = parent_dispatcher.pid
    parent_proc = ps.Process(parent_pid)

    # block until received object indicating ready to be terminated, or fail in 1 second
    worker_pid = tq.get(True, 1)
    worker_proc = ps.Process(worker_pid)

    # Put right back on termination queue as if not removed, so exit_handler can discover the worker process PID
    # to perform its conditional hang-or-cleanup logic
    tq.put(worker_pid)
    parent_proc.terminate()  # kills with SIGTERM signal

    # Wait at most this many seconds for the worker and dispatcher to finish, let TimeoutExpired raise and fail.
    try:
        # Check the same process that was started is still running. Must also check equality in case pid recycled
        while worker_proc.is_running() and ps.pid_exists(worker_pid) and worker_proc == ps.Process(worker_pid):
            wait_while = 5
            logger.debug(
                f"Waiting {wait_while}s for worker to complete after parent received kill signal. "
                f"worker pid = {worker_pid}, worker status = {worker_proc.status()}"
            )
            worker_proc.wait(timeout=wait_while)
    except TimeoutExpired as tex:
        pytest.fail(f"TimeoutExpired waiting for worker with pid {worker_proc.pid} to terminate (complete work).", tex)
    try:
        # Check the same process that was started is still running. Must also check equality in case pid recycled
        while (
            parent_dispatcher.is_alive()
            and parent_proc.is_running()
            and ps.pid_exists(parent_pid)
            and parent_proc == ps.Process(parent_pid)
        ):
            wait_while = 3
            logger.debug(
                f"Waiting {wait_while}s for parent dispatcher to complete after kill signal. "
                f"parent_dispatcher pid = {parent_pid}, parent_dispatcher status = {parent_proc.status()}"
            )
            parent_dispatcher.join(timeout=wait_while)
    except TimeoutExpired as tex:
        pytest.fail(
            f"TimeoutExpired waiting for parent dispatcher with pid {parent_pid} to terminate (complete work).", tex
        )

    try:
        # Parent dispatcher process should have gracefully exited after receiving SIGTERM (exit code of -15)
        assert parent_dispatcher.exitcode == -signal.SIGTERM

        # Message should NOT have been deleted from the queue, but available for receive again, because this
        # dispatcher is configured to allow retries
        msgs = queue.receive_messages(WaitTimeSeconds=0)
        assert msgs is not None
        assert len(msgs) == 1, "Should be only 1 message received from queue"
        assert msgs[0].body == msg_body, "Should be the same message available for retry on the queue"

        # If the job function was called, it should have put the task_id (msg_body in this case) into the
        # work_tracking_queue as its first queued item
        assert work_done == msg_body, "Was expecting to find worker task_id (msg_body) tracked in the queue"
        fail1_msg = "Was expecting to find a trace of cleanup attempt 1 tracked in the work queue"
        try:
            cleanup_attempt_1 = wq.get(True, 1)
            assert cleanup_attempt_1 == "cleanup_start_{}".format(msg_body), fail1_msg
        except Empty as e:
            pytest.fail(fail1_msg + ", but queue was empty", e)

        fail2_msg = "Was expecting to find a trace of cleanup attempt 2 tracked in the work queue"
        try:
            cleanup_attempt_2 = wq.get(True, 1)
            assert cleanup_attempt_2 == "cleanup_start_{}".format(msg_body), fail2_msg
        except Empty as e:
            pytest.fail(fail2_msg + ", but queue was empty", e)

        failend_msg = "Was expecting to find a trace of cleanup reaching its end, after worker was killed (try 2)"
        try:
            cleanup_end = wq.get(True, 1)
            assert cleanup_end == "cleanup_end_with_dead_worker_{}".format(msg_body), failend_msg
        except Empty as e:
            pytest.fail(failend_msg + ", but queue was empty", e)

        assert wq.empty(), "There should be no more work tracked on the work Queue"
    finally:
        if worker_proc and worker_proc.is_running():
            logger.warning(
                "Dispatched worker process with PID {} did not complete in timeout. "
                "Killing it.".format(worker_proc.pid)
            )
            os.kill(worker_proc.pid, signal.SIGKILL)
            pytest.fail("Worker did not complete in timeout as expected. Test fails.")
    _fail_runaway_processes(logger, dispatcher=parent_dispatcher)


@pytest.mark.signal_handling  # see mark doc in pyproject.toml
def test_worker_process_error_exception_data(fake_sqs_queue):
    unknown_worker = "Unknown Worker"
    no_queue_message = "Queue Message None or not provided"
    queue = fake_sqs_queue
    fake_queue_message = FakeSQSMessage(queue.url)

    def raise_exc_no_args():
        raise QueueWorkerProcessError()

    with pytest.raises(QueueWorkerProcessError) as err_ctx1:
        raise_exc_no_args()
    assert err_ctx1.match(unknown_worker), f"QueueWorkerProcessError did not mention '{unknown_worker}'."
    assert err_ctx1.match(
        "Queue Message None or not provided"
    ), f"QueueWorkerProcessError did not mention '{no_queue_message}'."
    assert unknown_worker == err_ctx1.value.worker_process_name
    assert err_ctx1.value.queue_message is None

    def raise_exc_with_message():
        raise QueueWorkerProcessError("A special message")

    with pytest.raises(QueueWorkerProcessError) as err_ctx2:
        raise_exc_with_message()
    assert err_ctx2.match("A special message"), "QueueWorkerProcessError did not mention 'A special message'."
    assert unknown_worker == err_ctx2.value.worker_process_name
    assert err_ctx2.value.queue_message is None

    def raise_exc_with_qmsg():
        fake_queue_message.body = 1133
        raise QueueWorkerProcessError(queue_message=fake_queue_message)

    with pytest.raises(QueueWorkerProcessError) as err_ctx3:
        raise_exc_with_qmsg()
    assert 1133 == err_ctx3.value.queue_message.body
    assert unknown_worker == err_ctx3.value.worker_process_name
    assert err_ctx3.match("1133"), "QueueWorkerProcessError did not mention '1133'."
    assert err_ctx3.match(unknown_worker), f"QueueWorkerProcessError did not mention '{unknown_worker}'."

    def raise_exc_with_qmsg_and_message():
        fake_queue_message.body = 1144
        raise QueueWorkerProcessError("Custom Message about THIS", queue_message=fake_queue_message)

    with pytest.raises(QueueWorkerProcessError) as err_ctx4:
        raise_exc_with_qmsg_and_message()
    assert err_ctx4.match(
        "Custom Message about THIS"
    ), "QueueWorkerProcessError did not mention 'Custom Message about THIS'."
    assert "1144" not in str(
        err_ctx4.value
    ), "QueueWorkerProcessError seems to be using the default message, not the given message"
    assert 1144 == err_ctx4.value.queue_message.body
    assert unknown_worker == err_ctx4.value.worker_process_name

    def raise_exc_with_worker_process_name_and_message():
        raise QueueWorkerProcessError("Custom Message about THIS", worker_process_name="MyJob")

    with pytest.raises(QueueWorkerProcessError) as err_ctx5:
        raise_exc_with_worker_process_name_and_message()
    assert err_ctx5.match(
        "Custom Message about THIS"
    ), "QueueWorkerProcessError did not mention 'Custom Message about THIS'."
    assert "MyJob" not in str(
        err_ctx5.value
    ), "QueueWorkerProcessError seems to be using the default message, not the given message"
    assert err_ctx5.value.queue_message is None
    assert "MyJob" == err_ctx5.value.worker_process_name

    def raise_exc_with_qmsg_and_worker_process_name_and_message():
        fake_queue_message.body = 1166
        raise QueueWorkerProcessError(
            "Custom Message about THIS", worker_process_name="MyJob", queue_message=fake_queue_message
        )

    with pytest.raises(QueueWorkerProcessError) as err_ctx6:
        raise_exc_with_qmsg_and_worker_process_name_and_message()
    assert err_ctx6.match(
        "Custom Message about THIS"
    ), "QueueWorkerProcessError did not mention 'Custom Message about THIS'."
    assert "MyJob" not in str(
        err_ctx6.value
    ), "QueueWorkerProcessError seems to be using the default message, not the given message"
    assert "1166" not in str(
        err_ctx6.value
    ), "QueueWorkerProcessError seems to be using the default message, not the given message"
    assert 1166 == err_ctx6.value.queue_message.body
    assert "MyJob" == err_ctx6.value.worker_process_name

    def raise_exc_with_job_id_and_worker_process_name_and_no_message():
        fake_queue_message.body = 1177
        raise QueueWorkerProcessError(worker_process_name="MyJob", queue_message=fake_queue_message)

    with pytest.raises(QueueWorkerProcessError) as err_ctx7:
        raise_exc_with_job_id_and_worker_process_name_and_no_message()
    assert err_ctx7.match(
        "MyJob"
    ), "QueueWorkerProcessError seems to be using the default message, not the given message"
    assert err_ctx7.match(
        "1177"
    ), "QueueWorkerProcessError seems to be using the default message, not the given message"
    assert 1177 == err_ctx7.value.queue_message.body
    assert "MyJob" == err_ctx7.value.worker_process_name


def _worker_terminator(terminate_queue: mp.Queue, sleep_interval=0, logger=logging.getLogger(__name__)):
    logger.debug("Started worker_terminator. Waiting for the Queue to surface a PID to be terminated.")
    # Wait until there is a worker in the given dispatcher to kill
    pid = None
    while not pid:
        logger.debug("No work yet to be terminated. Waiting {} seconds".format(sleep_interval))
        sleep(sleep_interval)
        pid = terminate_queue.get()

    # Process is running. Now terminate it with signal.SIGTERM
    logger.debug("Found work to be terminated: Worker PID=[{}]".format(pid))
    ps.Process(pid).terminate()  # kills with SIGTERM signal
    logger.debug("Terminated worker with PID=[{}] using signal.SIGTERM".format(pid))


def _work_to_be_terminated(task_id, termination_queue: mp.Queue, work_tracking_queue: mp.Queue, *args, **kwargs):
    # Put task_id in the work_tracking_queue so we know we saw this work
    work_tracking_queue.put(task_id)
    # Put PID of worker process in the termination_queue to let worker_terminator proc know what to kill
    termination_queue.put(os.getpid())
    sleep(0.25)  # hang for a short period to ensure terminator has time to kill this


def _fail_runaway_processes(
    logger, worker: mp.Process = None, terminator: mp.Process = None, dispatcher: mp.Process = None
):
    fail_with_runaway_proc = False
    if worker and worker.is_alive() and ps.pid_exists(worker.pid):
        os.kill(worker.pid, signal.SIGKILL)
        logger.warning(
            "Dispatched worker process with PID {} did not complete in timeout and was killed.".format(worker.pid)
        )
        fail_with_runaway_proc = True
    if terminator and terminator.is_alive() and ps.pid_exists(terminator.pid):
        os.kill(terminator.pid, signal.SIGKILL)
        logger.warning(
            "Terminator process with PID {} did not complete in timeout and was killed.".format(terminator.pid)
        )
        fail_with_runaway_proc = True
    if dispatcher and dispatcher.is_alive() and ps.pid_exists(dispatcher.pid):
        os.kill(dispatcher.pid, signal.SIGKILL)
        logger.warning(
            "Parent dispatcher process with PID {} did not complete in timeout and was killed.".format(dispatcher.pid)
        )
        fail_with_runaway_proc = True
    if fail_with_runaway_proc:
        pytest.fail("Worker or its Terminator or the Dispatcher did not complete in timeout as expected. Test fails.")

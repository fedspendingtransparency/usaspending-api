import inspect
import boto3
import logging
import multiprocessing as mp
import os
import signal
import psutil as ps

from unittest import TestCase
from random import randint

import pytest
from botocore.config import Config
from botocore.exceptions import EndpointConnectionError, ClientError, NoCredentialsError, NoRegionError
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


@pytest.mark.usefixtures("_patch_get_sqs_queue")
class SQSWorkDispatcherTests(TestCase):
    def test_dispatch_with_default_numeric_message_body_succeeds(self):
        """ SQSWorkDispatcher can execute work on a numeric message body successfully

            - Given a numeric message body
            - When on a SQSWorkDispatcher.dispatch() is called
            - Then the default message_transformer provides it as an argument to the job
        """
        queue = get_sqs_queue()
        queue.send_message(MessageBody=1234)

        dispatcher = SQSWorkDispatcher(
            queue, worker_process_name="Test Worker Process", long_poll_seconds=1, monitor_sleep_time=1
        )

        def do_some_work(task_id):
            self.assertEqual(1234, task_id)  # assert the message body is passed in as arg by default

            # The "work" we're doing is just putting something else on the queue
            queue_in_use = get_sqs_queue()
            queue_in_use.send_message(MessageBody=9999)

        dispatcher.dispatch(do_some_work)
        dispatcher._worker_process.join(5)  # wait at most 5 sec for the work to complete

        # Make sure the "work" was done
        messages = queue.receive_messages(WaitTimeSeconds=1, MaxNumberOfMessages=10)
        self.assertEqual(1, len(messages))
        self.assertEqual(9999, messages[0].body)

        # Worker process should have a successful (0) exitcode
        self.assertEqual(0, dispatcher._worker_process.exitcode)

    def test_dispatch_with_single_dict_item_message_transformer_succeeds(self):
        """ SQSWorkDispatcher can execute work on a numeric message body provided from the message
            transformer as a dict

            - Given a numeric message body
            - When on a SQSWorkDispatcher.dispatch() is called
            - And a message_transformer wraps that message body into a dict,
                keyed by the name of the single param of the job
            - Then the dispatcher provides the dict item's value to the job when invoked
        """
        queue = get_sqs_queue()
        queue.send_message(MessageBody=1234)

        dispatcher = SQSWorkDispatcher(
            queue, worker_process_name="Test Worker Process", long_poll_seconds=1, monitor_sleep_time=1
        )

        def do_some_work(task_id):
            self.assertEqual(1234, task_id)  # assert the message body is passed in as arg by default

            # The "work" we're doing is just putting something else on the queue
            queue_in_use = get_sqs_queue()
            queue_in_use.send_message(MessageBody=9999)

        dispatcher.dispatch(do_some_work, message_transformer=lambda x: {"task_id": x.body})
        dispatcher._worker_process.join(5)  # wait at most 5 sec for the work to complete

        # Make sure the "work" was done
        messages = queue.receive_messages(WaitTimeSeconds=1, MaxNumberOfMessages=10)
        self.assertEqual(1, len(messages))
        self.assertEqual(9999, messages[0].body)

        # Worker process should have a successful (0) exitcode
        self.assertEqual(0, dispatcher._worker_process.exitcode)

    def test_dispatch_with_multi_arg_message_transformer_succeeds(self):
        """ SQSWorkDispatcher can execute work when a message_transformer provides tuple-based args to use

            - Given a message_transformer that returns a tuple as the arguments
            - When on a SQSWorkDispatcher.dispatch() is called
            - Then the given job will run with those unnamed (positional) args
        """
        queue = get_sqs_queue()
        queue.send_message(MessageBody=1234)

        dispatcher = SQSWorkDispatcher(
            queue, worker_process_name="Test Worker Process", long_poll_seconds=1, monitor_sleep_time=1
        )

        def do_some_work(task_id, task_id_times_two):
            self.assertEqual(1234, task_id)
            self.assertEqual(2468, task_id_times_two)

            # The "work" we're doing is just putting something else on the queue
            queue_in_use = get_sqs_queue()
            queue_in_use.send_message(MessageBody=9999)

        dispatcher.dispatch(do_some_work, message_transformer=lambda x: (x.body, x.body * 2))
        dispatcher._worker_process.join(5)  # wait at most 5 sec for the work to complete

        # Make sure the "work" was done
        messages = queue.receive_messages(WaitTimeSeconds=1, MaxNumberOfMessages=10)
        self.assertEqual(1, len(messages))
        self.assertEqual(9999, messages[0].body)

        # Worker process should have a successful (0) exitcode
        self.assertEqual(0, dispatcher._worker_process.exitcode)

    def test_dispatch_with_multi_kwarg_message_transformer_succeeds(self):
        """ SQSWorkDispatcher can execute work when a message_transformer provides dict-based args to use

            - Given a message_transformer that returns a tuple as the arguments
            - When on a SQSWorkDispatcher.dispatch() is called
            - Then the given job will run with those unnamed (positional) args
        """
        queue = get_sqs_queue()
        queue.send_message(MessageBody=1234)

        dispatcher = SQSWorkDispatcher(
            queue, worker_process_name="Test Worker Process", long_poll_seconds=1, monitor_sleep_time=1
        )

        def do_some_work(task_id, task_id_times_two):
            self.assertEqual(1234, task_id)
            self.assertEqual(2468, task_id_times_two)

            # The "work" we're doing is just putting something else on the queue
            queue_in_use = get_sqs_queue()
            queue_in_use.send_message(MessageBody=9999)

        dispatcher.dispatch(
            do_some_work, message_transformer=lambda x: {"task_id": x.body, "task_id_times_two": x.body * 2}
        )
        dispatcher._worker_process.join(5)  # wait at most 5 sec for the work to complete

        # Make sure the "work" was done
        messages = queue.receive_messages(WaitTimeSeconds=1, MaxNumberOfMessages=10)
        self.assertEqual(1, len(messages))
        self.assertEqual(9999, messages[0].body)

        # Worker process should have a successful (0) exitcode
        self.assertEqual(0, dispatcher._worker_process.exitcode)

    def test_additional_job_args_can_be_passed(self):
        """ Additional args can be passed to the job to execute

            This should combine the singular element arg from the message transformer with the additional
            tuple-based args given as positional args
        """
        queue = get_sqs_queue()
        queue.send_message(MessageBody=1234)

        dispatcher = SQSWorkDispatcher(
            queue, worker_process_name="Test Worker Process", long_poll_seconds=1, monitor_sleep_time=1
        )

        def do_some_work(task_id, category):
            self.assertEqual(task_id, 1234)  # assert the message body is passed in as arg by default
            self.assertEqual(category, "easy work")

            # The "work" we're doing is just putting something else on the queue
            queue_in_use = get_sqs_queue()
            queue_in_use.send_message(MessageBody=9999)

        dispatcher.dispatch(do_some_work, "easy work")

        dispatcher._worker_process.join(5)  # wait at most 5 sec for the work to complete

        # Make sure the "work" was done
        messages = queue.receive_messages(WaitTimeSeconds=1, MaxNumberOfMessages=10)
        self.assertEqual(1, len(messages))
        self.assertEqual(9999, messages[0].body)

        # Worker process should have a successful (0) exitcode
        self.assertEqual(0, dispatcher._worker_process.exitcode)

    def test_additional_job_kwargs_can_be_passed(self):
        """ Additional args can be passed to the job to execute

            This should combine the singular element arg from the message transformer with the additional
            dictionary-based args given as keyword arguments
        """
        queue = get_sqs_queue()
        queue.send_message(MessageBody=1234)

        dispatcher = SQSWorkDispatcher(
            queue, worker_process_name="Test Worker Process", long_poll_seconds=1, monitor_sleep_time=1
        )

        def do_some_work(task_id, category):
            self.assertEqual(task_id, 1234)  # assert the message body is passed in as arg by default
            self.assertEqual(category, "easy work")

            # The "work" we're doing is just putting something else on the queue
            queue_in_use = get_sqs_queue()
            queue_in_use.send_message(MessageBody=9999)

        dispatcher.dispatch(do_some_work, category="easy work")

        dispatcher._worker_process.join(5)  # wait at most 5 sec for the work to complete

        # Make sure the "work" was done
        messages = queue.receive_messages(WaitTimeSeconds=1, MaxNumberOfMessages=10)
        self.assertEqual(1, len(messages))
        self.assertEqual(9999, messages[0].body)

        # Worker process should have a successful (0) exitcode
        self.assertEqual(0, dispatcher._worker_process.exitcode)

    def test_additional_job_kwargs_can_be_passed_alongside_dict_args_from_message_transformer(self):
        """ Additional args can be passed to the job to execute.

            This should combine the dictionary-based args from the message transformer with the additional
            dictionary-based args given as keyword arguments
        """
        queue = get_sqs_queue()
        queue.send_message(MessageBody=1234)

        dispatcher = SQSWorkDispatcher(
            queue, worker_process_name="Test Worker Process", long_poll_seconds=1, monitor_sleep_time=1
        )

        def do_some_work(task_id, category):
            self.assertEqual(task_id, 1234)  # assert the message body is passed in as arg by default
            self.assertEqual(category, "easy work")

            # The "work" we're doing is just putting something else on the queue
            queue_in_use = get_sqs_queue()
            queue_in_use.send_message(MessageBody=9999)

        dispatcher.dispatch(do_some_work, message_transformer=lambda x: {"task_id": x.body}, category="easy work")

        dispatcher._worker_process.join(5)  # wait at most 5 sec for the work to complete

        # Make sure the "work" was done
        messages = queue.receive_messages(WaitTimeSeconds=1, MaxNumberOfMessages=10)
        self.assertEqual(1, len(messages))
        self.assertEqual(9999, messages[0].body)

        # Worker process should have a successful (0) exitcode
        self.assertEqual(0, dispatcher._worker_process.exitcode)

    def test_dispatching_by_message_attribute_succeeds_with_job_args(self):
        """ SQSWorkDispatcher can read a message attribute to determine which function to call

            - Given a message with a user-defined message attribute
            - When on a SQSWorkDispatcher.dispatch_by_message_attribute() is called
            - And a message_transformer is given to route execution based on that message attribute
            - Then the correct function is executed
        """
        queue = get_sqs_queue()
        message_attr = {"work_type": {"DataType": "String", "StringValue": "a"}}
        queue.send_message(MessageBody=1234, MessageAttributes=message_attr)

        dispatcher = SQSWorkDispatcher(
            queue, worker_process_name="Test Worker Process", long_poll_seconds=1, monitor_sleep_time=1
        )

        def one_work(task_id):
            self.assertEqual(1234, task_id)  # assert the message body is passed in as arg by default

            # The "work" we're doing is just putting "a" on the queue
            queue_in_use = get_sqs_queue()
            queue_in_use.send_message(MessageBody=1)

        def two_work(task_id):
            self.assertEqual(1234, task_id)  # assert the message body is passed in as arg by default

            # The "work" we're doing is just putting "b" on the queue
            queue_in_use = get_sqs_queue()
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
        self.assertEqual(1, len(messages))
        self.assertEqual(1, messages[0].body)

        # Worker process should have a successful (0) exitcode
        self.assertEqual(0, dispatcher._worker_process.exitcode)

    def test_dispatching_by_message_attribute_succeeds_with_job_kwargs(self):
        """ SQSWorkDispatcher can read a message attribute to determine which function to call

            - Given a message with a user-defined message attribute
            - When on a SQSWorkDispatcher.dispatch_by_message_attribute() is called
            - And a message_transformer is given to route execution based on that message attribute
            - Then the correct function is executed
            - And it can get its keyword arguments for execution from the job_kwargs item of the
                message_transformer's returned dictionary
        """
        queue = get_sqs_queue()
        message_attr = {"work_type": {"DataType": "String", "StringValue": "a"}}
        queue.send_message(MessageBody=1234, MessageAttributes=message_attr)

        dispatcher = SQSWorkDispatcher(
            queue, worker_process_name="Test Worker Process", long_poll_seconds=1, monitor_sleep_time=1
        )

        def one_work(task_id):
            self.assertEqual(1234, task_id)  # assert the message body is passed in as arg by default

            # The "work" we're doing is just putting "a" on the queue
            queue_in_use = get_sqs_queue()
            queue_in_use.send_message(MessageBody=1)

        def two_work(task_id):
            self.assertEqual(1234, task_id)  # assert the message body is passed in as arg by default

            # The "work" we're doing is just putting "b" on the queue
            queue_in_use = get_sqs_queue()
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
        self.assertEqual(1, len(messages))
        self.assertEqual(1, messages[0].body)

        # Worker process should have a successful (0) exitcode
        self.assertEqual(0, dispatcher._worker_process.exitcode)

    def test_dispatching_by_message_attribute_succeeds_with_job_args_and_job_kwargs(self):
        """ SQSWorkDispatcher can read a message attribute to determine which function to call

            - Given a message with a user-defined message attribute
            - When on a SQSWorkDispatcher.dispatch_by_message_attribute() is called
            - And a message_transformer is given to route execution based on that message attribute
            - Then the correct function is executed
            - And it can get its args from the job_args item and keyword arguments for execution from the job_kwargs
                item of the message_transformer's returned dictionary
        """
        queue = get_sqs_queue()
        message_attr = {"work_type": {"DataType": "String", "StringValue": "a"}}
        queue.send_message(MessageBody=1234, MessageAttributes=message_attr)

        dispatcher = SQSWorkDispatcher(
            queue, worker_process_name="Test Worker Process", long_poll_seconds=1, monitor_sleep_time=1
        )

        def one_work(task_id, category):
            self.assertEqual(1234, task_id)  # assert the message body is passed in as arg by default
            self.assertEqual("one work", category)

            # The "work" we're doing is just putting 1 on the queue
            queue_in_use = get_sqs_queue()
            queue_in_use.send_message(MessageBody=1)

        def two_work(task_id, category):
            self.assertEqual(1234, task_id)  # assert the message body is passed in as arg by default
            self.assertEqual("two work", category)

            # The "work" we're doing is just putting 2 on the queue
            queue_in_use = get_sqs_queue()
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
        self.assertEqual(1, len(messages))
        self.assertEqual(1, messages[0].body)

        # Worker process should have a successful (0) exitcode
        self.assertEqual(0, dispatcher._worker_process.exitcode)

    def test_dispatching_by_message_attribute_succeeds_with_job_args_and_job_kwargs_and_additional(self):
        """ SQSWorkDispatcher can read a message attribute to determine which function to call

            - Given a message with a user-defined message attribute
            - When on a SQSWorkDispatcher.dispatch_by_message_attribute() is called
            - And a message_transformer is given to route execution based on that message attribute
            - Then the correct function is executed
            - And it can get its args from the job_args item and keyword arguments for execution from the job_kwargs
                item of the message_transformer's returned dictionary
            - And can append to those additional args and additional kwargs
        """
        queue = get_sqs_queue()
        message_attr = {"work_type": {"DataType": "String", "StringValue": "a"}}
        queue.send_message(MessageBody=1234, MessageAttributes=message_attr)

        dispatcher = SQSWorkDispatcher(
            queue, worker_process_name="Test Worker Process", long_poll_seconds=1, monitor_sleep_time=1
        )

        def one_work(task_id, category, extra1, extra2, kwarg1, xkwarg1, xkwarg2=None, xkwarg3="override_me"):
            self.assertEqual(1234, task_id)  # assert the message body is passed in as arg by default
            self.assertEqual("one work", category)
            self.assertEqual("my_kwarg_1", kwarg1)
            self.assertEqual("my_extra_arg_1", extra1)
            self.assertEqual("my_extra_arg_2", extra2)
            self.assertEqual("my_extra_kwarg_1", xkwarg1)
            self.assertEqual("my_extra_kwarg_2", xkwarg2)
            self.assertEqual("my_extra_kwarg_3", xkwarg3)

            # The "work" we're doing is just putting 1 on the queue
            queue_in_use = get_sqs_queue()
            queue_in_use.send_message(MessageBody=1)

        def two_work(task_id, category, extra1, extra2, kwarg1, xkwarg1, xkwarg2=None, xkwarg3="override_me"):
            self.assertEqual(1234, task_id)  # assert the message body is passed in as arg by default
            self.assertEqual("two work", category)
            self.assertEqual("my_kwarg_1", kwarg1)
            self.assertEqual("my_extra_arg_1", extra1)
            self.assertEqual("my_extra_arg_2", extra2)
            self.assertEqual("my_extra_kwarg_1", xkwarg1)
            self.assertEqual("my_extra_kwarg_2", xkwarg2)
            self.assertEqual("my_extra_kwarg_3", xkwarg3)

            # The "work" we're doing is just putting 2 on the queue
            queue_in_use = get_sqs_queue()
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
        self.assertEqual(1, len(messages))
        self.assertEqual(1, messages[0].body)

        # Worker process should have a successful (0) exitcode
        self.assertEqual(0, dispatcher._worker_process.exitcode)

    def test_faulty_queue_connection_raises_correct_exception(self):
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
            self.assertIsInstance(e, SystemExit)
            self.assertIsNotNone(e.__cause__)
            self.assertTrue(
                isinstance(e.__cause__, EndpointConnectionError)
                or isinstance(e.__cause__, ClientError)
                or isinstance(e.__cause__, NoRegionError)
                or isinstance(e.__cause__, NoCredentialsError)
            )

    def test_failed_job_detected(self):
        """ SQSWorkDispatcher handles failed work within the child process

            - Given a numeric message body
            - When on a SQSWorkDispatcher.dispatch() is called
            - And the function to execute in the child process fails
            - Then a QueueWorkerProcessError exception is raised
            - And the exit code of the worker process is > 0
        """
        with self.assertRaises(QueueWorkerProcessError):
            queue = get_sqs_queue()
            queue.send_message(MessageBody=1234)

            dispatcher = SQSWorkDispatcher(
                queue, worker_process_name="Test Worker Process", long_poll_seconds=1, monitor_sleep_time=1
            )

            def fail_at_work(task_id):
                raise Exception("failing at this particular job...")

            dispatcher.dispatch(fail_at_work)
            dispatcher._worker_process.join(5)  # wait at most 5 sec for the work to complete

        # Worker process should have a failed (> 0)  exitcode
        self.assertGreater(dispatcher._worker_process.exitcode, 0)

    def test_separate_signal_handlers_for_child_process(self):
        """ Demonstrate (via log output) that a forked child process will inherit signal-handling of the parent
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
        self.assertIsNotNone(fired)
        self.assertEqual(os.getpid(), fired[0], "PID of handled signal != this process's PID")
        self.assertEqual(signal.SIGALRM, fired[1], "Signal handled was not signal.SIGALRM ({})".format(signal.SIGALRM))

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

    def test_terminated_job_triggers_exit_signal_handling_with_retry(self):
        """ The child worker process is terminated, and exits indicating the exit signal of the termination. The
            parent monitors this, and initiates exit-handling. Because the dispatcher allows retries, this message
            should be made receivable again on the queue.
        """
        logger = logging.getLogger(__name__ + "." + inspect.stack()[0][3])
        logger.setLevel(logging.DEBUG)

        msg_body = randint(1111, 9998)
        queue = get_sqs_queue()
        queue.send_message(MessageBody=msg_body)

        dispatcher = SQSWorkDispatcher(
            queue, worker_process_name="Test Worker Process", long_poll_seconds=0, monitor_sleep_time=0.05
        )
        dispatcher.sqs_queue_instance.max_receive_count = 2  # allow retries

        wq = mp.Queue()  # work tracking queue
        tq = mp.Queue()  # termination queue

        terminator = mp.Process(
            name="worker_terminator", target=self._worker_terminator, args=(tq, 0.05, logger), daemon=True
        )
        terminator.start()  # start terminator
        # Start dispatcher with work, and with the inter-process Queue so it can pass along its PID
        # Passing its PID on this Queue will let the terminator know the worker to terminate
        dispatcher.dispatch(self._work_to_be_terminated, termination_queue=tq, work_tracking_queue=wq)
        dispatcher._worker_process.join(2)  # wait at most 2 sec for the work to complete
        terminator.join(1)  # ensure terminator completes within 3 seconds. Don't let it run away.

        try:
            # Worker process should have an exitcode less than zero
            self.assertLess(dispatcher._worker_process.exitcode, 0)
            self.assertEqual(dispatcher._worker_process.exitcode, -signal.SIGTERM)
            # Message should NOT have been deleted from the queue, but available for receive again
            msgs = queue.receive_messages(WaitTimeSeconds=0)
            self.assertIsNotNone(msgs)
            self.assertTrue(len(msgs) == 1, "Should be only 1 message received from queue")
            self.assertEqual(msg_body, msgs[0].body, "Should be the same message available for retry on the queue")
            # If the job function was called, it should have put the task_id (msg_body in this case) into the
            # work_tracking_queue as its first queued item
            work = wq.get(True, 1)
            self.assertEqual(msg_body, work, "Was expecting to find worker task_id (msg_body) tracked in the queue")
        finally:
            self._fail_runaway_processes(logger, worker=dispatcher._worker_process, terminator=terminator)

    def test_terminated_job_triggers_exit_signal_handling_to_dlq(self):
        """ The child worker process is terminated, and exits indicating the exit signal of the termination. The
            parent monitors this, and initiates exit-handling. Because the dispatcher does not allow retries, the
            message is copied to teh dead letter queue, and deleted from the queue.
        """
        logger = logging.getLogger(__name__ + "." + inspect.stack()[0][3])
        logger.setLevel(logging.DEBUG)

        msg_body = randint(1111, 9998)
        queue = get_sqs_queue()
        queue.send_message(MessageBody=msg_body)

        dispatcher = SQSWorkDispatcher(
            queue, worker_process_name="Test Worker Process", long_poll_seconds=0, monitor_sleep_time=0.05
        )

        wq = mp.Queue()  # work tracking queue
        tq = mp.Queue()  # termination queue

        terminator = mp.Process(
            name="worker_terminator", target=self._worker_terminator, args=(tq, 0.05, logger), daemon=True
        )
        terminator.start()  # start terminator

        with self.assertRaises(QueueWorkerProcessError) as err_ctx:
            # Start dispatcher with work, and with the inter-process Queue so it can pass along its PID
            # Passing its PID on this Queue will let the terminator know the worker to terminate
            dispatcher.dispatch(self._work_to_be_terminated, termination_queue=tq, work_tracking_queue=wq)

        # Ensure to wait on the processes to end with join
        dispatcher._worker_process.join(2)  # wait at most 2 sec for the work to complete
        terminator.join(1)  # ensure terminator completes within 3 seconds. Don't let it run away.
        self.assertTrue("SIGTERM" in str(err_ctx.exception), "QueueWorkerProcessError did not mention 'SIGTERM'.")

        try:
            # Worker process should have an exitcode less than zero
            self.assertLess(dispatcher._worker_process.exitcode, 0)
            self.assertEqual(dispatcher._worker_process.exitcode, -signal.SIGTERM)
            # Message SHOULD have been deleted from the queue
            msgs = queue.receive_messages(WaitTimeSeconds=0)
            self.assertIsNotNone(msgs)
            self.assertTrue(len(msgs) == 0, "Should be NO messages received from queue")
            # If the job function was called, it should have put the task_id (msg_body in this case) into the
            # work_tracking_queue as its first queued item
            work = wq.get(True, 1)
            self.assertEqual(msg_body, work, "Was expecting to find worker task_id (msg_body) tracked in the queue")
            # TODO: Test that the "dead letter queue" has this message - maybe by asserting the log statement
        finally:
            self._fail_runaway_processes(logger, worker=dispatcher._worker_process, terminator=terminator)

    def test_terminated_parent_dispatcher_exits_with_negative_signal_code(self):
        """ After a parent dispatcher process receives an exit signal, and kills its child worker process, it itself
            exits. Verify that the exit code it exits with is the negative value of the signal received, consistent with
            how Python handles this.
        """
        logger = logging.getLogger(__name__ + "." + inspect.stack()[0][3])
        logger.setLevel(logging.DEBUG)

        msg_body = randint(1111, 9998)
        queue = get_sqs_queue()
        queue.send_message(MessageBody=msg_body)

        dispatcher = SQSWorkDispatcher(
            queue, worker_process_name="Test Worker Process", long_poll_seconds=0, monitor_sleep_time=0.05
        )
        dispatcher.sqs_queue_instance.max_receive_count = 2  # allow retries

        wq = mp.Queue()  # work tracking queue
        tq = mp.Queue()  # termination queue

        dispatch_kwargs = {"job": self._work_to_be_terminated, "termination_queue": tq, "work_tracking_queue": wq}
        parent_dispatcher = mp.Process(target=dispatcher.dispatch, kwargs=dispatch_kwargs)
        parent_dispatcher.start()

        # block until worker is running and ready to be terminated, or fail in 3 seconds
        work_done = wq.get(True, 3)
        worker_pid = tq.get(True, 1)
        worker = ps.Process(worker_pid)

        self.assertEqual(msg_body, work_done)
        os.kill(parent_dispatcher.pid, signal.SIGTERM)

        # simulate join(2). Wait at most 2 sec for work to complete
        ps.wait_procs([worker], timeout=2)
        parent_dispatcher.join(1)  # ensure dispatcher completes within 3 seconds. Don't let it run away.

        try:
            # Parent dispatcher process should have an exit code less than zero
            self.assertLess(parent_dispatcher.exitcode, 0)
            self.assertEqual(parent_dispatcher.exitcode, -signal.SIGTERM)
            # Message should NOT have been deleted from the queue, but available for receive again
            msgs = queue.receive_messages(WaitTimeSeconds=0)
            self.assertIsNotNone(msgs)
            self.assertTrue(len(msgs) == 1, "Should be only 1 message received from queue")
            self.assertEqual(msg_body, msgs[0].body, "Should be the same message available for retry on the queue")
            # If the job function was called, it should have put the task_id (msg_body in this case) into the
            # work_tracking_queue as its first queued item
            self.assertEqual(
                msg_body, work_done, "Was expecting to find worker task_id (msg_body) tracked in the queue"
            )
        finally:
            if worker and worker.is_running():
                logger.warning(
                    "Dispatched worker process with PID {} did not complete in timeout. "
                    "Killing it.".format(worker.pid)
                )
                os.kill(worker.pid, signal.SIGKILL)
                self.fail("Worker did not complete in timeout as expected. Test fails.")
            self._fail_runaway_processes(logger, dispatcher=parent_dispatcher)

    def test_exit_handler_can_receive_queue_message_as_arg(self):
        """ Verify that exit_handlers provided whose signatures allow keyword args can receive the queue message
            as a keyword arg
        """
        logger = logging.getLogger(__name__ + "." + inspect.stack()[0][3])
        logger.setLevel(logging.DEBUG)

        msg_body = randint(1111, 9998)
        queue = get_sqs_queue()
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
            target=self._worker_terminator,
            args=(termination_queue, 0.05, logger),
            daemon=True,
        )
        terminator.start()  # start terminator
        # Start dispatcher with work, and with the inter-process Queue so it can pass along its PID
        # Passing its PID on this Queue will let the terminator know the worker to terminate
        dispatcher.dispatch(
            self._work_to_be_terminated,
            termination_queue=termination_queue,
            work_tracking_queue=work_tracking_queue,
            exit_handler=exit_handler_with_msg,
        )
        dispatcher._worker_process.join(2)  # wait at most 2 sec for the work to complete
        terminator.join(1)  # ensure terminator completes within 3 seconds. Don't let it run away.

        try:
            # Worker process should have an exitcode less than zero
            self.assertLess(dispatcher._worker_process.exitcode, 0)
            self.assertEqual(dispatcher._worker_process.exitcode, -signal.SIGTERM)
            # Message SHOULD have been deleted from the queue
            msgs = queue.receive_messages(WaitTimeSeconds=0)

            # Message should NOT have been deleted from the queue, but available for receive again
            msgs = queue.receive_messages(WaitTimeSeconds=0)
            self.assertIsNotNone(msgs)
            self.assertTrue(len(msgs) == 1, "Should be only 1 message received from queue")
            self.assertEqual(msg_body, msgs[0].body, "Should be the same message available for retry on the queue")
            # If the job function was called, it should have put the task_id (msg_body in this case) into the
            # work_tracking_queue as its first queued item. FIFO queue, so it should be "first out"
            work = work_tracking_queue.get(True, 1)
            self.assertEqual(msg_body, work, "Was expecting to find worker task_id (msg_body) tracked in the queue")
            # If the exit_handler was called, and if it passed along args from the original worker, then it should
            # have put the message body into the "work_tracker_queue" after work was started and exit occurred
            exit_handling_work = work_tracking_queue.get(True, 1)
            self.assertEqual(
                "exit_handling:{}".format(msg_body),
                exit_handling_work,
                "Was expecting to find tracking in the work_tracking_queue of the message being handled "
                "by the exit_handler",
            )
        finally:
            self._fail_runaway_processes(logger, worker=dispatcher._worker_process, terminator=terminator)

    def test_default_to_queue_long_poll_works(self):
        """ Same as testing exit handling when allowing retries, but not setting the long_poll_seoconds value,
            to leave it to the default setting.
        """
        logger = logging.getLogger(__name__ + "." + inspect.stack()[0][3])
        logger.setLevel(logging.DEBUG)

        msg_body = randint(1111, 9998)
        queue = get_sqs_queue()
        queue.send_message(MessageBody=msg_body)

        dispatcher = SQSWorkDispatcher(queue, worker_process_name="Test Worker Process", monitor_sleep_time=0.05)
        dispatcher.sqs_queue_instance.max_receive_count = 2  # allow retries

        wq = mp.Queue()  # work tracking queue
        tq = mp.Queue()  # termination queue

        terminator = mp.Process(
            name="worker_terminator", target=self._worker_terminator, args=(tq, 0.05, logger), daemon=True
        )
        terminator.start()  # start terminator
        # Start dispatcher with work, and with the inter-process Queue so it can pass along its PID
        # Passing its PID on this Queue will let the terminator know the worker to terminate
        dispatcher.dispatch(self._work_to_be_terminated, termination_queue=tq, work_tracking_queue=wq)
        dispatcher._worker_process.join(2)  # wait at most 2 sec for the work to complete
        terminator.join(1)  # ensure terminator completes within 3 seconds. Don't let it run away.

        try:
            # Worker process should have an exitcode less than zero
            self.assertLess(dispatcher._worker_process.exitcode, 0)
            self.assertEqual(dispatcher._worker_process.exitcode, -signal.SIGTERM)
            # Message should NOT have been deleted from the queue, but available for receive again
            msgs = queue.receive_messages(WaitTimeSeconds=0)
            self.assertIsNotNone(msgs)
            self.assertTrue(len(msgs) == 1, "Should be only 1 message received from queue")
            self.assertEqual(msg_body, msgs[0].body, "Should be the same message available for retry on the queue")
            # If the job function was called, it should have put the task_id (msg_body in this case) into the
            # work_tracking_queue as its first queued item
            work = wq.get(True, 1)
            self.assertEqual(msg_body, work, "Was expecting to find worker task_id (msg_body) tracked in the queue")
        finally:
            self._fail_runaway_processes(logger, worker=dispatcher._worker_process, terminator=terminator)

    def test_hanging_cleanup_of_signaled_child_fails_dispatcher_and_sends_to_dlq(self):
        """ When detecting the child worker process received an exit signal, and the parent dispatcher
            process is handling cleanup and termination of the child worker process, if the cleanup hangs for longer
            than the allowed exit_handling_timeout, it will short-circuit, and retry a 2nd time. If that also hangs
            longer than exit_handling_timeout, the message will be put on the dead letter queue, deleted from the
            origin queue, and :exc:`QueueWorkDispatcherError` will be raised stating unable to do exit handling
        """
        logger = logging.getLogger(__name__ + "." + inspect.stack()[0][3])
        logger.setLevel(logging.DEBUG)

        msg_body = randint(1111, 9998)
        queue = get_sqs_queue()
        queue.send_message(MessageBody=msg_body)
        worker_sleep_interval = 0.05  # how long to "work"

        def hanging_cleanup(task_id, termination_queue: mp.Queue, work_tracking_queue: mp.Queue, queue_message):
            work_tracking_queue.put_nowait("cleanup_start_{}".format(queue_message.body))
            sleep(2.5)  # sleep for longer than the allowed time for exit handling
            # Should never get to this point, since it should be short-circuited by exit_handling_timeout
            work_tracking_queue.put_nowait("cleanup_end_{}".format(queue_message.body))

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
            target=self._worker_terminator,
            args=(tq, worker_sleep_interval, logger),
            daemon=True,
        )
        terminator.start()  # start terminator

        with self.assertRaises(QueueWorkDispatcherError) as err_ctx:
            # Start dispatcher with work, and with the inter-process Queue so it can pass along its PID
            # Passing its PID on this Queue will let the terminator know the worker to terminate
            dispatcher.dispatch(
                self._work_to_be_terminated, termination_queue=tq, work_tracking_queue=wq, exit_handler=hanging_cleanup
            )

        # Ensure to wait on the processes to end with join
        dispatcher._worker_process.join(2)  # wait at most 2 sec for the work to complete
        terminator.join(1)  # ensure terminator completes within 3 seconds. Don't let it run away.
        timeout_error_fragment = (
            "Could not perform cleanup during exiting "
            "of job in allotted _exit_handling_timeout ({}s)".format(cleanup_timeout)
        )
        self.assertTrue(
            timeout_error_fragment in str(err_ctx.exception),
            "QueueWorkDispatcherError did not mention exit_handling timeouts",
        )
        self.assertTrue("after 2 tries" in str(err_ctx.exception), "QueueWorkDispatcherError did not mention '2 tries'")

        try:
            # Worker process should have an exitcode less than zero
            self.assertLess(dispatcher._worker_process.exitcode, 0)
            self.assertEqual(dispatcher._worker_process.exitcode, -signal.SIGTERM)
            # Message SHOULD have been deleted from the queue (after going to DLQ)
            msgs = queue.receive_messages(WaitTimeSeconds=0)
            self.assertIsNotNone(msgs)
            self.assertTrue(len(msgs) == 0, "Should be NO messages received from queue")
            # If the job function was called, it should have put the task_id (msg_body in this case) into the
            # work_tracking_queue as its first queued item
            work = wq.get_nowait()
            self.assertEqual(msg_body, work, "Was expecting to find worker task_id (msg_body) tracked in the queue")
            cleanup_attempt_1 = wq.get(True, 1)
            self.assertEqual(
                "cleanup_start_{}".format(msg_body),
                cleanup_attempt_1,
                "Was expecting to find a trace of cleanup attempt 1 tracked in the work queue",
            )
            cleanup_attempt_2 = wq.get(True, 1)
            self.assertEqual(
                "cleanup_start_{}".format(msg_body),
                cleanup_attempt_2,
                "Was expecting to find a trace of cleanup attempt 2 tracked in the work queue",
            )
            self.assertTrue(wq.empty(), "There should be no more work tracked on the work Queue")
            # TODO: Test that the "dead letter queue" has this message - maybe by asserting the log statement
        finally:
            self._fail_runaway_processes(logger, worker=dispatcher._worker_process, terminator=terminator)

    def test_hanging_cleanup_of_signaled_parent_fails_dispatcher_and_sends_to_dlq(self):
        """ When detecting the parent dispatcher process received an exit signal, and the parent dispatcher
            process is handling cleanup and termination of the child worker process, if the cleanup hangs for longer
            than the allowed exit_handling_timeout, it will short-circuit, and retry a 2nd time. If that also hangs
            longer than exit_handling_timeout, the message will be put on the dead letter queue, deleted from the

            Slight differences from :meth:`test_hanging_cleanup_fails_dispatcher_and_sends_to_dlq` is that the child
            worker process is not "dead" when doing these exit_handling cleanups.
        """
        logger = logging.getLogger(__name__ + "." + inspect.stack()[0][3])
        logger.setLevel(logging.DEBUG)

        msg_body = randint(1111, 9998)
        queue = get_sqs_queue()
        queue.send_message(MessageBody=msg_body)
        worker_sleep_interval = 0.05  # how long to "work"

        def hanging_cleanup(task_id, termination_queue: mp.Queue, work_tracking_queue: mp.Queue, queue_message):
            work_tracking_queue.put_nowait("cleanup_start_{}".format(queue_message.body))
            sleep(2.5)  # sleep for longer than the allowed time for exit handling
            # Should never get to this point, since it should be short-circuited by exit_handling_timeout
            work_tracking_queue.put_nowait("cleanup_end_{}".format(queue_message.body))

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
                error_queue.put_nowait(exc)
                raise exc

        dispatch_kwargs = {
            "job": self._work_to_be_terminated,
            "termination_queue": tq,
            "work_tracking_queue": wq,
            "exit_handler": hanging_cleanup,
        }
        parent_dispatcher = mp.Process(target=error_handling_dispatcher, args=(dispatcher, eq), kwargs=dispatch_kwargs)
        parent_dispatcher.start()

        # block until worker is running, or fail in 3 seconds
        work_done = wq.get(True, 3)
        self.assertEqual(msg_body, work_done)

        # block until received object indicating ready to be terminated, or fail in 1 second
        worker_pid = tq.get(True, 1)
        worker = ps.Process(worker_pid)
        os.kill(parent_dispatcher.pid, signal.SIGTERM)

        # simulate join(2). Wait at most 2 sec for work to complete
        ps.wait_procs([worker], timeout=2)
        parent_dispatcher.join(3)  # ensure dispatcher completes within 3 seconds. Don't let it run away.

        self.assertFalse(eq.empty(), "No errors detected in parent dispatcher")
        exc = eq.get_nowait()
        self.assertIsInstance(exc, QueueWorkDispatcherError, "Error was not of type QueueWorkDispatcherError")

        timeout_error_fragment = (
            "Could not perform cleanup during exiting "
            "of job in allotted _exit_handling_timeout ({}s)".format(cleanup_timeout)
        )
        self.assertTrue(
            timeout_error_fragment in str(exc), "QueueWorkDispatcherError did not mention exit_handling timeouts"
        )
        self.assertTrue("after 2 tries" in str(exc), "QueueWorkDispatcherError did not mention '2 tries'")

        try:
            # Parent dispatcher process should have an exit code of 1, since it will have raised an exception and
            # failed, rather than a graceful exit
            self.assertEqual(parent_dispatcher.exitcode, 1)

            # Message SHOULD have been deleted from the queue (after going to DLQ)
            msgs = queue.receive_messages(WaitTimeSeconds=0)
            self.assertIsNotNone(msgs)
            self.assertTrue(len(msgs) == 0, "Should be NO messages received from queue")
            # If the job function was called, it should have put the task_id (msg_body in this case) into the
            # work_tracking_queue as its first queued item
            self.assertEqual(
                msg_body, work_done, "Was expecting to find worker task_id (msg_body) tracked in the queue"
            )
            cleanup_attempt_1 = wq.get(True, 1)
            self.assertEqual(
                "cleanup_start_{}".format(msg_body),
                cleanup_attempt_1,
                "Was expecting to find a trace of cleanup attempt 1 tracked in the work queue",
            )
            cleanup_attempt_2 = wq.get(True, 1)
            self.assertEqual(
                "cleanup_start_{}".format(msg_body),
                cleanup_attempt_2,
                "Was expecting to find a trace of cleanup attempt 2 tracked in the work queue",
            )
            self.assertTrue(wq.empty(), "There should be no more work tracked on the work Queue")
            # TODO: Test that the "dead letter queue" has this message - maybe by asserting the log statement
        finally:
            if worker and worker.is_running():
                logger.warning(
                    "Dispatched worker process with PID {} did not complete in timeout. "
                    "Killing it.".format(worker.pid)
                )
                os.kill(worker.pid, signal.SIGKILL)
                self.fail("Worker did not complete in timeout as expected. Test fails.")
            self._fail_runaway_processes(logger, dispatcher=parent_dispatcher)

    def test_cleanup_second_try_succeeds_after_killing_worker_with_dlq(self):
        """ Simulate an exit_handler that interferes with work that was being done by the worker, to see that we can
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
        queue = get_sqs_queue()
        queue.send_message(MessageBody=msg_body)
        worker_sleep_interval = 0.05  # how long to "work"
        cleanup_timeout = int(worker_sleep_interval + 3)  # how long to allow cleanup to run, max (integer seconds)

        def hanging_cleanup_if_worker_alive(
            task_id, termination_queue: mp.Queue, work_tracking_queue: mp.Queue, queue_message
        ):
            cleanup_logger = logging.getLogger(__name__ + "." + inspect.stack()[0][3])
            cleanup_logger.setLevel(logging.DEBUG)
            cleanup_logger.debug("CLEANUP CLEANUP CLEANUP !!!!!!!!!!!!!!")

            work_tracking_queue.put_nowait("cleanup_start_{}".format(queue_message.body))

            sleep_before_cleanup = 0.25
            cleanup_logger.debug(
                "Sleeping for {} seconds to allow worker process state to stabilize before "
                "inspecting it during this cleanup.".format(sleep_before_cleanup)
            )
            sleep(sleep_before_cleanup)

            # Get the PID of the worker off the termination_queue, then put it back on, as if it wasn't removed
            worker_pid_during_cleanup = termination_queue.get(True, 1)
            termination_queue.put_nowait(worker_pid_during_cleanup)

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
                "worker_during_cleanup.status() = {} [PID={}]".format(
                    worker_during_cleanup.status(), worker_pid_during_cleanup
                )
            )
            if worker_during_cleanup.status() != ps.STATUS_ZOMBIE:  # hang cleanup if worker is running
                sleep(cleanup_timeout + 0.5)  # sleep for longer than the allowed time for exit handling
                # Should not get to this point, since the timeout wrapper should interrupt during the above sleep
                work_tracking_queue.put("cleanup_end_with_live_worker_{}".format(queue_message.body))
            else:
                # Should only get to this point if the worker was dead/killed when this cleanup ran
                work_tracking_queue.put("cleanup_end_with_dead_worker_{}".format(queue_message.body))

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
                error_queue.put_nowait(exc)
                raise exc

        dispatch_kwargs = {
            "job": self._work_to_be_terminated,
            "termination_queue": tq,
            "work_tracking_queue": wq,
            "exit_handler": hanging_cleanup_if_worker_alive,
        }
        parent_dispatcher = mp.Process(target=error_handling_dispatcher, args=(dispatcher, eq), kwargs=dispatch_kwargs)
        parent_dispatcher.start()

        # block until worker is running, or fail in 3 seconds
        work_done = wq.get(True, 3)
        self.assertEqual(msg_body, work_done)

        # block until received object indicating ready to be terminated, or fail in 1 second
        worker_pid = tq.get(True, 1)
        # Put right back on termination queue as if not removed, so exit_handler can discover the worker process PID
        # to perform its conditional hang-or-cleanup logic
        tq.put(worker_pid)
        worker = ps.Process(worker_pid)
        os.kill(parent_dispatcher.pid, signal.SIGTERM)

        # simulate join(2). Wait at most 2 sec for work to complete
        ps.wait_procs([worker], timeout=2)
        parent_dispatcher.join(3)  # ensure dispatcher completes within 3 seconds. Don't let it run away.

        self.assertTrue(eq.empty(), "Errors detected in parent dispatcher")

        try:
            # Parent dispatcher process should have gracefully exited after receiving SIGTERM (exit code of -15)
            self.assertEqual(parent_dispatcher.exitcode, -signal.SIGTERM)

            # Message SHOULD have been deleted from the queue (after going to DLQ)
            msgs = queue.receive_messages(WaitTimeSeconds=0)
            self.assertIsNotNone(msgs)
            self.assertTrue(len(msgs) == 0, "Should be NO messages received from queue")
            # If the job function was called, it should have put the task_id (msg_body in this case) into the
            # work_tracking_queue as its first queued item
            self.assertEqual(
                msg_body, work_done, "Was expecting to find worker task_id (msg_body) tracked in the queue"
            )
            cleanup_attempt_1 = wq.get(True, 1)
            self.assertEqual(
                "cleanup_start_{}".format(msg_body),
                cleanup_attempt_1,
                "Was expecting to find a trace of cleanup attempt 1 tracked in the work queue",
            )
            cleanup_attempt_2 = wq.get(True, 1)
            self.assertEqual(
                "cleanup_start_{}".format(msg_body),
                cleanup_attempt_2,
                "Was expecting to find a trace of cleanup attempt 2 tracked in the work queue",
            )
            cleanup_end = wq.get(True, 1)
            self.assertEqual(
                "cleanup_end_with_dead_worker_{}".format(msg_body),
                cleanup_end,
                "Was expecting to find a trace of cleanup reaching its end, after worker was killed (try 2)",
            )
            self.assertTrue(wq.empty(), "There should be no more work tracked on the work Queue")
            # TODO: Test that the "dead letter queue" has this message - maybe by asserting the log statement
        finally:
            if worker and worker.is_running():
                logger.warning(
                    "Dispatched worker process with PID {} did not complete in timeout. "
                    "Killing it.".format(worker.pid)
                )
                os.kill(worker.pid, signal.SIGKILL)
                self.fail("Worker did not complete in timeout as expected. Test fails.")
            self._fail_runaway_processes(logger, dispatcher=parent_dispatcher)

    def test_cleanup_second_try_succeeds_after_killing_worker_with_retry(self):
        """ Same as :meth:`test_cleanup_second_try_succeeds_after_killing_worker_with_dlq`, but this queue allows
            retries. Changes asserts to ensure the message gets retried after cleanup.
        """
        logger = logging.getLogger(__name__ + "." + inspect.stack()[0][3])
        logger.setLevel(logging.DEBUG)

        msg_body = randint(1111, 9998)
        queue = get_sqs_queue()
        queue.send_message(MessageBody=msg_body)
        worker_sleep_interval = 0.05  # how long to "work"
        cleanup_timeout = int(worker_sleep_interval + 3)  # how long to allow cleanup to run, max (integer seconds)

        def hanging_cleanup_if_worker_alive(
            task_id,
            termination_queue: mp.Queue,
            work_tracking_queue: mp.SimpleQueue,
            queue_message,
            cleanup_timeout=cleanup_timeout,
        ):
            cleanup_logger = logging.getLogger(__name__ + "." + inspect.stack()[0][3])
            cleanup_logger.setLevel(logging.DEBUG)
            cleanup_logger.debug("CLEANUP CLEANUP CLEANUP !!!!!!!!!!!!!!")

            work_tracking_queue.put("cleanup_start_{}".format(queue_message.body))

            sleep_before_cleanup = 0.25
            cleanup_logger.debug(
                "Sleeping for {} seconds to allow worker process state to stabilize before "
                "inspecting it during this cleanup.".format(sleep_before_cleanup)
            )
            sleep(sleep_before_cleanup)

            # Get the PID of the worker off the termination_queue, then put it back on, as if it wasn't removed
            worker_pid_during_cleanup = termination_queue.get(True, 1)
            termination_queue.put_nowait(worker_pid_during_cleanup)

            worker_during_cleanup = ps.Process(worker_pid_during_cleanup)

            # Log what psutil sees of this worker. 'zombie' is what we're looking for if it was killed
            cleanup_logger.debug(
                "psutil.pid_exists({}) = {}".format(worker_pid_during_cleanup, ps.pid_exists(worker_pid_during_cleanup))
            )
            cleanup_logger.debug(
                "worker_during_cleanup.is_running() = {} [PID={}]".format(
                    worker_during_cleanup.is_running(), worker_during_cleanup.pid
                )
            )
            cleanup_logger.debug(
                "worker_during_cleanup.status() = {} [PID={}]".format(
                    worker_during_cleanup.status(), worker_during_cleanup.pid
                )
            )
            if worker_during_cleanup.status() != ps.STATUS_ZOMBIE:  # hang cleanup if worker is running
                sleep(cleanup_timeout + 0.5)  # sleep for longer than the allowed time for exit handling
                # Should not get to this point, since the timeout wrapper should interrupt during the above sleep
                work_tracking_queue.put("cleanup_end_with_live_worker_{}".format(queue_message.body))
            else:
                # Should only get to this point if the worker was dead/killed when this cleanup ran
                work_tracking_queue.put("cleanup_end_with_dead_worker_{}".format(queue_message.body))

        dispatcher = SQSWorkDispatcher(
            queue,
            worker_process_name="Test Worker Process",
            long_poll_seconds=0,
            monitor_sleep_time=0.05,
            exit_handling_timeout=cleanup_timeout,
        )
        dispatcher.sqs_queue_instance.max_receive_count = 2  # allow retries

        wq = mp.SimpleQueue()  # work tracking queue
        tq = mp.Queue()  # termination queue
        eq = mp.Queue()  # error queue

        def error_handling_dispatcher(work_dispatcher: SQSWorkDispatcher, error_queue: mp.Queue, **kwargs):
            try:
                work_dispatcher.dispatch(**kwargs)
            except Exception as exc:
                error_queue.put_nowait(exc)
                raise exc

        dispatch_kwargs = {
            "job": self._work_to_be_terminated,
            "termination_queue": tq,
            "work_tracking_queue": wq,
            "exit_handler": hanging_cleanup_if_worker_alive,
        }
        parent_dispatcher = mp.Process(target=error_handling_dispatcher, args=(dispatcher, eq), kwargs=dispatch_kwargs)
        parent_dispatcher.start()

        # block until worker is running, or fail in 3 seconds
        work_done = wq.get()
        self.assertEqual(msg_body, work_done)

        # block until received object indicating ready to be terminated, or fail in 1 second
        worker_pid = tq.get(True, 1)
        # Put right back on termination queue as if not removed, so exit_handler can discover the worker process PID
        # to perform its conditional hang-or-cleanup logic
        tq.put(worker_pid)
        worker = ps.Process(worker_pid)
        os.kill(parent_dispatcher.pid, signal.SIGTERM)

        # simulate join(2). Wait at most 2 sec for work to complete
        ps.wait_procs([worker], timeout=2)
        parent_dispatcher.join(3)  # ensure dispatcher completes within 3 seconds. Don't let it run away.

        self.assertTrue(eq.empty(), "Errors detected in parent dispatcher")

        try:
            # Parent dispatcher process should have gracefully exited after receiving SIGTERM (exit code of -15)
            self.assertEqual(parent_dispatcher.exitcode, -signal.SIGTERM)

            # Message should NOT have been deleted from the queue, but available for receive again
            msgs = queue.receive_messages(WaitTimeSeconds=0)
            self.assertIsNotNone(msgs)
            self.assertTrue(len(msgs) == 1, "Should be only 1 message received from queue")
            self.assertEqual(msg_body, msgs[0].body, "Should be the same message available for retry on the queue")
            # If the job function was called, it should have put the task_id (msg_body in this case) into the
            # work_tracking_queue as its first queued item
            self.assertEqual(
                msg_body, work_done, "Was expecting to find worker task_id (msg_body) tracked in the queue"
            )

            # If the job function was called, it should have put the task_id (msg_body in this case) into the
            # work_tracking_queue as its first queued item
            self.assertEqual(
                msg_body, work_done, "Was expecting to find worker task_id (msg_body) tracked in the queue"
            )
            cleanup_attempt_1 = wq.get()
            self.assertEqual(
                "cleanup_start_{}".format(msg_body),
                cleanup_attempt_1,
                "Was expecting to find a trace of cleanup attempt 1 tracked in the work queue",
            )
            cleanup_attempt_2 = wq.get()
            self.assertEqual(
                "cleanup_start_{}".format(msg_body),
                cleanup_attempt_2,
                "Was expecting to find a trace of cleanup attempt 2 tracked in the work queue",
            )
            cleanup_end = wq.get()
            self.assertEqual(
                "cleanup_end_with_dead_worker_{}".format(msg_body),
                cleanup_end,
                "Was expecting to find a trace of cleanup reaching its end, after worker was killed (try 2)",
            )
            self.assertTrue(wq.empty(), "There should be no more work tracked on the work Queue")
        finally:
            if worker and worker.is_running():
                logger.warning(
                    "Dispatched worker process with PID {} did not complete in timeout. "
                    "Killing it.".format(worker.pid)
                )
                os.kill(worker.pid, signal.SIGKILL)
                self.fail("Worker did not complete in timeout as expected. Test fails.")
            self._fail_runaway_processes(logger, dispatcher=parent_dispatcher)

    def test_worker_process_error_exception_data(self):
        unknown_worker = "Unknown Worker"
        no_queue_message = "Queue Message None or not provided"
        queue = get_sqs_queue()
        fake_queue_message = FakeSQSMessage(queue.url)

        def raise_exc_no_args():
            raise QueueWorkerProcessError()

        with self.assertRaises(QueueWorkerProcessError) as err_ctx1:
            raise_exc_no_args()
        self.assertTrue(
            (unknown_worker in str(err_ctx1.exception)), f"QueueWorkerProcessError did not mention '{unknown_worker}'."
        )
        self.assertTrue(
            ("Queue Message None or not provided" in str(err_ctx1.exception)),
            f"QueueWorkerProcessError did not mention '{no_queue_message}'.",
        )
        self.assertEqual(err_ctx1.exception.worker_process_name, unknown_worker)
        self.assertIsNone(err_ctx1.exception.queue_message)

        def raise_exc_with_message():
            raise QueueWorkerProcessError("A special message")

        with self.assertRaises(QueueWorkerProcessError) as err_ctx2:
            raise_exc_with_message()
        self.assertTrue(
            ("A special message" in str(err_ctx2.exception)),
            "QueueWorkerProcessError did not mention 'A special message'.",
        )
        self.assertEqual(err_ctx2.exception.worker_process_name, unknown_worker)
        self.assertIsNone(err_ctx2.exception.queue_message)

        def raise_exc_with_qmsg():
            fake_queue_message.body = 1133
            raise QueueWorkerProcessError(queue_message=fake_queue_message)

        with self.assertRaises(QueueWorkerProcessError) as err_ctx3:
            raise_exc_with_qmsg()
        self.assertEqual(err_ctx3.exception.queue_message.body, 1133)
        self.assertEqual(err_ctx3.exception.worker_process_name, unknown_worker)
        self.assertTrue(("1133" in str(err_ctx3.exception)), "QueueWorkerProcessError did not mention '1133'.")
        self.assertTrue(
            (unknown_worker in str(err_ctx3.exception)), f"QueueWorkerProcessError did not mention '{unknown_worker}'."
        )

        def raise_exc_with_qmsg_and_message():
            fake_queue_message.body = 1144
            raise QueueWorkerProcessError("Custom Message about THIS", queue_message=fake_queue_message)

        with self.assertRaises(QueueWorkerProcessError) as err_ctx4:
            raise_exc_with_qmsg_and_message()
        self.assertTrue(
            ("Custom Message about THIS" in str(err_ctx4.exception)),
            "QueueWorkerProcessError did not mention 'Custom Message about THIS'.",
        )
        self.assertFalse(
            ("1144" in str(err_ctx4.exception)),
            "QueueWorkerProcessError seems to be using the default message, not the given message",
        )
        self.assertEqual(err_ctx4.exception.queue_message.body, 1144)
        self.assertEqual(err_ctx4.exception.worker_process_name, unknown_worker)

        def raise_exc_with_worker_process_name_and_message():
            raise QueueWorkerProcessError("Custom Message about THIS", worker_process_name="MyJob")

        with self.assertRaises(QueueWorkerProcessError) as err_ctx5:
            raise_exc_with_worker_process_name_and_message()
        self.assertTrue(
            ("Custom Message about THIS" in str(err_ctx5.exception)),
            "QueueWorkerProcessError did not mention 'Custom Message about THIS'.",
        )
        self.assertFalse(
            ("MyJob" in str(err_ctx5.exception)),
            "QueueWorkerProcessError seems to be using the default message, not the given message",
        )
        self.assertIsNone(err_ctx5.exception.queue_message)
        self.assertEqual(err_ctx5.exception.worker_process_name, "MyJob")

        def raise_exc_with_qmsg_and_worker_process_name_and_message():
            fake_queue_message.body = 1166
            raise QueueWorkerProcessError(
                "Custom Message about THIS", worker_process_name="MyJob", queue_message=fake_queue_message,
            )

        with self.assertRaises(QueueWorkerProcessError) as err_ctx6:
            raise_exc_with_qmsg_and_worker_process_name_and_message()
        self.assertTrue(
            ("Custom Message about THIS" in str(err_ctx6.exception)),
            "QueueWorkerProcessError did not mention 'Custom Message about THIS'.",
        )
        self.assertFalse(
            ("MyJob" in str(err_ctx6.exception)),
            "QueueWorkerProcessError seems to be using the default message, not the given message",
        )
        self.assertFalse(
            ("1166" in str(err_ctx6.exception)),
            "QueueWorkerProcessError seems to be using the default message, not the given message",
        )
        self.assertEqual(err_ctx6.exception.queue_message.body, 1166)
        self.assertEqual(err_ctx6.exception.worker_process_name, "MyJob")

        def raise_exc_with_job_id_and_worker_process_name_and_no_message():
            fake_queue_message.body = 1177
            raise QueueWorkerProcessError(worker_process_name="MyJob", queue_message=fake_queue_message)

        with self.assertRaises(QueueWorkerProcessError) as err_ctx7:
            raise_exc_with_job_id_and_worker_process_name_and_no_message()
        self.assertTrue(
            ("MyJob" in str(err_ctx7.exception)),
            "QueueWorkerProcessError seems to be using the default message, not the given message",
        )
        self.assertTrue(
            ("1177" in str(err_ctx7.exception)),
            "QueueWorkerProcessError seems to be using the default message, not the given message",
        )
        self.assertEqual(err_ctx7.exception.queue_message.body, 1177)
        self.assertEqual(err_ctx7.exception.worker_process_name, "MyJob")

    @classmethod
    def _worker_terminator(cls, terminate_queue: mp.Queue, sleep_interval=0, logger=logging.getLogger(__name__)):
        logger.debug("Started worker_terminator. Waiting for the Queue to surface a PID to be terminated.")
        # Wait until there is a worker in the given dispatcher to kill
        pid = None
        while not pid:
            logger.debug("No work yet to be terminated. Waiting {} seconds".format(sleep_interval))
            sleep(sleep_interval)
            pid = terminate_queue.get()

        # Process is running. Now terminate it with signal.SIGTERM
        logger.debug("Found work to be terminated: Worker PID=[{}]".format(pid))
        os.kill(pid, signal.SIGTERM)
        logger.debug("Terminated worker with PID=[{}] using signal.SIGTERM".format(pid))

    @classmethod
    def _work_to_be_terminated(
        cls, task_id, termination_queue: mp.Queue, work_tracking_queue: mp.Queue, *args, **kwargs
    ):
        # Put task_id in the work_tracking_queue so we know we saw this work
        work_tracking_queue.put(task_id)
        # Put PID of worker process in the termination_queue to let worker_terminator proc know what to kill
        termination_queue.put(os.getpid())
        sleep(0.25)  # hang for a short period to ensure terminator has time to kill this

    def _fail_runaway_processes(
        self, logger, worker: mp.Process = None, terminator: mp.Process = None, dispatcher: mp.Process = None
    ):
        fail_with_runaway_proc = False
        if worker and worker.is_alive():
            logger.warning(
                "Dispatched worker process with PID {} did not complete in timeout. Killing it.".format(worker.pid)
            )
            os.kill(worker.pid, signal.SIGKILL)
            fail_with_runaway_proc = True
        if terminator and terminator.is_alive():
            logger.warning(
                "Terminator worker process with PID {} did not complete in timeout. "
                "Killing it.".format(terminator.pid)
            )
            os.kill(terminator.pid, signal.SIGKILL)
            fail_with_runaway_proc = True
        if dispatcher and dispatcher.is_alive():
            logger.warning(
                "Parent dispatcher process with PID {} did not complete in timeout. "
                "Killing it.".format(dispatcher.pid)
            )
            os.kill(dispatcher.pid, signal.SIGKILL)
            fail_with_runaway_proc = True
        if fail_with_runaway_proc:
            self.fail("Worker or its Terminator or the Dispatcher did not complete in timeout as expected. Test fails.")

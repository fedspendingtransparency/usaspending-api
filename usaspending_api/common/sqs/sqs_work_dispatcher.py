import logging
import inspect
import json
import os

import psutil as ps
import signal
import time
import multiprocessing as mp

from botocore.exceptions import ClientError, EndpointConnectionError, NoCredentialsError, NoRegionError

from usaspending_api.common.sqs.queue_exceptions import (
    QueueWorkDispatcherError,
    QueueWorkerProcessError,
    ExecutionTimeout,
)
from usaspending_api.common.sqs.sqs_handler import get_sqs_queue
from usaspending_api.common.sqs.sqs_job_logging import log_dispatcher_message

# Not a complete list of signals
# NOTE: 1-15 are relatively standard on unix platforms; > 15 can change from platform to platform
BSD_SIGNALS = {
    1: "SIGHUP [1] (Hangup detected on controlling terminal or death of controlling process)",
    2: "SIGINT [2] (Interrupt from keyboard)",
    3: "SIGQUIT [3] (Quit from keyboard)",
    4: "SIGILL [4] (Illegal Instruction)",
    6: "SIGABRT [6] (Abort signal from abort(3))",
    8: "SIGFPE [8] (Floating point exception)",
    9: "SIGKILL [9] (Non-catchable, non-ignorable kill)",
    11: "SIGSEGV [11] (Invalid memory reference)",
    14: "SIGALRM [14] (Timer signal from alarm(2))",
    15: "SIGTERM [15] (Software termination signal)",
    19: "SIGSTOP [19] (Suspend process execution)",  # NOTE: 17 on Mac OSX, 19 on RHEL
    20: "SIGTSTP [20] (Interrupt from keyboard to suspend (CTRL-Z)",  # NOTE: 18 on Mac OSX, 20 on RHEL
}


class SQSWorkDispatcher:
    """SQSWorkDispatcher object that is used to pull work from an SQS queue, and then dispatch it to be
    executed on a child worker process.

    This has the benefit of the worker process terminating after completion of the work, which will clear all
    resources used during its execution, and return memory to the operating system. It also allows for periodically
    monitoring the progress of the worker process from the parent process, and extending the SQS
    VisibilityTimeout, so that the message is not prematurely terminated and/or moved to the Dead Letter Queue

    Attributes:
        EXIT_SIGNALS (List[int]): Delineates each of the signals we want to handle, which represent a case where
            the work being performed is prematurely halting, and the process will exit



    """

    EXIT_SIGNALS = [signal.SIGHUP, signal.SIGABRT, signal.SIGINT, signal.SIGQUIT, signal.SIGTERM]
    # NOTE: We are not handling signal.SIGSTOP or signal.SIGTSTP because those are suspensions.
    # There may be valid cases to suspend and then later resume the process.
    # It is even done here via multiprocessing.Process.suspend() to suspend during cleanup.
    # So we do NOT want to handle that

    def __init__(
        self,
        sqs_queue_instance,
        worker_process_name=None,
        default_visibility_timeout=60,
        long_poll_seconds=None,
        monitor_sleep_time=5,
        exit_handling_timeout=30,
        worker_can_start_child_processes=False,
    ):
        """
        Args:
            sqs_queue_instance (SQS.Queue): the SQS queue to get work from
            worker_process_name (str): the name to give to the worker process. It will use the name of the callable
                job to be executed if not provided
            default_visibility_timeout (int): how long until the message is made visible in the queue again,
               for other consumers. If it only allows 1 retry, this may end up directing it to the Dead Letter queue
               when the next consumer attempts to receive it.
            long_poll_seconds (int): if it should wait patiently for some time to receive a message when requesting.
               Defaults to None, which means it should honor the value configured on the SQS queue
            monitor_sleep_time (float): periodicity to check up on the status of the worker process
            exit_handling_timeout (int): expected window of time during which cleanup should complete (not
                guaranteed). This for example would be the time to finish cleanup before the messages is re-queued
                or DLQ'd
            worker_can_start_child_processes (bool): Controls the multiprocessing.Process daemon attribute. When
                ``daemon=True``, it would ensure that if the parent dispatcher dies, it will kill its child
                worker. However, from the docs "a daemonic process is not allowed to create child processes",
                because termination is not cascaded, so a terminated parent killing a direct child would leave
                orphaned grand-children. So if the worker process needs to start its own child processes in order to
                do its work, set this to ``True``, so that ``Process.daemon`` will be set to ``False``,
                allowing the creation of grandchild processes from the dispatcher's child worker process.
        """
        self._logger = logging.getLogger(__name__ + "." + self.__class__.__name__)
        self.sqs_queue_instance = sqs_queue_instance
        self.worker_process_name = worker_process_name
        self._default_visibility_timeout = default_visibility_timeout
        self._monitor_sleep_time = monitor_sleep_time
        self._exit_handling_timeout = exit_handling_timeout
        self._worker_can_start_child_processes = worker_can_start_child_processes
        self._current_sqs_message = None
        self._worker_process = None
        self._job_args = ()
        self._job_kwargs = {}
        self._exit_handler = None
        self._parent_dispatcher_pid = os.getppid()
        self._sqs_heartbeat_log_period_seconds = 15  # log the heartbeat extension of visibility at most this often
        self._long_poll_seconds = 0  # if nothing is set anywhere, it defaults to 0 (short-polling)

        if long_poll_seconds:
            self._long_poll_seconds = long_poll_seconds
        else:
            receive_message_wait_time_seconds = self.sqs_queue_instance.attributes.get("ReceiveMessageWaitTimeSeconds")
            if receive_message_wait_time_seconds:
                self._long_poll_seconds = int(receive_message_wait_time_seconds)

        # Flag used to prevent handling an exit signal more than once, if multiple signals come in succession
        # True when the parent dispatcher process is handling one of the signals in EXIT_SIGNALS, otherwise False
        self._handling_exit_signal = False
        # Flagged True when exit handling should lead to an exit of the parent dispatcher process
        self._dispatcher_exiting = False

        if self._monitor_sleep_time >= self._default_visibility_timeout:
            msg = (
                "_monitor_sleep_time must be less than _default_visibility_timeout. "
                "Otherwise job duplication can occur"
            )
            raise QueueWorkDispatcherError(
                msg, worker_process_name=self.worker_process_name, queue_message=self._current_sqs_message
            )

        # Map handler functions for each of the exit signals we want to handle on the parent dispatcher process
        for sig in self.EXIT_SIGNALS:
            signal.signal(sig, self._handle_exit_signal)

    @property
    def is_exiting(self):
        """bool: True when this parent dispatcher process has received a signal that will lead to the process
        exiting
        """
        return self._dispatcher_exiting

    @property
    def allow_retries(self):
        """bool: Determine from the provided queue if retries of messages should be performed

        If False, the message will not be returned to the queue for retries by other consumers
        """
        redrive_policy = self.sqs_queue_instance.attributes.get("RedrivePolicy")
        if not redrive_policy:
            return True  # Messages in queues without a redrive policy basically have endless retries
        redrive_json = json.loads(redrive_policy)
        retries = redrive_json.get("maxReceiveCount")
        return retries and retries > 1

    @property
    def message_try_attempt(self):
        """int: Give an approximation of which attempt this is to process this message

        Will return None if the message has not been received yet or if the receive count is not being tracked
        This can be used to determine if this is a retry of this message (if return value > 1)
        """
        if not self._current_sqs_message:
            return None
        # get the non-user-defined (queue-defined) attributes on the message
        q_msg_attr = self._current_sqs_message.attributes
        if q_msg_attr.get("ApproximateReceiveCount") is None:
            return None
        else:
            return int(q_msg_attr.get("ApproximateReceiveCount"))

    def _dispatch(self, job, worker_process_name=None, exit_handler=None, *job_args, **job_kwargs):
        """Dispatch work to be performed in a newly started worker process.

        Execute the callable job that will run in a separate child worker process. The worker process
        will be monitored so that heartbeats can be sent to SQS to allow it to keep going.

        Args:
            job (Callable): Callable to use as the target of a new child process
            worker_process_name (str): Name given to the newly created child process. If not already set,
                defaults to the name of the provided job callable
            exit_handler: a callable to be called when handling an :attr:`EXIT_SIGNALS` signal, giving the
                opportunity to perform cleanup before the process exits. When invoked, it gets the same args passed
                to it that were passed to the :param:`job` callable.
            job_args: Zero or many variadic args that are unnamed (not keyword) that can be
                passed to the callable job. NOTE: Passing args to the job this
                way can be done when needed, but it is preferred to use keyword-args through :param:`job_kwargs`
                to be more explicit what arg values are going to what params.
            job_kwargs: Zero or many variadic keyword-args that can be passed to the callable job

        Returns:
            bool: True if a message was found on the queue and dispatched to completion, without error. Otherwise it
                is an error condition and will raise an exception.

        Raises:
            AttributeError: If this is called before setting self._current_sqs_message
            Others: Exceptions raised by :meth:`_monitor_work_progress`
        """
        if self._current_sqs_message is None:
            raise AttributeError("Cannot dispatch work when there is no current message in self._current_sqs_message")
        self.worker_process_name = worker_process_name or self.worker_process_name or job.__name__
        log_dispatcher_message(
            self,
            message="Creating and starting worker process named [{}] to invoke callable [{}] "
            "with args={} and kwargs={}".format(self.worker_process_name, job, job_args, job_kwargs),
        )

        # Set the exit_handler function if provided, or reset to None
        # Also save the job_args, and job_kwargs, which will be passed to the exit_handler
        self._exit_handler = exit_handler
        self._job_args = job_args
        self._job_kwargs = job_kwargs

        # Use the 'fork' method to create a new child process.
        # This shares the same python interpreter and memory space and references as the parent process
        # A side-effect of that is that it inherits the signal-handlers of the parent process. So wrap the job to be
        # executed with some before-advice that resets the signal-handlers to python-defaults within the child process
        ctx = mp.get_context("fork")

        def signal_reset_wrapper(func, args, kwargs):
            # Reset signal handling to defaults in process that calls this
            for sig in self.EXIT_SIGNALS:
                signal.signal(sig, signal.SIG_DFL)
            # Then execute the given function with the given args
            func(*args, **kwargs)

        self._worker_process = ctx.Process(
            name=self.worker_process_name,
            target=signal_reset_wrapper,
            args=(job, job_args, job_kwargs),
            daemon=not self._worker_can_start_child_processes,
        )
        self._worker_process.start()
        log_dispatcher_message(
            self,
            message="Worker process named [{}] started with process ID [{}]".format(
                self.worker_process_name, self._worker_process.pid
            ),
            is_debug=True,
        )
        self._monitor_work_progress()
        return True

    def dispatch(
        self,
        job,
        *additional_job_args,
        message_transformer=lambda msg: msg.body,
        worker_process_name=None,
        exit_handler=None,
        **additional_job_kwargs,
    ):
        """Get work from the queue and dispatch it in a newly started worker process.

        Poll the queue for a message to work on. Any additional (not named in the signature) args or additional
        (not named in the signature) keyword-args will be combined with the args/kwargs provided by the
        message_transformer. This complete set of arguments is provided to the callable :param:`job` that will be
        executed in a separate worker process. The worker process will be monitored so that heartbeats can be
        sent to the SQS to allow it to keep going.

        Args:
            job (Callable[]): the callable to execute as work for the job
            additional_job_args: Zero or many variadic args that are unnamed (not keyword) that may be
                passed along with those from the message to the callable job. NOTE: Passing args to the job this
                way can be done when needed, but it is preferred to use keyword-args through
                :param:`additional_job_kwargs` to be more explicit what arg values are going to what params.
            message_transformer: A lambda to extract arguments to be passed to the callable from the queue message.
                By default, a function that just returns the message body is used. A provided transformer can
                return a single object as its arg, a tuple of args, or a dictionary of named args (which will
                be used as keyword-args in the job invocation). This is a keyword-only argument that can only be
                passed with its name: ``message_transformer=xxx``
            worker_process_name: Name given to the newly created child process. If not already set, defaults to
                the name of the provided job callable. This is a keyword-only argument that can only be
                passed with its name: ``worker_process_name=abc``
            exit_handler: a callable to be called when handling an :attr:`EXIT_SIGNALS` signal, giving the
                opportunity to perform cleanup before the process exits. When invoked, it gets the same args passed
                to it that were passed to the :param:`job` callable.
            additional_job_kwargs: Zero or many variadic keyword-args that may be passed along with those from
                the message to the callable job

        Examples:
            Given job function::

                def my_job(a, b, c, d):
                    pass  # do something

            The preferred way to dispatch it is with a message_transformer that returns a dictionary and,
            if necessary, additional args passed as keyword args::

                dispatcher.dispatch(my_job,
                                    message_transformer=lambda msg: {"a": msg.body, "b": db.get_org(msg.body)},
                                    c=some_tracking_id,
                                    d=datetime.datetime.now())

        Returns:
            bool: True if a message was found on the queue and dispatched, otherwise False if nothing on the queue

        Raises:
            SystemExit(1): If it can't connect to the queue or receive messages
            QueueWorkerProcessError: Under various conditions where the child worker process fails to run the job
            QueueWorkDispatcherError: Under various conditions where the parent dispatcher process can't
                orchestrate work execution, monitoring, or exit-signal-handling
        """
        self._dequeue_message(self._long_poll_seconds)
        if self._current_sqs_message is None:
            return False

        job_args = ()
        job_kwargs = additional_job_kwargs  # important that this be set as default, in case msg_args is not also a dict
        msg_args = message_transformer(self._current_sqs_message)
        if isinstance(msg_args, list) or isinstance(msg_args, tuple):
            job_args = tuple(msg_args) + additional_job_args
        elif isinstance(msg_args, dict):
            if msg_args.keys() & additional_job_kwargs.keys():
                raise KeyError(
                    "message_transformer produces named keyword args that are duplicative of those in "
                    "additional_job_kwargs and would be overwritten"
                )
            job_kwargs = {**msg_args, **additional_job_kwargs}
        else:
            job_args = (msg_args,) + additional_job_args

        return self._dispatch(job, worker_process_name, exit_handler, *job_args, **job_kwargs)

    def dispatch_by_message_attribute(
        self, message_transformer, *additional_job_args, worker_process_name=None, **additional_job_kwargs
    ):
        """Use a provided function to derive the callable job and its arguments from attributes within the queue
        message

        Args:
            message_transformer (Callable[[SQS.Message], dict]): A callable function that takes in the SQS
            message as its argument and returns a dict of::

                {
                    '_job': Callable,          # Required. The job to run
                    '_exit_handler': Callable  # Optional. A callable to be called when handling an
                                                    :attr:`EXIT_SIGNALS` signal, giving the opportunity to
                                                    perform cleanup before the process exits. Gets the ``job_args``
                                                    and any other items in this dict as args passed to it when run.
                    '_job_args': tuple,        # Optional. Partial or full collection of job args as a list or
                                                    tuple.
                    'named_job_arg1': Any,     # Optional. A named argument to be used as a keyword arg when
                                                    calling the job. ``named_job_arg1`` is representative, and the
                                                    actual names of the ``_job``'s params should be used here.
                                                    This is the preferred way to pass args to the ``_job``
                    'named_job_argN: Any       # Optional: Same as above. As many as are needed.
                }

            worker_process_name (str): Name given to the newly created child process. If not already set, defaults
                to the name of the provided job Callable
            additional_job_args: Zero or many variadic args that are unnamed (not keyword) that may be
                passed along with those from the message to the callable job. NOTE: Passing args to the job this
                way can be done when needed, but it is preferred to use keyword-args through
                :param:`additional_job_kwargs` to be more explicit what arg values are going to what params.
            additional_job_kwargs: Zero or many variadic keyword-args that may be passed along with those from
                the message to the callable job

        Examples:
            Given job function::

                def my_job(a, b, c, d):
                    pass  # do something

            The preferred way to dispatch it is with a ``message_transformer`` that returns a dictionary and,
            if necessary, additional args passed as keyword args::

                def work_routing_message_transformer(msg):
                    job = job_strategy_factory.from(msg.message_attributes["origin"]["StringValue"])
                    return {"_job": job,
                            "a": msg.body,
                            "b": db.get_org(msg.body)}

                dispatcher.dispatch_by_message_attribute(work_routing_message_transformer,
                                                         c=some_tracking_id,
                                                         d=datetime.datetime.now())

        Returns:
            bool: True if a message was found on the queue and dispatched, otherwise False if nothing on the queue

        Raises:
            SystemExit(1): If it can't connect to the queue or receive messages
            QueueWorkerProcessError: Under various conditions where the child worker process fails to run the job
            QueueWorkDispatcherError: Under various conditions where the parent dispatcher process can't
                orchestrate work execution, monitoring, or exit-signal-handling
        """
        self._dequeue_message(self._long_poll_seconds)
        if self._current_sqs_message is None:
            return False

        job_args = ()
        job_kwargs = {}
        results = message_transformer(self._current_sqs_message)

        def parse_message_transformer_results(_job, _exit_handler=None, _job_args=(), **_job_kwargs):
            """Use to map dictionary items to this functions params, and packs up remaining items in a separate
            dictionary. Then returns the organized data as a 4-tuple
            """
            return _job, _exit_handler, _job_args, _job_kwargs

        # Parse the result components from the returned dictionary
        job, exit_handler, msg_args, msg_kwargs = parse_message_transformer_results(**results)

        if isinstance(msg_args, list) or isinstance(msg_args, tuple):
            job_args = tuple(msg_args) + additional_job_args
        else:
            job_args = (msg_args,) + additional_job_args  # single element

        if msg_kwargs.keys() & additional_job_kwargs.keys():
            raise KeyError(
                "message_transformer produces named keyword args that are duplicative of those in "
                "additional_job_kwargs and would be overwritten"
            )
        job_kwargs = {**msg_kwargs, **additional_job_kwargs}

        return self._dispatch(job, worker_process_name, exit_handler, *job_args, **job_kwargs)

    def _dequeue_message(self, wait_time):
        """Attempt to get a single message from the queue.

        It will set this message in the `self._current_sqs_message` field if received, otherwise it leaves that None

        Args:
            wait_time: If no message is readily available, wait for this many seconds for one to arrive before
                returning
        """
        try:
            # NOTE: Forcing MaxNumberOfMessages=1
            # This will pull at most 1 message off the queue, or no messages. This dispatcher is built to dispatch
            # one queue message at a time for work, such that one job at a time is handled by the child worker
            # process. The best way to then scale up work-throughput is to have multiple consumers (e.g. multiple
            # parent dispatcher processes on one machine, or multiple machines each running a parent dispatcher process)
            # This may not be an ideal configuration for jobs that may expect to complete with sub-second performance,
            # and handle a massive amount of message-throughput (e.g. 10+ messages/second, or 1M+ messages/day),
            # where the added latency of connecting to the queue to fetch each message could add up.
            received_messages = self.sqs_queue_instance.receive_messages(
                WaitTimeSeconds=wait_time,
                AttributeNames=["All"],
                MessageAttributeNames=["All"],
                VisibilityTimeout=self._default_visibility_timeout,
                MaxNumberOfMessages=1,
            )
        except (EndpointConnectionError, ClientError, NoCredentialsError, NoRegionError) as conn_exc:
            log_dispatcher_message(
                self, message="SQS connection issue. See Traceback and investigate settings", is_exception=True
            )
            raise SystemExit(1) from conn_exc
        except Exception as exc:
            log_dispatcher_message(
                self,
                message="Unknown error occurred when attempting to receive messages from the queue. See Traceback",
                is_exception=True,
            )
            raise SystemExit(1) from exc

        if received_messages:
            self._current_sqs_message = received_messages[0]
            log_dispatcher_message(self, message="Message received: {}".format(self._current_sqs_message.body))

    def delete_message_from_queue(self):
        """Deletes the message from SQS. This is usually treated as a *successful* culmination of message handling,
        so long as it was not previously copied to the Dead Letter Queue before deletion

        Raises:
            QueueWorkDispatcherError: If for some reason the boto3 delete() operation caused an error
        """
        if self._current_sqs_message is None:
            log_dispatcher_message(
                self,
                message="Message to delete does not exist. "
                "Message might have previously been moved, released, or deleted",
                is_warning=True,
            )
            return
        try:
            self._current_sqs_message.delete()
            self._current_sqs_message = None
        except Exception as exc:
            message = (
                "Unable to delete SQS message from queue. "
                "Message might have previously been deleted upon completion or failure"
            )
            log_dispatcher_message(self, message=message, is_exception=True)
            raise QueueWorkDispatcherError(
                message, worker_process_name=self.worker_process_name, queue_message=self._current_sqs_message
            ) from exc

    def surrender_message_to_other_consumers(self, delay=0):
        """Return the message back into its original queue for other consumers to process it.

        Does this by making it immediately visible (it was always there, just invisible).

        See Also:
            https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html#terminating-message-visibility-timeout  # noqa

        Args:
            delay (int): How long to wait in seconds before message becomes visible to other consumers. Default
            is 0 seconds (immediately consumable)
        """
        # Protect against the scenario where one of many consumers is in the midst of a long-poll when an exit signal
        # is received. If they are in this position, and the message is returned to the queue, they will dequeue it
        # (receive it) when they should not have, because they should have been responding to the signal.
        # They will respond to the signal, but not until after they return from receiving the message, and the message
        # will be lost.
        # So, so only make the message visible after any long-poll has returned
        if self._long_poll_seconds and self._long_poll_seconds > 0:
            delay = self._long_poll_seconds + 1

        if self._current_sqs_message:
            retried = 0
            if self._current_sqs_message.attributes.get("ApproximateReceiveCount") is not None:
                retried = int(self._current_sqs_message.attributes.get("ApproximateReceiveCount"))
            log_dispatcher_message(
                self,
                message="Returning message to its queue for retry number {} in {} seconds. "
                "If this exceeds the configured number of retries in the queue, it "
                "will be moved to its Dead Letter Queue".format(retried + 1, delay),
            )

        self._set_message_visibility(delay)

    def move_message_to_dead_letter_queue(self):
        """Move the current message to the Dead Letter Queue.

        Moves the message to the Dead Letter Queue associated with this worker's SQS queue via its RedrivePolicy.
        Then delete the message from its originating queue.

        Raises:
            QueueWorkDispatcherError: Raise an error if there is no RedrivePolicy or a Dead Letter Queue is not
                configured, specified, or connectible for this queue.
        """
        if self._current_sqs_message is None:
            log_dispatcher_message(
                self,
                message="Unable to move SQS message to the dead letter queue. No current message exists. "
                "Message might have previously been moved, released, or deleted",
                is_warning=True,
            )
            return

        dlq_name = "not discovered yet"
        try:
            redrive_policy = self.sqs_queue_instance.attributes.get("RedrivePolicy")
            if not redrive_policy:
                error = (
                    'Failed to move message to dead letter queue. Cannot get RedrivePolicy for SQS queue "{}". '
                    "It was not set, or was not included as an attribute to be "
                    "retrieved with this queue.".format(self.sqs_queue_instance)
                )
                log_dispatcher_message(self, message=error, is_error=True)
                raise QueueWorkDispatcherError(
                    error, worker_process_name=self.worker_process_name, queue_message=self._current_sqs_message
                )

            redrive_json = json.loads(redrive_policy)
            dlq_arn = redrive_json.get("deadLetterTargetArn")
            if not dlq_arn:
                error = (
                    "Failed to move message to dead letter queue. "
                    "Cannot find a dead letter queue in the RedrivePolicy "
                    'for SQS queue "{}"'.format(self.sqs_queue_instance)
                )
                log_dispatcher_message(self, message=error, is_error=True)
                raise QueueWorkDispatcherError(
                    error, worker_process_name=self.worker_process_name, queue_message=self._current_sqs_message
                )
            dlq_name = dlq_arn.split(":")[-1]

            # Copy the message to the designated dead letter queue
            dlq = get_sqs_queue(queue_name=dlq_name)
            message_attr = (
                self._current_sqs_message.message_attributes.copy()
                if self._current_sqs_message.message_attributes
                else {}
            )
            dlq_response = dlq.send_message(MessageBody=self._current_sqs_message.body, MessageAttributes=message_attr)
        except Exception as exc:
            error = (
                "Error occurred when parent dispatcher with PID [{}] tried to move the message "
                "for worker process with PID [{}] "
                "to the dead letter queue [{}].".format(os.getpid(), self._worker_process.pid, dlq_name)
            )
            log_dispatcher_message(self, message=error, is_exception=True)
            raise QueueWorkDispatcherError(
                error, worker_process_name=self.worker_process_name, queue_message=self._current_sqs_message
            ) from exc

        log_dispatcher_message(
            self,
            message='Message sent to dead letter queue "{}" '
            "with [{}] response code. "
            "Now deleting message from origin queue".format(
                dlq_name, dlq_response["ResponseMetadata"]["HTTPStatusCode"]
            ),
            is_debug=True,
        )
        self.delete_message_from_queue()

    def _monitor_work_progress(self):
        """Tracks the running or exit status of the child worker process.

        There are four scenarios it detects when monitoring:
            1. It extends life of the process back at the queue if it detects the child worker process is still
               running
            2. It sees the child worker process exited with a 0 exit code (success). It deletes the message from
               the queue and logs success.
            3. It sees the child worker process exited with a > 0 exit code. Handles this and raises
               :exc:`QueueWorkerProcessError` to alert the caller.
            4. It sees the child worker process exited with a < 0 exit code, indicating it received some kind of
               signal. It calls the signal-handling code to handle this accordingly.

        Raises:
            QueueWorkerProcessError: When the child worker process was found to have exited with a > 0 exit
                code.
            Others: Anything raised by :meth:`_set_message_visibility`, :meth:`delete_message_from_queue`,
                or :meth:`_handle_exit_signal`
        """
        monitor_process = True
        heartbeats = 0
        while monitor_process:
            monitor_process = False
            if self._worker_process.is_alive():
                # Process still working. Send "heartbeat" to SQS so it may continue
                if (heartbeats * self._monitor_sleep_time) >= self._sqs_heartbeat_log_period_seconds:
                    log_dispatcher_message(
                        self,
                        message="Job worker process with PID [{}] is still running. "
                        "Renewing VisibilityTimeout of {} seconds".format(
                            self._worker_process.pid, self._default_visibility_timeout
                        ),
                        is_debug=True,
                    )
                    heartbeats = 0
                else:
                    heartbeats += 1
                self._set_message_visibility(self._default_visibility_timeout)
                time.sleep(self._monitor_sleep_time)
                monitor_process = True
            elif self._worker_process.exitcode == 0:
                # If process exits with 0: success! Remove from queue
                log_dispatcher_message(
                    self,
                    message="Job worker process with PID [{}] completed with 0 for exit code (success). Deleting "
                    "message from the queue".format(self._worker_process.pid),
                )
                self.delete_message_from_queue()
            elif self._worker_process.exitcode > 0:
                # If process exits with positive code, an ERROR occurred within the worker process.
                # Don't delete the message, don't force retry, and don't force into dead letter queue.
                # Let the VisibilityTimeout expire, and the queue will handle whether to retry or put in
                # the DLQ based on its queue configuration.
                # Raise an exception to give control back to the caller to allow any error-handling to take place there
                message = "Job worker process with PID [{}] errored with exit code: {}.".format(
                    self._worker_process.pid, self._worker_process.exitcode
                )
                log_dispatcher_message(self, message=message, is_error=True)
                raise QueueWorkerProcessError(
                    message, worker_process_name=self.worker_process_name, queue_message=self._current_sqs_message
                )
            elif self._worker_process.exitcode < 0:
                # If process exits with a negative code, process was terminated by a signal since
                # a Python subprocess returns the negative value of the signal.
                signum = self._worker_process.exitcode * -1
                # In the rare case where the child worker process's exit signal is detected before its parent
                # process received the same signal, proceed with handling the child's exit signal
                self._handle_exit_signal(signum=signum, frame=None, parent_dispatcher_signaled=False)

    def _handle_exit_signal(self, signum, frame, parent_dispatcher_signaled=True, is_retry=False):
        """Attempt to gracefully handle the exiting of the job as a result of receiving an exit signal.

        NOTE: This handler is only expected to be run from the parent dispatcher process. It is an error
        condition if it is invoked from the child worker process. Signals received in the child worker
        process should not have this handler registered for those signals. Because signal handlers registered for a
        parent process are inherited by forked child processes (see: "man 7 signal" docs and search for
        "inherited"), this handler will initially be registered for each of this class's EXIT_SIGNALS; however,
        those handlers are reset to their default as the worker process is started by a wrapper function around
        the job to execute.

        The signal very likely indicates a non-error failure scenario from which the job might be restarted or
        rerun, if allowed. Handle cleanup, logging, job retry logic, etc.
        NOTE: Order is important:
            1. "Lock" (guard) this exit handler, to handle one exit signal only
            2. Extend VisibilityTimeout by _exit_handling_timeout, to allow time for cleanup
            3. Suspend child worker process, if alive, else skip (to not interfere with cleanup)
            4. Execute pre-exit cleanup
            5. Move the message (back to the queue, or to the dead letter queue)
            6. Kill child worker process
            7. Exit this parent dispatcher process if it received an exit signal

        Args:
            signum: number representing the signal received
            frame: Frame passed in with the signal
            parent_dispatcher_signaled: Assumed that this handler is being called as a result of signaling that
                happened on the parent dispatcher process. If this was instead the parent detecting that the child
                exited due to a signal, set this to False. Defaults to True.
            is_retry: If this is the 2nd and last try to handled the signal and perform pre-exit cleanup

        Raises:
            QueueWorkerProcessError: When the _exit_handling function cannot be completed in time to perform
                pre-exit cleanup
            QueueWorkerProcessError: If only the child process died (not the parent dispatcher), but the message it
                was working was moved to the Dead Letter Queue because the queue is not configured to allow retries,
                raise an exception so the parent process might mark the job as failed with its normal error-handling
                process
            SystemExit: If the parent dispatcher process received an exit signal, this should be raised
                after handling the signal, as part of running os.kill of that process using the original signal
                number that was originally handled
        """
        # If the process handling this signal is the same as the started worker process, this *is* the worker process
        is_worker_process = self._worker_process and self._worker_process.pid == os.getpid()

        # If the signal is in BSD_SIGNALS, use the human-readable string, otherwise use the signal value
        signal_or_human = BSD_SIGNALS.get(signum, signum)

        if is_worker_process:
            log_dispatcher_message(
                self,
                message="Worker process with PID [{}] received signal [{}] while running. "
                "It should not be handled in this exit signal handler, "
                "as it is meant to run from the parent dispatcher process."
                "Exiting worker process with original exit signal.".format(os.getpid(), signal_or_human),
                is_warning=True,
            )
            self._kill_worker(with_signal=signum)

        elif not parent_dispatcher_signaled:
            log_dispatcher_message(
                self,
                message="Job worker process with PID [{}] exited due to signal with exit code: [{}], "
                "after receiving exit signal [{}]. "
                "Gracefully handling exit of job".format(
                    self._worker_process.pid, self._worker_process.exitcode, signal_or_human
                ),
                is_error=True,
            )
            # Attempt to cleanup children, but likely will be a no-op because the worker has already exited and
            # orphaned them
            self._kill_worker(with_signal=signum, just_kill_descendants=True)

        else:  # dispatcher signaled
            log_dispatcher_message(
                self,
                message="Parent dispatcher process with PID [{}] received signal [{}] while running. "
                "Gracefully stopping job being worked".format(os.getpid(), signal_or_human),
                is_error=True,
            )

            # Make sure the parent dispatcher process exits after handling the signal by setting this flag
            self._dispatcher_exiting = True

        if self._handling_exit_signal and not is_retry:
            # Don't allow the parent dispatcher process to handle more than one exit signal, unless this is a retry
            # of the first attempt
            return

        self._handling_exit_signal = True

        exit_handling_failed = False  # flag to mark this kind of end-state of this handler
        try:
            # Extend message visibility for as long is given for the exit handling to process, so message does not get
            # prematurely returned to the queue or moved to the dead letter queue.
            # Give it a 5 second buffer in case retry mechanics put it over the timeout
            self._set_message_visibility(self._exit_handling_timeout + 5)

            if self._worker_process.is_alive():
                worker = ps.Process(self._worker_process.pid)
                if not is_retry:
                    # Suspend the child worker process so it does not conflict with doing cleanup in the exit_handler
                    log_dispatcher_message(
                        self,
                        message="Suspending worker process with PID [{}] during first try of exit_handler".format(
                            self._worker_process.pid
                        ),
                        is_debug=True,
                    )
                    worker.suspend()
                else:
                    # This is try 2. The cleanup was not able to complete on try 1 with the worker process merely
                    # suspended. There could be some kind of transaction deadlock. Kill the worker to clear the way
                    # for cleanup retry
                    log_dispatcher_message(
                        self,
                        message="Killing worker process with PID [{}] during second try of exit_handler".format(
                            self._worker_process.pid
                        ),
                        is_debug=True,
                    )
                    self._kill_worker()
            else:
                try_attempt = "second" if is_retry else "first"
                log_dispatcher_message(
                    self,
                    message="Worker process with PID [{}] is not alive during {} try of exit_handler".format(
                        self._worker_process.pid, try_attempt
                    ),
                    is_debug=True,
                )

            if self._exit_handler is not None:
                # Execute cleanup procedures to handle exiting of the worker process
                # Wrap cleanup in a fixed timeout, and a retry (one time)
                try:
                    with ExecutionTimeout(self._exit_handling_timeout):
                        # Call _exit_handler callable to do cleanup
                        queue_message_param = inspect.signature(self._exit_handler).parameters.get("queue_message")
                        arg_spec = inspect.getfullargspec(self._exit_handler)
                        if queue_message_param or arg_spec.varkw == "kwargs":
                            # it defines a param named "queue_message", or accepts kwargs,
                            # so pass along the message as "queue_message" in  case it's needed
                            log_dispatcher_message(
                                self,
                                message="Invoking job exit_handler [{}] with args={}, kwargs={}, "
                                "and queue_message={}".format(
                                    self._exit_handler, self._job_args, self._job_kwargs, self._current_sqs_message
                                ),
                            )
                            self._exit_handler(
                                *self._job_args, **self._job_kwargs, queue_message=self._current_sqs_message
                            )
                        else:
                            log_dispatcher_message(
                                self,
                                message="Invoking job exit_handler [{}] with args={} "
                                "and kwargs={}".format(self._exit_handler, self._job_args, self._job_kwargs),
                            )
                            self._exit_handler(*self._job_args, **self._job_kwargs)
                except TimeoutError:
                    exit_handling_failed = True
                    if not is_retry:
                        log_dispatcher_message(
                            self,
                            message="Could not perform cleanup during exiting of job in allotted "
                            "_exit_handling_timeout ({}s). "
                            "Retrying once.".format(self._exit_handling_timeout),
                            is_warning=True,
                        )
                        # Attempt retry
                        self._handle_exit_signal(signum, frame, parent_dispatcher_signaled, is_retry=True)
                    else:
                        message = (
                            "Could not perform cleanup during exiting of job in allotted "
                            "_exit_handling_timeout ({}s) after 2 tries. "
                            "Raising exception.".format(self._exit_handling_timeout)
                        )
                        log_dispatcher_message(self, message=message, is_error=True)
                        raise QueueWorkDispatcherError(
                            message,
                            worker_process_name=self.worker_process_name,
                            queue_message=self._current_sqs_message,
                        )
                except Exception as exc:
                    exit_handling_failed = True
                    message = "Execution of exit_handler failed for unknown reason. See Traceback."
                    log_dispatcher_message(self, message=message, is_exception=True)
                    raise QueueWorkDispatcherError(
                        message, worker_process_name=self.worker_process_name, queue_message=self._current_sqs_message
                    ) from exc

            if self.allow_retries:
                log_dispatcher_message(
                    self,
                    message="Dispatcher for this job allows retries of the message. "
                    "Attempting to return message to its queue.",
                )
                self.surrender_message_to_other_consumers()
            else:
                # Otherwise, attempt to send message directly to the dead letter queue
                log_dispatcher_message(
                    self,
                    message="Dispatcher for this job does not allow message to be retried. "
                    "Attempting to move message to its Dead Letter Queue.",
                )
                self.move_message_to_dead_letter_queue()

            # Now that SQS message has been handled, ensure worker process is dead
            if self._worker_process.is_alive():
                log_dispatcher_message(
                    self,
                    message="Message handled. Now killing worker process with PID [{}] "
                    "as it is alive but has no more work to do.".format(self._worker_process.pid),
                    is_debug=True,
                )
                self._kill_worker()
        finally:
            if exit_handling_failed:
                # Don't perform final logic if we got here after failing to execute the exit handler
                if is_retry:
                    # 2nd time we've failed to process exit_handle? - log so and move to DLQ
                    log_dispatcher_message(
                        self,
                        message="Graceful exit_handling failed after 2 attempts for job being worked by worker process "
                        "with PID [{}]. An attempt to put message directly on the Dead Letter Queue will be "
                        "made, because cleanup logic in the exit_handler could not be guaranteed "
                        "(retrying a message without prior cleanup "
                        "may compound problems).".format(self._worker_process.pid),
                        is_error=True,
                    )
                    self.move_message_to_dead_letter_queue()
            else:
                # exit_handler not provided, or processed successfully
                self._handling_exit_signal = False
                if self._dispatcher_exiting:
                    # An exit signal was received by the parent dispatcher process.
                    # Continue with exiting the parent process, as per the original signal, after having handled it
                    log_dispatcher_message(
                        self,
                        message="Exiting from parent dispatcher process with PID [{}] "
                        "to culminate exit-handling of signal [{}]".format(os.getpid(), signal_or_human),
                        is_debug=True,
                    )
                    # Simply doing sys.exit or raise SystemExit, even with a given negative exit code won't exit this
                    # process with the negative signal value, as is the Python custom.
                    # So override the assigned handler for the signal we're handing, resetting it back to default,
                    # and kill the process using that signal to get it to exit as it would if we never handled it
                    signal.signal(signum, signal.SIG_DFL)
                    os.kill(os.getpid(), signum)
                elif not self.allow_retries:
                    # If only the child process died, but the message it was working was moved to the Dead Letter Queue
                    # because the queue is not configured to allow retries, raise an exception so the caller
                    # might mark the job as failed with its normal error-handling process
                    raise QueueWorkerProcessError(
                        "Worker process with PID [{}] was not able to complete its job due to "
                        "interruption from exit signal [{}]. The queue message for that job "
                        "was moved to the Dead Letter Queue, where it can be reviewed for a "
                        "manual restart, or other remediation.".format(self._worker_process.pid, signal_or_human),
                        worker_process_name=self.worker_process_name,
                        queue_message=self._current_sqs_message,
                    )

    def _set_message_visibility(self, new_visibility):
        """Sends a request back to the SQS API via boto3 to refresh the time that the currently-in-flight message
        will be *invisible* to other consumers of the queue, such that they cannot also process it.

        Examples:
            If :param:`new_visibility` is 60, then in 60 seconds from when SQS receives the request, so long as
            the ``VisibilityTimeout`` is not adjusted again, the in-flight message will reappear on the queue. It
            will remain invisible until that time.

        Raises:
            QueueWorkDispatcherError: If for some reason the boto3 change_visibility() operation caused an error
        """
        if self._current_sqs_message is None:
            log_dispatcher_message(
                self,
                message="No SQS message to change visibility of. Message might have previously been released, "
                "deleted, or moved to dead letter queue.",
                is_warning=True,
            )
            return
        try:
            self._current_sqs_message.change_visibility(VisibilityTimeout=new_visibility)
        except Exception as exc:
            message = (
                "Unable to set VisibilityTimeout. "
                "Message might have previously been deleted upon completion or failure"
            )
            log_dispatcher_message(self, message=message, is_exception=True)
            raise QueueWorkDispatcherError(
                message, worker_process_name=self.worker_process_name, queue_message=self._current_sqs_message
            ) from exc

    def _kill_worker(self, with_signal=None, just_kill_descendants=False):
        """
        Cleanup (kill) a worker process and its spawned descendant processes
        Args:
            with_signal: Use this termination signal when killing. Otherwise, hard kill (-9)
            just_kill_descendants: Set to True to leave the worker and only kill its descendants
        """
        if not self._worker_process or not ps.pid_exists(self._worker_process.pid):
            return
        try:
            worker = ps.Process(self._worker_process.pid)
        except ps.NoSuchProcess:
            return

        if self._worker_can_start_child_processes:
            # First kill any other processes the worker may have spawned
            for spawn_of_worker in worker.children(recursive=True):
                if with_signal:
                    log_dispatcher_message(
                        self,
                        message=f"Attempting to terminate child process with PID [{spawn_of_worker.pid}] and name "
                        f"[{spawn_of_worker.name}] using signal [{with_signal}]",
                        is_warning=True,
                    )
                    try:
                        spawn_of_worker.send_signal(with_signal)
                    except ps.NoSuchProcess:
                        pass
                else:
                    log_dispatcher_message(
                        self,
                        message=f"Attempting to kill child process with PID [{spawn_of_worker.pid}] and name "
                        f"[{spawn_of_worker.name}]",
                        is_warning=True,
                    )
                    try:
                        spawn_of_worker.kill()
                    except ps.NoSuchProcess:
                        pass
        if not just_kill_descendants:
            if with_signal:
                # Ensure the worker exits as would have occurred if not handling its signals
                signal.signal(with_signal, signal.SIG_DFL)
                worker.send_signal(with_signal)
            else:
                try:
                    worker.kill()
                except ps.NoSuchProcess:
                    pass

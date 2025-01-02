import signal
from abc import ABCMeta


class AbstractQueueError(Exception, metaclass=ABCMeta):
    """Abstract base exception representing for error scenarios encountered in the queue dispatcher or worker."""

    def __init__(self, *args, worker_process_name=None, queue_message=None, **kwargs):
        if not worker_process_name:
            worker_process_name = "Unknown Worker"

        if queue_message:
            default_message = (
                f"Queue processing error with non-zero exit code for worker process"
                f' "{worker_process_name}" working on message: {queue_message}'
            )
        else:
            default_message = (
                f"Queue processing error with non-zero exit code for worker process"
                f' "{worker_process_name}". Queue Message None or not provided.'
            )

        if args or kwargs:
            super().__init__(*args, **kwargs)
        else:
            super().__init__(default_message)
        self.worker_process_name = worker_process_name
        self.queue_message = queue_message


class QueueWorkerProcessError(AbstractQueueError):
    """Custom exception representing the scenario where the spawned worker process has failed
    with a non-zero exit code, indicating some kind of failure.
    """

    pass


class QueueWorkDispatcherError(AbstractQueueError):
    """Custom exception representing the scenario where the parent process dispatching to and monitoring the worker
    process has failed with some kind of unexpected exception.
    """

    pass


class ExecutionTimeout:
    def __init__(self, seconds=0, error_message="Execution took longer than the allotted time"):
        self.seconds = seconds
        self.error_message = error_message

    def _timeout_handler(self, signum, frame):
        raise TimeoutError(self.error_message)

    def __enter__(self):
        if self.seconds > 0:
            signal.signal(signal.SIGALRM, self._timeout_handler)
            signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        signal.alarm(0)

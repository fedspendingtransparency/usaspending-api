import contextlib
import math
import time

from datetime import timedelta


@contextlib.contextmanager
def timer(msg="", logging_func=print):
    """
    Use as a context manager or decorator to report on elapsed time.

        with timer('stuff', logger.info):
            # (your code here)

    Simple to use, but the automatic logging of "finished" messages is a little
    confusing when errors occur in code wrapped by the timer.
    """
    start = time.perf_counter()
    logging_func("Beginning {}...".format(msg))
    try:
        yield {}
    finally:
        elapsed = time.perf_counter() - start
        logging_func("... finished {} in {:.2f}s".format(msg, elapsed))


class Timer:
    """
    A bit less elegant, but provides the caller total control over what gets
    displayed and when.  Can also estimate run time/remaining run time.

    You can keep it simple.

        with Timer("my thing"):
            # (your code here)

    Or get as sophisticated as you want.

        print("Starting my thing")
        try:
            with Timer() as t:
                for n in range(100):
                    # (your code here)
                    print(
                        "Finished thing {} after {}, "
                        "{} estimated remaining, "
                        "{} estimated overall runtime".format(
                            n,
                            t.elapsed,  # default formatting for timedelta
                            t.as_string(t.estimated_remaining_runtime((n + 1) / 100)),  # special as_string formatting
                            t.as_string(t.estimated_total_runtime((n + 1) / 100))  # special as_string formatting
                        )
                    )
            print("Finished all things after {}".format(t))
        except:
            print("Failed to do my thing after {}".format(t))
            raise

    """

    _formats = "{:,} d", "{} h", "{} m", "{} s", "{} ms"

    def __init__(self, message=None, success_logger=print, failure_logger=print):
        """
        For automatic logging, include a message.  To change from print to, say,
        an actual logger, provide the success and failure log functions you want
        to use.

            t = Timer("my thing", success_logger=logger.info, failure_logger=logger.error)

        I would probably not pass an exception logger in here.  Exceptions
        should be handled in your code.
        """
        self.message = message
        self.success_logger = success_logger
        self.failure_logger = failure_logger
        self.start()

    def __enter__(self):
        self.log_starting_message()
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()
        if exc_type is None:
            self.log_success_message()
        else:
            self.log_failure_message()

    def __repr__(self):
        return self.as_string(self.elapsed)

    def start(self):
        self._start = time.perf_counter()
        self._stop = None
        self._elapsed = None

    def stop(self):
        self._stop = time.perf_counter()
        self._elapsed = timedelta(seconds=(self._stop - self._start))

    def log_starting_message(self):
        if self.message:
            self.success_logger(self.starting_message)

    @property
    def starting_message(self):
        return "[{}] starting...".format(self.message)

    def log_success_message(self):
        if self.message:
            self.success_logger(self.success_message)

    @property
    def success_message(self):
        return "[{}] finished successfully after {}".format(self.message, self)

    def log_failure_message(self):
        if self.message:
            self.failure_logger(self.failure_message)

    @property
    def failure_message(self):
        return "[{}] FAILED AFTER {}".format(self.message, self)

    @property
    def elapsed(self):
        if self._start is None:
            raise RuntimeError("Timer has not been started")
        if self._elapsed is None:
            return timedelta(seconds=(time.perf_counter() - self._start))
        return self._elapsed

    def estimated_total_runtime(self, ratio):
        if self._start is None:
            raise RuntimeError("Timer has not been started")
        if self._elapsed is None:
            return timedelta(seconds=((time.perf_counter() - self._start) / ratio))
        return self._elapsed

    def estimated_remaining_runtime(self, ratio):
        if self._elapsed is None:
            return max(self.estimated_total_runtime(ratio) - self.elapsed, timedelta())
        return timedelta()  # 0

    @classmethod
    def as_string(cls, elapsed):
        """elapsed should be a timedelta"""
        f, s = math.modf(elapsed.total_seconds())
        ms = round(f * 1000)
        m, s = divmod(s, 60)
        h, m = divmod(m, 60)
        d, h = divmod(h, 24)

        return (
            " ".join(
                f.format(b) for f, b in zip(cls._formats, tuple(int(n) for n in (d, h, m, s, ms))) if b > 0
            )
            or "less than a millisecond"
        )

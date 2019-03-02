import functools
import time

MAX_ARGUMENT_CHARACTERS = 77


def time_this(func):
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start_time = time.perf_counter()
        value = func(*args, **kwargs)
        end_time = time.perf_counter()

        print("Finished {!r} in {:.4f}s".format(func.__name__, end_time - start_time))
        return value
    return wrapper_timer


def print_this(func):
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        arg_string = ", ".join([str(a) for a in args] + ["{}={}".format(k, v) for k, v in kwargs.items()])
        if len(arg_string) > MAX_ARGUMENT_CHARACTERS:
            arg_string = arg_string[:MAX_ARGUMENT_CHARACTERS] + '...'
        print("--> {}({})".format(func.__name__, arg_string))

        return func(*args, **kwargs)
    return wrapper_timer

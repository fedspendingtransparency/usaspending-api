from argparse import ArgumentTypeError
from datetime import timezone
from dateutil import parser


def cast_datetime_to_naive(datetime):
    """
    Removes timezone information, but converts non-UTC datetimes to UTC
    beforehand so that the returned datetime will be naive but will also be UTC.
    """
    if datetime.tzinfo is not None:
        datetime = datetime.astimezone(timezone.utc)
    return datetime.replace(tzinfo=None)


def cast_datetime_to_utc(datetime):
    """
    If datetime has no tzinfo, assume it is UTC, otherwise convert the
    datetime to UTC.
    """
    if datetime.tzinfo is None:
        return datetime.replace(tzinfo=timezone.utc)
    return datetime.astimezone(timezone.utc)


def datetime_command_line_argument_type(naive):
    """
    This function is designed to be used as a date/time type for argparse
    command line parameters.  argparse parameter types need to be passed
    in as functions that take a single input string.  Wrapping the function
    allows us to provide time zone handling instructions to the wrapped function.

    If naive is True, the parsed date/time will be converted to UTC and
    stripped of its timezone information.

    If naive is False, the parsed date/time will be converted to UTC if it
    is timezone aware.  If it is timezone naive, it is assumed to be UTC.
    """
    def _datetime_command_line_argument_type(input_string):
        """
        A very flexible date/time parser to be used as a command line argument
        parser.  See wrapper for timezone handling instructions.

        Accepts a string that is, presumably, some sort of date.

        Returns a datetime with appropriate adjusted for timezone as dictated
        by the naive parameter in the wrapper.
        """
        try:
            parsed = parser.parse(input_string)
            if naive:
                return cast_datetime_to_naive(parsed)
            else:
                return cast_datetime_to_utc(parsed)

        except (OverflowError, TypeError, ValueError):
            raise ArgumentTypeError('Unable to convert provided value to date/time')

    return _datetime_command_line_argument_type


def get_date_from_datetime(date_time, **kwargs):
    """
    Pass a keyword argument called "default" if you wish to have a specific
    value returned when the date cannot be extracted from date_time, otherwise
    date_time will be returned.
    """
    try:
        return date_time.date()
    except Exception:
        return kwargs.get('default', date_time)

import logging

from django.db import connection
from django.db.utils import OperationalError

from usaspending_api.common.exceptions import EndpointTimeoutException

logger = logging.getLogger(__name__)


def set_db_timeout(timeout_in_seconds):
    """Decorator used to set the database statement timeout within the Django app scope

    Args:
        timeout_in_seconds (required): timeout value, in seconds

    NOTE:
        The statement_timeout is only set for this specific connection. The timeout is reset to 0 at the end of
        each call so that even idle connections that may be reused aren't bound by the timeout settings from an
        old API call.

    Examples:
        @set_db_timeout(test_timeout_in_ms)
        def func_running_db_call(...):
            ...

        OR

        @set_db_timeout()  # This will use the default value in settings
        def func_running_db_call(...):
            ...
    """
    timeout_in_ms = int(timeout_in_seconds * 1000)

    def wrap(func):
        def wrapper(*args, **kwargs):
            with connection.cursor() as cursor:
                cursor.execute("show statement_timeout")
                prev_timeout = cursor.fetchall()[0][0]

                logger.warning(
                    "DB TIMEOUT DECORATOR: Old Postgres statement_timeout value = %s on this connection"
                    % str(prev_timeout)
                )

                logger.warning(
                    "DB TIMEOUT DECORATOR: Setting Postgres statement_timeout to %ds  on this connection"
                    % timeout_in_seconds
                )
                cursor.execute("set statement_timeout={0}".format(timeout_in_ms))

                cursor.execute("show statement_timeout")
                logger.warning(
                    "DB TIMEOUT DECORATOR: New Postgres statement_timeout value = %s on this connection"
                    % str(cursor.fetchall()[0][0])
                )

            try:
                func_response = func(*args, **kwargs)
            except OperationalError:
                raise EndpointTimeoutException("Django ORM exceeded the specified timeout of %ds" % timeout_in_seconds)
            finally:
                with connection.cursor() as cursor:
                    cursor.execute("show statement_timeout")
                    logger.warning(
                        "DB TIMEOUT DECORATOR: Old Postgres statement_timeout value = %s on this connection"
                        % str(cursor.fetchall()[0][0])
                    )

                    logger.warning(
                        "DB TIMEOUT DECORATOR: Setting Postgres statement_timeout to {0} on this connection".format(
                            prev_timeout
                        )
                    )
                    cursor.execute("set statement_timeout='{0}'".format(prev_timeout))

                    cursor.execute("show statement_timeout")
                    logger.warning(
                        "DB TIMEOUT DECORATOR: New Postgres statement_timeout value = %s on this connection"
                        % str(cursor.fetchall()[0][0])
                    )

            return func_response

        return wrapper

    return wrap

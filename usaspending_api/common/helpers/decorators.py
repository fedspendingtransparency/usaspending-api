import logging

from django.db import connection
from django.conf import settings

logger = logging.getLogger(__name__)


def set_db_timeout(timeout_in_seconds=settings.DEFAULT_DB_TIMEOUT_IN_SECONDS):
    """ Decorator used to set the database statement timeout within the Django app scope

        Args:
            timeout_in_seconds: timeout value to set, in milliseconds

        NOTE:
            The statement_timeout is only set for this specific connection. Any subsequent request will have a default
            value of 0. The alteration DOES NOT persist across connections and since each request is a new connection to
            the database, there is no need to reset the statement_timeout value.

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
                logger.warning('DB TIMEOUT DECORATOR: Old Django statement_timeout value = %s' %
                               str(cursor.fetchall()[0][0]))

                logger.warning(
                    'DB TIMEOUT DECORATOR: Setting Django statement_timeout to %sms' % str(timeout_in_ms))
                cursor.execute("set statement_timeout={0}".format(timeout_in_ms))

                cursor.execute("show statement_timeout")
                logger.warning('DB TIMEOUT DECORATOR: New Django statement_timeout value = %s' %
                               str(cursor.fetchall()[0][0]))

            func_response = func(*args, **kwargs)

            with connection.cursor() as cursor:
                cursor.execute("show statement_timeout")
                logger.warning('DB TIMEOUT DECORATOR: Old Django statement_timeout value = %s' %
                               str(cursor.fetchall()[0][0]))

                logger.warning(
                    'DB TIMEOUT DECORATOR: Setting Django statement_timeout to 0')
                cursor.execute("set statement_timeout=0")

                cursor.execute("show statement_timeout")
                logger.warning('DB TIMEOUT DECORATOR: New Django statement_timeout value = %s' %
                               str(cursor.fetchall()[0][0]))

            return func_response
        return wrapper
    return wrap

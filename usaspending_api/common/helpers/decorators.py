import logging

from django.db import connection
from django.utils.decorators import method_decorator

DEFAULT_DB_TIMEOUT_IN_MS = 300000
logger = logging.getLogger(__name__)


def set_db_timeout(timeout_in_ms=DEFAULT_DB_TIMEOUT_IN_MS):
    """ Decorator used to set the database statement timeout within the Django app scope

        Args:
            timeout_in_ms: timeout value to set, in milliseconds

        NOTE:
            The statement_timeout is only set for this specific connection. Any subsequent request will have a default
            value of 0. The alteration DOES NOT persist across connections and since each request is a new connection to
            the database, there is no need to reset the statement_timeout value.
    """
    def class_based_decorator(class_based_view):
        def view_func(function):
            def wrap(request, *args, **kwargs):
                with connection.cursor() as cursor:
                    cursor.execute("show statement_timeout")
                    logger.warning('DB TIMEOUT DECORATOR: Old Django statement_timeout value = %sms' %
                                   str(cursor.fetchall()[0][0]))

                    logger.warning(
                        'DB TIMEOUT DECORATOR: Setting Django statement_timeout to %sms' % str(timeout_in_ms))
                    cursor.execute("set statement_timeout={0}".format(timeout_in_ms))

                    cursor.execute("show statement_timeout")
                    logger.warning('DB TIMEOUT DECORATOR: New Django statement_timeout value = %s' %
                                   str(cursor.fetchall()[0][0]))
                return function(request, *args, **kwargs)
            return wrap
        class_based_view.post = method_decorator(view_func)(class_based_view.post)
        return class_based_view
    return class_based_decorator

import logging
from typing import Callable

from django.db import connection
from django.db.utils import OperationalError
from ninja import Router
from rest_framework.request import Request
from rest_framework.views import APIView

from usaspending_api.common.exceptions import EndpointTimeoutException
from usaspending_api.common.helpers.endpoint_documentation import DJANGO_NINJA_TEMP_NAME_FOR_DRF

logger = logging.getLogger(__name__)


def set_db_timeout(timeout_in_seconds: int) -> Callable:
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

    def wrap(func: Callable) -> Callable:
        def wrapper(*args, **kwargs) -> Callable:
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
            except OperationalError as exc:
                raise EndpointTimeoutException(
                    "Django ORM exceeded the specified timeout of %ds" % timeout_in_seconds
                ) from exc
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


def _served_by_ninja(self: APIView, request: Request, *args, **kwargs) -> NotImplementedError:
    """Real traffic is handled by the Django Ninja operation; this exists only so
    DRF advertises the method and renders the browsable page for a browser GET."""
    raise NotImplementedError


def browsable(
    router: Router, path: str, *, endpoint_doc: str, methods: tuple[str] = ("POST",), **ninja_kwargs
) -> Callable:
    """
    This decorator is a temporary solution while we have the DRF API UI that needs to display the endpoints
    defined with Django Ninja.
    """

    def decorator(view_func: Callable) -> Callable:
        doc_view = type(
            f"{view_func.__name__.title().replace('_', '')}",
            (APIView,),
            {
                "__doc__": view_func.__doc__,
                "endpoint_doc": endpoint_doc,
                **{m.lower(): _served_by_ninja for m in methods},
            },
        )
        rendered = doc_view.as_view()
        router.get(path, auth=None, include_in_schema=False, url_name=DJANGO_NINJA_TEMP_NAME_FOR_DRF)(
            lambda request: rendered(request)
        )
        return router.api_operation(list(methods), path, **ninja_kwargs)(view_func)

    return decorator

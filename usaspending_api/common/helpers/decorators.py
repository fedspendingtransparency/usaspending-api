import logging
from typing import Callable, Coroutine

from asgiref.sync import sync_to_async
from django.db import connection
from django.db.utils import OperationalError
from django.http import HttpResponseBase, StreamingHttpResponse
from ninja import Router
from ninja.decorators import decorate_view
from rest_framework.request import Request
from rest_framework.response import Response as DRFResponse
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


def _show_browsable_result(self: APIView, request: Request, *args, **kwargs) -> DRFResponse:
    """Renders the real Ninja response that the browsable()-installed run() wrapper
    already buffered onto the request, through DRF's normal browsable rendering pipeline."""
    try:
        status_code, body_text = request._browsable_result
    except AttributeError as exc:
        raise NotImplementedError("browsable() did not buffer a result for this request.") from exc
    return DRFResponse(body_text, status=status_code)


async def _buffer_streaming_response(response: HttpResponseBase) -> tuple[int, str]:
    if isinstance(response, StreamingHttpResponse):
        body = b"".join([part async for part in response])
    else:
        body = response.content
    return response.status_code, body.decode()


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
                **{m.lower(): _show_browsable_result for m in methods},
            },
        )
        rendered = doc_view.as_view()
        router.get(path, auth=None, include_in_schema=False, url_name=DJANGO_NINJA_TEMP_NAME_FOR_DRF)(
            lambda request: rendered(request)
        )

        registered_view_func = router.api_operation(list(methods), path, **ninja_kwargs)(view_func)
        operation = registered_view_func._ninja_operation
        if not operation.is_async:
            raise RuntimeError("@browsable only supports async Ninja operations.")

        def _html_aware_run(run_func: Callable) -> Callable:
            async def wrapper(request: Request, *args, **kwargs) -> Coroutine:
                result = await run_func(request, *args, **kwargs)
                if "text/html" not in request.headers.get("Accept", ""):
                    return result
                status_code, body_text = await _buffer_streaming_response(result)
                request._browsable_result = (status_code, body_text)
                return await sync_to_async(rendered, thread_sensitive=True)(request)

            return wrapper

        decorate_view(_html_aware_run)(registered_view_func)

        return registered_view_func

    return decorator

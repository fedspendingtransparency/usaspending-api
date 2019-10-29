from django.utils.decorators import method_decorator
from usaspending_api.common.exceptions import InvalidParameterException, EndpointRemovedException
from usaspending_api.awards.v2.filters.filter_helpers import transform_keyword
import logging


API_TRANSFORM_FUNCTIONS = [transform_keyword]
logger = logging.getLogger("console")


def api_transformations(api_version, function_list):
    """
    Decorator designed to transform request object from API call to allow for backwards
    compatibility between API versions. Functions being passed to this decorator should
    accept a request object, and return it after modifications.
    """

    def class_based_decorator(ClassBasedView):
        def view_func(function):
            def wrap(request, *args, **kwargs):
                for func in function_list:
                    try:
                        request = func(request, api_version)
                    except InvalidParameterException:
                        raise
                return function(request, *args, **kwargs)

            return wrap

        ClassBasedView.post = method_decorator(view_func)(ClassBasedView.post)
        return ClassBasedView

    return class_based_decorator


def deprecated(function):
    """Add deprecation warning to endpoint"""

    def wrap(request, *args, **kwargs):
        response = function(request, *args, **kwargs)
        logger.warning('Endpoint "{}" is deprecated. Please move to v2 endpoints.'.format(request.path))
        msg = (
            "WARNING! You are using a deprecated version of the API"
            " which may be unstable and will be removed in the future."
        )
        response["X-API-Warn"] = msg
        return response

    return wrap


def removed(function):
    """Return 410 response instead of doing anything else"""

    def wrap(request, *args, **kwargs):

        raise EndpointRemovedException()

    return wrap

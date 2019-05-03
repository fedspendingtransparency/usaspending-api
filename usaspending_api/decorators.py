import logging


logger = logging.getLogger('console')


def deprecated(function):
    """Add deprecation warning to endpoint"""
    def wrap(request, *args, **kwargs):
        response = function(request, *args, **kwargs)
        logger.warning('Endpoint "{}" is deprecated. Please move to v2 endpoint.'.format(request.path))
        response["X-API-Warn"] = "Deprecated API"
        return response
    return wrap

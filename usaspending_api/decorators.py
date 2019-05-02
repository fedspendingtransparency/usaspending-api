def deprecated(function):
    """Add deprecation warning to endpoint"""
    def wrap(request, *args, **kwargs):
        response = function(request, *args, **kwargs)
        response["X-API-Warn"] = "Deprecated API"
        return response
    return wrap

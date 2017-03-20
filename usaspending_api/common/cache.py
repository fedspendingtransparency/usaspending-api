from rest_framework_extensions.key_constructor import bits
from rest_framework_extensions.key_constructor.constructors import DefaultKeyConstructor


class GetPostQueryParamsKeyBit(bits.QueryParamsKeyBit):
    """
    Override QueryParamsKey method in drf-extensions to ensure that
    the query params part of our cache key includes directives in
    a POST request (i.e., request.data) as well as GET parameters
    """

    def get_source_dict(self, params, view_instance, view_method, request, args, kwargs):
        params = dict(request.query_params)
        params.update(dict(request.data))
        return params


class USAspendingKeyConstructor(DefaultKeyConstructor):
    """
    Handle cache key construction for API requests. If we never need to create
    more nuanced keys, see the drf-extensions documentation:
    http://chibisov.github.io/drf-extensions/docs/#default-key-constructor
    """
    request_params = GetPostQueryParamsKeyBit()
    unique_view_id = bits.UniqueMethodIdKeyBit()

usaspending_key_func = USAspendingKeyConstructor()

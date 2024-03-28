import hashlib
import json

from rest_framework_extensions.key_constructor import bits
from rest_framework_extensions.key_constructor.constructors import DefaultKeyConstructor

from usaspending_api.common.helpers.dict_helpers import order_nested_object


class PathKeyBit(bits.QueryParamsKeyBit):
    """
    Adds query path as a key bit
    """

    def get_source_dict(self, params, view_instance, view_method, request, args, kwargs):
        return {"path": request.path}


class GetPostQueryParamsKeyBit(bits.QueryParamsKeyBit):
    """
    Override QueryParamsKey method in drf-extensions to ensure that the query params part of our cache key includes
    directives in a POST request (i.e., request.data) as well as GET parameters
    """

    def get_source_dict(self, params, view_instance, view_method, request, args, kwargs):

        if hasattr(view_instance, "cache_key_whitelist"):
            whitelist = view_instance.cache_key_whitelist
            params = {}
            for param in whitelist:
                if param in request.query_params:
                    params[param] = request.query_params[param]
                if param in request.data:
                    params[param] = request.data[param]
        else:
            params = dict(request.query_params)
            params.update(dict(request.data))

        if "auditTrail" in params:
            del params["auditTrail"]
        return {"request": json.dumps(order_nested_object(params))}


class USAspendingKeyConstructor(DefaultKeyConstructor):
    """
    Handle cache key construction for API requests. If we never need to create more nuanced keys, see the
    drf-extensions documentation: http://chibisov.github.io/drf-extensions/docs/#default-key-constructor
    """

    path_bit = PathKeyBit()
    request_params = GetPostQueryParamsKeyBit()

    def prepare_key(self, key_dict):
        # Order the key_dict using the order_nested_object function to make sure cache keys are always exactly the same
        ordered_key_dict = json.dumps(order_nested_object(key_dict))
        key_hex = hashlib.md5(ordered_key_dict.encode("utf-8"), usedforsecurity=False).hexdigest()
        return key_hex


usaspending_key_func = USAspendingKeyConstructor()

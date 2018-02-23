# -*- coding: utf-8 -*-
import logging
from django.utils import six
from django.utils.decorators import available_attrs
from functools import wraps
from rest_framework_extensions.cache.decorators import get_cache
from rest_framework_extensions.settings import extensions_api_settings

logger = logging.getLogger(__name__)


# Borrowed from django-rest-framework_extensions cache decorator (rest_framework_extensions.cache.decorators)
class CacheResponse(object):
    def __init__(self, timeout=None, key_func=None, cache=None, cache_errors=None):
        self.timeout = timeout or extensions_api_settings.DEFAULT_CACHE_RESPONSE_TIMEOUT
        self.key_func = key_func or extensions_api_settings.DEFAULT_CACHE_KEY_FUNC
        self.cache_errors = cache_errors or extensions_api_settings.DEFAULT_CACHE_ERRORS
        self.cache = get_cache(cache or extensions_api_settings.DEFAULT_USE_CACHE)

    def __call__(self, func):
        this = self

        @wraps(func, assigned=available_attrs(func))
        def inner(self, request, *args, **kwargs):
            return this.process_cache_response(
                view_instance=self,
                view_method=func,
                request=request,
                args=args,
                kwargs=kwargs,
            )
        return inner

    def process_cache_response(self, view_instance, view_method, request, args, kwargs):
        key = self.calculate_key(view_instance=view_instance, view_method=view_method,
                                 request=request, args=args, kwargs=kwargs)
        response = None
        try:
            response = self.cache.get(key)
        except Exception as e:
            msg = 'Problem while retriving key [{k}] from cache for path:\'{p}\''
            logger.exception(msg.format(k=key, p=str(request.path)))

        if not response:
            response = view_method(view_instance, request, *args, **kwargs)
            response = view_instance.finalize_response(request, response, *args, **kwargs)
            response.render()  # should be rendered, before picklining while storing to cache

            if not response.status_code >= 400 or self.cache_errors:
                try:
                    self.cache.set(key, response, self.timeout)
                except Exception as e:
                    msg = 'Problem while writing to cache: path:\'{p}\' data:\'{d}\''
                    logger.exception(msg.format(p=str(request.path), d=str(request.data)))

        if not hasattr(response, '_closable_objects'):
            response._closable_objects = []

        return response

    def calculate_key(self, view_instance, view_method, request, args, kwargs):
        if isinstance(self.key_func, six.string_types):
            key_func = getattr(view_instance, self.key_func)
        else:
            key_func = self.key_func
        return key_func(view_instance=view_instance, view_method=view_method, request=request, args=args, kwargs=kwargs)


cache_response = CacheResponse

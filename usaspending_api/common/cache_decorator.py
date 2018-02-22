# -*- coding: utf-8 -*-
from functools import wraps
import logging
from django.utils.decorators import available_attrs
from django.utils import six
from rest_framework_extensions.cache.decorators import get_cache
from rest_framework_extensions.settings import extensions_api_settings

logger = logging.getLogger('console')


# Borrowed from django-rest-framework_extensions cache decorator (rest_framework_extensions.cache.decorators)
class CacheResponse(object):
    def __init__(self,
                 timeout=None,
                 key_func=None,
                 cache=None,
                 cache_errors=None):
        if timeout is None:
            self.timeout = extensions_api_settings.DEFAULT_CACHE_RESPONSE_TIMEOUT
        else:
            self.timeout = timeout

        if key_func is None:
            self.key_func = extensions_api_settings.DEFAULT_CACHE_KEY_FUNC
        else:
            self.key_func = key_func

        if cache_errors is None:
            self.cache_errors = extensions_api_settings.DEFAULT_CACHE_ERRORS
        else:
            self.cache_errors = cache_errors

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

    def process_cache_response(self,
                               view_instance,
                               view_method,
                               request,
                               args,
                               kwargs):
        key = self.calculate_key(
            view_instance=view_instance,
            view_method=view_method,
            request=request,
            args=args,
            kwargs=kwargs
        )

        response = None
        try:
            response = self.cache.get(key)
        except Exception as e:
            logger.error('Problem encountered while trying to fetch key from Cache')

        if not response:
            response = view_method(view_instance, request, *args, **kwargs)
            response = view_instance.finalize_response(request, response, *args, **kwargs)
            response.render()  # should be rendered, before picklining while storing to cache

            if not response.status_code >= 400 or self.cache_errors:
                try:
                    self.cache.set(key, response, self.timeout)
                except Exception as e:
                    # msg = 'Problem encountered while trying to write to Cache {r} {a} {k}'
                    # logger.error(msg.format(r=request, a=args, k=kwargs))
                    msg = 'Problem encountered while trying to write to Cache {r}'
                    logger.error(msg.format(r=request))

        if not hasattr(response, '_closable_objects'):
            response._closable_objects = []

        return response

    def calculate_key(self,
                      view_instance,
                      view_method,
                      request,
                      args,
                      kwargs):
        if isinstance(self.key_func, six.string_types):
            key_func = getattr(view_instance, self.key_func)
        else:
            key_func = self.key_func
        return key_func(
            view_instance=view_instance,
            view_method=view_method,
            request=request,
            args=args,
            kwargs=kwargs,
        )


cache_response = CacheResponse

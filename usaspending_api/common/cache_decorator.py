# -*- coding: utf-8 -*-
import logging

from collections.abc import Iterable
from django.conf import settings
from django.db.models import QuerySet
from rest_framework_extensions.cache.decorators import CacheResponse
from typing import Any
from usaspending_api.common.experimental_api_flags import is_experimental_elasticsearch_api

logger = logging.getLogger("console")


def contains_queryset(data: Any) -> bool:
    """Traverse a complex object and return True if a Queryset exists anywhere"""
    type_checks = (
        (dict, lambda x: any([contains_queryset(datum) for datum in x.values()])),
        (str, lambda x: False),  # short-circuit since str is an iterable. Leave before Iterable
        (Iterable, lambda x: any([contains_queryset(datum) for datum in x])),
        (QuerySet, lambda x: True),
    )
    for type_check in type_checks:
        if isinstance(data, type_check[0]):
            return type_check[1](data)
    else:
        return False


class CustomCacheResponse(CacheResponse):
    def process_cache_response(self, view_instance, view_method, request, args, kwargs):
        if is_experimental_elasticsearch_api(request):
            # bypass cache altogether
            response = view_method(view_instance, request, *args, **kwargs)
            response = view_instance.finalize_response(request, response, *args, **kwargs)
            response["Cache-Trace"] = "no-cache"
            return response
        key = self.calculate_key(
            view_instance=view_instance, view_method=view_method, request=request, args=args, kwargs=kwargs
        )
        response = None
        try:
            response = self.cache.get(key)
        except Exception:
            msg = "Problem while retrieving key [{k}] from cache for path:'{p}'"
            logger.exception(msg.format(k=key, p=str(request.path)))

        if not response:
            response = view_method(view_instance, request, *args, **kwargs)
            response = view_instance.finalize_response(request, response, *args, **kwargs)

            # While returning a Queryset is functional most of the time, it isn't
            # fully supported by Django Rest Framework. This check was inserted
            # in local mode to catch if a Queryset is being returned by the view
            # which could cause an exception when setting the cache
            if settings.IS_LOCAL and response and not response.is_rendered:
                if contains_queryset(response.data):
                    raise RuntimeError(
                        "Your view is returning a QuerySet. QuerySets are not"
                        " really designed to be pickled and can cause caching"
                        " issues. Please materialize the QuerySet using a List"
                        " or some other more primitive data structure."
                    )

            response["Cache-Trace"] = "no-cache"
            response.render()  # should be rendered, before pickling while storing to cache

            if not response.status_code >= 400 or self.cache_errors:
                if self.cache_errors:
                    logger.error(self.cache_errors)
                try:
                    self.cache.set(key, response, self.timeout)
                    response["Cache-Trace"] = "set-cache"
                except Exception:
                    msg = "Problem while writing to cache: path:'{p}' data:'{d}'"
                    logger.exception(msg.format(p=str(request.path), d=str(request.data)))
        else:
            response["Cache-Trace"] = "hit-cache"

        if not hasattr(response, "_closable_objects"):
            response._closable_objects = []

        response["key"] = key
        return response


cache_response = CustomCacheResponse

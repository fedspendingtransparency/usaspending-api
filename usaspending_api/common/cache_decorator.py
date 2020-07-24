# -*- coding: utf-8 -*-
import logging

from django.conf import settings
from django.db.models import QuerySet
from rest_framework_extensions.cache.decorators import CacheResponse
from usaspending_api.common.experimental_api_flags import is_experimental_elasticsearch_api

logger = logging.getLogger("console")


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

            # THIS IS A STOPGAP UNTIL WE GET A CHANCE TO FIX ALL OF THE OFFENDING ENDPOINTS.
            # Subqueries with OuterRefs cannot be pickled currently.  Rather than attempt to locate
            # every single instance of a response meeting this criteria we will force QuerySet
            # materialization by simply replacing {"results": QuerySet} with {"results": List}.
            # This will not solve every case, but should catch the bulk of them.  Any stragglers
            # will need to be fixed by hand...
            if (
                response
                and not response.is_rendered
                and response.data
                and isinstance(response.data, dict)
                and "results" in response.data
                and isinstance(response.data["results"], QuerySet)
            ):
                if settings.IS_LOCAL:
                    raise RuntimeError(
                        "Your view is returning a QuerySet.  QuerySets are not really designed to "
                        "be pickled and can cause caching issues.  Please materialize the QuerySet "
                        "using a List or some other more primitive data structure.  Thank you, have "
                        "a lovely day, and don't forget to wash your hands regularly!"
                    )
                else:
                    response.data["results"] = list(response.data["results"])

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

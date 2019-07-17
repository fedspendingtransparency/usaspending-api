# -*- coding: utf-8 -*-
import logging
from rest_framework_extensions.cache.decorators import CacheResponse

logger = logging.getLogger("console")


class CustomCacheResponse(CacheResponse):
    def process_cache_response(self, view_instance, view_method, request, args, kwargs):
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
            response["Cache-Trace"] = "no-cache"
            response.render()  # should be rendered, before picklining while storing to cache

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

import logging

from django.views.generic import TemplateView
from rest_framework import status, viewsets
from rest_framework.response import Response
from rest_framework.views import APIView
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.mixins import AutocompleteResponseMixin
from usaspending_api.common.api_versioning import removed
from django.utils.decorators import method_decorator

exception_logger = logging.getLogger("exceptions")


class AutocompleteView(AutocompleteResponseMixin, APIView):
    def get_serializer_context(self):
        context = super(AutocompleteView, self).get_serializer_context()
        return {**context}

    @cache_response()
    def post(self, request, *args, **kwargs):
        try:
            response = self.build_response(request, queryset=self.get_queryset(), serializer=self.serializer_class)
            status_code = status.HTTP_200_OK
        except InvalidParameterException as e:
            response = {"message": str(e)}
            status_code = status.HTTP_400_BAD_REQUEST
            exception_logger.exception(e)
        except Exception as e:
            response = {"message": str(e)}
            status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
            exception_logger.exception(e)
        finally:
            return Response(response, status=status_code)


class DetailViewSet(viewsets.ReadOnlyModelViewSet):
    """
    Handles the views for endpoints that request a detailed view of model objects (either in the form of a single
    object or a list of objects).
    """

    def get_serializer_context(self):
        context = super(DetailViewSet, self).get_serializer_context()
        return {**context}

    def list(self, request, *args, **kwargs):
        try:
            # Get the queryset (this will handle filtering and ordering)
            queryset = self.get_queryset()
            # Grab the page of data
            page = self.paginate_queryset(queryset)
            # Serialize the page
            serializer = self.get_serializer(page, read_only=True, many=True)
            # Return the paginated response
            return self.get_paginated_response(serializer.data)
        except InvalidParameterException as e:
            response = {"message": str(e)}
            status_code = status.HTTP_400_BAD_REQUEST
            exception_logger.exception(e)
            return Response(response, status=status_code)
        except Exception as e:
            response = {"message": str(e)}
            status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
            exception_logger.exception(e)
            return Response(response, status=status_code)

    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)


class CachedDetailViewSet(DetailViewSet):
    """
    Caches the responses for some specific views that use DetailViewSet
    """

    @cache_response()
    def list(self, request, *args, **kwargs):
        return super(DetailViewSet, self).list(request, *args, **kwargs)

    @cache_response()
    def retrieve(self, request, *args, **kwargs):
        return super(DetailViewSet, self).retrieve(request, *args, **kwargs)


class MarkdownView(TemplateView):

    template_name = "index.html"
    markdown = ""

    def get_context_data(self, **kwargs):
        # Call the base implementation first to get a context
        context = super(TemplateView, self).get_context_data(**kwargs)
        # Add in the markdown to the context, for use in the template tags
        context["markdown"] = self.markdown
        return context


# placeholder for retired URLs
@method_decorator(removed, name="retrieve")
class RemovedEndpointView(viewsets.ViewSet):
    """Placeholder for endpoints that have been removed"""

    endpoint_doc = "usaspending_api/api_docs/api_documentation/removed_endpoint.md"

    def retrieve(self, request, *args, **kwargs):
        raise Exception("Endpoint failed to return HTTP 410")

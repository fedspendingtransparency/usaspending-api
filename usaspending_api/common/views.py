from rest_framework import viewsets
from rest_framework.response import Response
from rest_framework import status
from rest_framework_extensions.cache.decorators import cache_response
from django.views.generic import TemplateView

from usaspending_api.common.api_request_utils import ResponsePaginator
from usaspending_api.common.serializers import AggregateSerializer
from usaspending_api.common.mixins import AggregateQuerysetMixin

from usaspending_api.common.exceptions import InvalidParameterException

import logging


class AggregateView(AggregateQuerysetMixin,
                    viewsets.ReadOnlyModelViewSet):
    """
    Handles the view for endpoints that request aggregated data.
    The endpoint views inherit from this custom view instead of
    using GenericAPIView directly because we need to handle POST
    requests in addition to GET requests. We may want to re-think
    this in the future.
    """
    serializer_class = AggregateSerializer

    exception_logger = logging.getLogger("exceptions")

    @cache_response()
    def list(self, request, *args, **kwargs):
        """
        Override the parent list method so we can aggregate the data
        before constructing a respones.
        """
        try:
            queryset = self.aggregate(request, *args, **kwargs)

            # get paged data for this request
            paged_data = ResponsePaginator.get_paged_data(
                queryset, request_parameters=request.data)
            paged_queryset = paged_data.object_list.all()

            # construct metadata of entire queryset
            metadata = {"count": paged_data.paginator.count}

            # construct page-specific metadata
            page_metadata = {
                "page_number": paged_data.number,
                "num_pages": paged_data.paginator.num_pages,
                "count": len(paged_data)
            }

            # serialize the paged data
            serializer = self.get_serializer(paged_queryset, many=True)
            serialized_data = serializer.data

            response_object = {
                "total_metadata": metadata,
                "page_metadata": page_metadata
            }
            response_object.update({'results': serialized_data})
            status_code = status.HTTP_200_OK
        except InvalidParameterException as e:
            response_object = {"message": str(e)}
            status_code = status.HTTP_400_BAD_REQUEST
            self.exception_logger.exception(e)
        except Exception as e:
            response_object = {"message": str(e)}
            status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
            self.exception_logger.exception(e)
        finally:
            return Response(response_object, status=status_code)


class DetailViewSet(viewsets.ReadOnlyModelViewSet):
    """
    Handles the views for endpoints that request a detailed
    view of model objects (either in the form of a single
    object or a list of objects).
    """
    # Note: once the front-end has switched to using query parameter
    # pagination for endpoints that use DetailViewSet, we can remove
    # this class and inherit from ReadOnlyModelViewSet directly in
    # the application views.py files.

    exception_logger = logging.getLogger("exceptions")

    @cache_response()
    def list(self, request, *args, **kwargs):
        try:
            response = self.build_response(
                self.request, queryset=self.get_queryset(), serializer=self.get_serializer_class())
            status_code = status.HTTP_200_OK
        except InvalidParameterException as e:
            response = {"message": str(e)}
            status_code = status.HTTP_400_BAD_REQUEST
            self.exception_logger.exception(e)
        except Exception as e:
            response = {"message": str(e)}
            status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
            self.exception_logger.exception(e)
        finally:
            return Response(response, status=status_code)


class MarkdownView(TemplateView):

    template_name = 'index.html'
    markdown = ''

    def get_context_data(self, **kwargs):
        # Call the base implementation first to get a context
        context = super(TemplateView, self).get_context_data(**kwargs)
        # Add in the markdown to the context, for use in the template tags
        context.update({'markdown': self.markdown})
        return context

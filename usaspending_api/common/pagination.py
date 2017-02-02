from collections import OrderedDict

from rest_framework.response import Response
from rest_framework.pagination import PageNumberPagination


class UsaspendingPagination(PageNumberPagination):
    # the default number of items to be returned on a single page
    page_size = 100
    # the name of the query param that can be used to override the
    # default page size
    page_size_query_param = 'limit'
    # the param below sets the max number of items that a request will
    # return, regardless of the page size specified in the request
    max_page_size = 500

    def get_paginated_response(self, data):
        """Override the built-in serializer for paged data."""
        # return Response(OrderedDict([
        #     ('total_count', self.page.paginator.count),
        #     ('page_count', len(self.page)),
        #     ('page_num', self.page.number),
        #     ('page_total', self.page.paginator.num_pages),
        #     ('next', self.get_next_link()),
        #     ('previous', self.get_previous_link()),
        #     ('results', data)
        # ]))

        # note: code below will give us the old metadata format
        return Response({
            'page_metadata': {
                'count': len(self.page),
                'num_pages': self.page.paginator.num_pages,
                'page_number': self.page.number
            },
            'total_metadata': {
                'count': self.page.paginator.count
            },
            'results': data
        })

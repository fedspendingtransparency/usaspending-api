import json

from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response

from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.accounts.serializers import TreasuryAppropriationAccountSerializer
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.accounts.serializers import AppropriationAccountBalancesSerializer
from usaspending_api.common.api_request_utils import FilterGenerator, ResponsePaginator, DataQueryHandler


class TreasuryAppropriationAccountList(APIView):

    def get(self, request):
        """Return a response for an appropriation accounts GET request."""
        taa = TreasuryAppropriationAccount.objects.all()

        fg = FilterGenerator()
        filter_arguments = fg.create_from_get(request.GET)

        taa = taa.filter(**filter_arguments)

        paged_data = ResponsePaginator.get_paged_data(taa, request_parameters=request.GET)

        serializer = TreasuryAppropriationAccountSerializer(paged_data, many=True)
        response_object = {
            "total_metadata": {
                "count": taa.count(),
            },
            "page_metadata": {
                "page_number": paged_data.number,
                "num_pages": paged_data.paginator.num_pages,
                "count": len(paged_data),
            },
            "results": serializer.data
        }

        return Response(response_object)

    def post(self, request):
        """Return a response for an appropriation accounts POST request."""
        try:
            body_unicode = request.body.decode('utf-8')
            body = json.loads(body_unicode)
            dq = DataQueryHandler(
                TreasuryAppropriationAccount,
                TreasuryAppropriationAccountSerializer,
                body)
            response_data = dq.build_response()
        except Exception as e:
            return Response({"message": str(e)}, status=status.HTTP_400_BAD_REQUEST)

        return Response(response_data)


class AppropriationAccountBalancesList(APIView):

    def get(self, request):
        """Return a response for an appropriation account balance GET request."""
        taa = AppropriationAccountBalances.objects.all()

        fg = FilterGenerator()
        filter_arguments = fg.create_from_get(request.GET)

        taa = taa.filter(**filter_arguments)

        paged_data = ResponsePaginator.get_paged_data(taa, request_parameters=request.GET)

        serializer = AppropriationAccountBalancesSerializer(paged_data, many=True)
        response_object = {
            "total_metadata": {
                "count": taa.count(),
            },
            "page_metadata": {
                "page_number": paged_data.number,
                "num_pages": paged_data.paginator.num_pages,
                "count": len(paged_data),
            },
            "results": serializer.data
        }

        return Response(response_object)

    def post(self, request):
        """Return a response for an appropriation account balance POST request."""
        try:
            body_unicode = request.body.decode('utf-8')
            body = json.loads(body_unicode)
            dq = DataQueryHandler(
                AppropriationAccountBalances,
                AppropriationAccountBalancesSerializer,
                body)
            response_data = dq.build_response()
        except Exception as e:
            return Response({"message": str(e)}, status=status.HTTP_400_BAD_REQUEST)

        return Response(response_data)

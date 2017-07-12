from django.db.models import F, Sum

from usaspending_api.awards.models import TransactionContract
from usaspending_api.awards.serializers_v2.serializers import TransactionContractSerializer
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.views import DetailViewSet


class TransactionContractViewSet(DetailViewSet):
    """
        Return PSC and NAICS industry codes, description, and awards for a given fiscal year.
        Note:
            There is no description field for PSC in the database.
            PSC and NAICS have a many-to-many relationship.
            PSC Codes describe “WHAT” was bought for each contract action reported.
            NAICS Codes describe “HOW” purchased products and services will be used.
    """
    serializer_class = TransactionContractSerializer

    def get_queryset(self):
        # retrieve post request payload
        json_request = self.request.query_params
        # retrieve fiscal_year from request
        fiscal_year = json_request.get('fiscal_year', None)
        # required query parameters were not provided
        if not fiscal_year:
            raise InvalidParameterException(
                'Missing one or more required query parameters: fiscal_year'
            )
        queryset = TransactionContract.objects.all()
        # get PSC and NAICS relationships
        queryset = queryset.filter(
            transaction__fiscal_year=fiscal_year
        ).annotate(
            psc=F('product_or_service_code'),
            description=F('naics_description')
        )
        # sum obligations for each PSC & NAICS pair, sort descending
        queryset = queryset.values('psc', 'naics', 'description').annotate(
            obligated_amount=Sum('transaction__federal_action_obligation')
        ).order_by('-obligated_amount')

        return queryset

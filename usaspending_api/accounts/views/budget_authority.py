from django.db.models import Sum
from rest_framework.exceptions import ParseError
from rest_framework.viewsets import ModelViewSet

from usaspending_api.accounts.serializers import BudgetAuthoritySerializer
from usaspending_api.accounts.models import BudgetAuthority
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.views import DetailViewSet


class BudgetAuthorityViewSet(DetailViewSet):
    """Return historical budget authority for a given agency id"""

    serializer_class = BudgetAuthoritySerializer

    def get_queryset(self):
        cgac = self.kwargs['cgac']
        result = BudgetAuthority.objects.filter(federal_account__agency_identifier=cgac) \
            .values('year').annotate(amount=Sum('amount'))
        return result

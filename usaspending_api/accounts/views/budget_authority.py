from django.db.models import Sum

from usaspending_api.accounts.serializers import BudgetAuthoritySerializer
from usaspending_api.accounts.models import BudgetAuthority
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.views import CachedDetailViewSet


class BudgetAuthorityViewSet(CachedDetailViewSet):
    """
    Return historical budget authority for a given agency id.
    """

    serializer_class = BudgetAuthoritySerializer
    ordering_fields = ("year", "total")
    order_directions = {"asc": "", "desc": "-"}

    def sort(self):
        "Generate the order_by string based on query parameters"
        sort_by = self.request.query_params.get("sort", "year").lower()
        if sort_by not in self.ordering_fields:
            raise InvalidParameterException("sort should be one of {}, not {}".format(self.ordering_fields, sort_by))
        order_by = self.request.query_params.get("order", "asc").lower()
        if order_by not in self.order_directions:
            raise InvalidParameterException(
                "order should be {}, not {}".format(" or ".join(self.order_directions), order_by)
            )

        return "{}{}".format(self.order_directions[order_by], sort_by)
        # I tried to use standard DRF OrderingFilter, but failed, maybe
        # do to our project's view customizations?

    def get_queryset(self):
        cgac = self.kwargs["cgac"]
        result = (
            BudgetAuthority.objects.filter(agency_identifier__iexact=cgac).values("year").annotate(total=Sum("amount"))
        )
        result = result.order_by(self.sort())
        frec = self.request.query_params.get("frec", None)
        if frec:
            result = result.filter(fr_entity_code__iexact=frec)
        return result.all()

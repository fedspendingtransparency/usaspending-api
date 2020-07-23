from django.db.models import Q

from usaspending_api.awards.models import FinancialAccountsByAwards


class DefCodes:
    underscore_name = "def_codes"

    @classmethod
    def build_def_codes_filter(cls, queryset, filter_values):
        subquery = FinancialAccountsByAwards.objects.filter(disaster_emergency_fund__in=filter_values).values(
            "award__id"
        )
        q = Q(award_id__in=subquery)
        return q

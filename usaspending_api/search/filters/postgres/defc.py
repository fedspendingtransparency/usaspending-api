from django.db.models import Q

from usaspending_api.awards.models import FinancialAccountsByAwards


class DefCodes:
    underscore_name = "def_codes"

    @classmethod
    def build_def_codes_filter(cls, filter_values):
        """Build a SQL filter to only include Subawards that are associated with any
        DEF codes included in the API request.

        Args:
            filter_values (List[str]): List of DEF codes to use in filtering. These come
                from the API request.

        Returns:
            Q(): Subquery filter containing Award IDs that are associated with the
            provided DEF code filter(s).
        """

        subquery = FinancialAccountsByAwards.objects.filter(disaster_emergency_fund__in=filter_values).values(
            "award__award_id"
        )
        q = Q(award_id__in=subquery)

        return q

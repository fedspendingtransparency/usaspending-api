from django.db.models import OuterRef, Q

from usaspending_api.awards.models import FinancialAccountsByAwards


class DefCodes:
    underscore_name = "def_codes"

    @classmethod
    def build_def_codes_filter(cls, queryset, filter_values):
        """Build a SQL filter to only include Subawards that are associated with any
        DEF codes included in the API request

        Args:
            queryset (Queryset()): The existing queryset that will be added to.
            filter_values (List[str]): List of DEF codes to use in filtering.

        Returns:
            Q(): Subquery filter containing Award IDs that are associated with the
            provided DEF code filter(s).
        """

        # Return the Award IDs where one of these two things is True:
        #   1) The DEF code associated with that Award is in the API request and
        #       has no `earliest_public_law_enactment_date`.
        #   2) The DEF code associated with that Award is in the API request and
        #       the Subaward's action date is on/after the `earliest_public_law_enactment_date`
        subquery = FinancialAccountsByAwards.objects.filter(
            Q(disaster_emergency_fund__earliest_public_law_enactment_date__isnull=True)
            | Q(disaster_emergency_fund__earliest_public_law_enactment_date__lte=OuterRef("sub_action_date")),
            disaster_emergency_fund__in=filter_values,
        ).values("award__award_id")

        q = Q(award_id__in=subquery)
        return q

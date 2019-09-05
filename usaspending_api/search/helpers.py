from django.db.models import Q
from usaspending_api.accounts.helpers import TAS_COMPONENT_TO_FIELD_MAPPING
from usaspending_api.accounts.models import TreasuryAppropriationAccount


def build_tas_codes_filter(queryset, tas_filters):
    """
    Filter the queryset to take advantage of the GIN indexed integer array of
    treasury_appropriation_account treasury_account_identifiers by:

        - Grabbing the list of all treasury_account_identifiers that match the filters provided.
        - Assembling an integer array overlap (&&) query to search for those integers.
    """

    tas_qs = Q()
    for tas_filter in tas_filters:
        tas_qs |= Q(**{TAS_COMPONENT_TO_FIELD_MAPPING[k]: v for k, v in tas_filter.items()})

    # No sense continuing if this resulted in an empty Q statement for whatever reason.
    if not tas_qs:
        return queryset

    return queryset.filter(
        treasury_account_identifiers__overlap=list(
            TreasuryAppropriationAccount
            .objects
            .filter(tas_qs)
            .values_list("treasury_account_identifier", flat=True)
        )
    )

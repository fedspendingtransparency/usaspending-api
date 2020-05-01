from functools import reduce
from operator import ior

from django.db.models import Q
from usaspending_api.accounts.helpers import TAS_COMPONENT_TO_FIELD_MAPPING
from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.common.exceptions import UnprocessableEntityException


def build_award_ids_filter(queryset, values, award_id_fields):
    """
    DEV-3843 asks that, if an award ID is surrounded by quotes, we perform an exact
    match.  To this end, if value is surrounded by quotes, perform the full text search
    for performance reasons, but also filter the results by exact match on piid, fain,
    or uri.  Otherwise, just perform the full text search as we did prior to DEV-3843.

    Note that different searches have different fields that count as award id fields,
    for example, Award has piid, fain, and uri whereas Subaward only has piid and fain.
    To account for this, the caller will need to provide the fields they want searched.
    """
    award_id_filter = Q()
    for value in values:
        if value and value.startswith('"') and value.endswith('"'):
            value = value[1:-1]  # Strip off the quotes.
            ors = (Q(**{f: value}) for f in award_id_fields)
            award_id_filter |= Q(award_ts_vector=value) & (reduce(ior, ors))
        else:
            award_id_filter |= Q(award_ts_vector=value)
    return queryset.filter(award_id_filter) if award_id_filter else queryset


def build_tas_codes_filter(queryset, tas_filters):
    """
    Filter the queryset to take advantage of the GIN indexed integer array of
    treasury_appropriation_account treasury_account_identifiers by:

        - Grabbing the list of all treasury_account_identifiers that match the filters provided.
        - Assembling an integer array overlap (&&) query to search for those integers.
    """
    if isinstance(tas_filters, dict):
        raise UnprocessableEntityException("Heirarchical TAS filters not supported in Postgres implementation")

    tas_qs = Q()
    for tas_filter in tas_filters:
        tas_qs |= Q(**{TAS_COMPONENT_TO_FIELD_MAPPING[k]: v for k, v in tas_filter.items()})

    # No sense continuing if this resulted in an empty Q statement for whatever reason.
    if not tas_qs:
        return queryset

    return queryset.filter(
        treasury_account_identifiers__overlap=list(
            TreasuryAppropriationAccount.objects.filter(tas_qs).values_list("treasury_account_identifier", flat=True)
        )
    )

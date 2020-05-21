from functools import reduce
from operator import ior

from django.db.models import Q


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

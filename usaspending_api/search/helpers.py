from django.db.models import Q
from usaspending_api.accounts.helpers import TAS_COMPONENT_TO_FIELD_MAPPING
from usaspending_api.accounts.models import TASAutocompleteMatview


def build_tas_codes_filter(queryset, model, tas_filters):
    """
    We're using full text indexes to find TAS.  To query these, we will need
    to grab all of the valid TAS for the provided filter, construct a full text
    query string, then perform a full text search.
    """

    # Build the query to grab all valid TASes for the provided filters.
    where = Q()
    for tas_filter in tas_filters:
        where |= Q(**{TAS_COMPONENT_TO_FIELD_MAPPING[k]: v for k, v in tas_filter.items()})

    if where:

        # Build a full text OR query (any one of the TASes can match).  This is
        # done by concatenating the TASes together using pipes.
        tas_query = "|".join(TASAutocompleteMatview.objects.filter(where).values_list("tas_rendering_label", flat=True))

        # Perform a full text query.  To do this, we will simply cast the query
        # to tsquery.  Doing this performs no pre-processing on the query string.
        # If we were to run it through one of the full text query functions (for
        # example to_tsquery) much pre-processing will occur including removing
        # punctuation which is a problem for TASes.
        return queryset.extra(
            where=['"{}"."tas_ts_vector" @@ %s::tsquery'.format(model._meta.db_table)],
            params=[tas_query]
        )

    return queryset

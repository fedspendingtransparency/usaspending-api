from django.db.models import Q
from usaspending_api.accounts.helpers import TAS_COMPONENT_TO_FIELD_MAPPING
from usaspending_api.accounts.models import TASAutocompleteMatview, TASSearchMatview
from usaspending_api.common.helpers.orm_helpers import generate_where_clause


def build_tas_codes_filter(queryset, model, tas_filters):
    """
    We're using full text indexes to find TAS.  To query these, we will need
    to grab all of the valid TAS for the provided filter, construct a full text
    query string, then perform a full text search.
    """

    # Build a queryset filter that contains all of the TAS request filters.
    where = Q()
    for tas_filter in tas_filters:
        where |= Q(**{TAS_COMPONENT_TO_FIELD_MAPPING[k]: v for k, v in tas_filter.items()})

    if where:

        # Build a full text OR query (any one of the TASes can match).  This is
        # done by concatenating the TASes together using pipes.
        tas_query = "|".join(TASAutocompleteMatview.objects.filter(where).values_list("tas_rendering_label", flat=True))
        tas_queryset = TASSearchMatview.objects.extra(
            where=['"{}"."tas_ts_vector" @@ %s::tsquery'.format(TASSearchMatview._meta.db_table)],
            params=[tas_query]
        )

        # Now that we've built the actual queryset filter, let's turn it into SQL
        # that we can provide to the queryset.extra method.  We do this by converting
        # the queryset to raw SQL and nabbing the where clause.
        where_sql, where_params = generate_where_clause(tas_queryset)
        return queryset.extra(
            tables=[TASSearchMatview._meta.db_table],
            where=[
                '"{}".award_id = "{}".award_id'.format(TASSearchMatview._meta.db_table, model._meta.db_table),
                where_sql,
            ],
            params=where_params
        )

    return queryset

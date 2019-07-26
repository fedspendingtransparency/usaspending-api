from django.db.models import Q
from usaspending_api.accounts.helpers import TAS_COMPONENT_TO_FIELD_MAPPING
from usaspending_api.accounts.models import TASAwardMatview
from usaspending_api.common.helpers.orm_helpers import generate_where_clause


def build_tas_codes_filter(queryset, model, value):
    """
    Build the TAS codes filter.  Because of performance issues, the normal
    trick of checking for award_id in a subquery wasn't cutting it.  To
    work around this, we're going to use the query.extra function to add SQL
    that should give us a better query plan.
    """

    # Build the filtering for the tas_award_matview.
    or_queryset = Q()
    for tas in value:
        or_queryset |= Q(**{TAS_COMPONENT_TO_FIELD_MAPPING[k]: v for k, v in tas.items()})

    if or_queryset:
        # Now that we've built the actual filter, let's turn it into SQL that we
        # can provide to the queryset.extra method.  We do this by converting the
        # queryset to raw SQL.
        where_sql, where_params = generate_where_clause(TASAwardMatview.objects.filter(or_queryset))
        return queryset.extra(
            tables=[TASAwardMatview._meta.db_table],
            where=[
                '"{}".award_id = "{}".award_id'.format(TASAwardMatview._meta.db_table, model._meta.db_table),
                where_sql,
            ],
            params=where_params
        )

    return queryset

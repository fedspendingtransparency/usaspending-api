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

    # If we found any TASes, construct the full text search string.  This
    # construction has to match the manner in which tas_ts_vector is built in
    # the materialized views.  It should resemble something like:
    #
    #   072_019_2013_2018__1031_000|075_019_2018_2022__1031_001
    #
    # Where underscores are used to separate the 7 TAS components.  Pipes are
    # used to separate the TASes as this is the syntax for OR in Postgres full
    # text jargon.
    if where:
        tases = "|".join(
            "{}_{}_{}_{}_{}_{}_{}".format(
                tas.allocation_transfer_agency_id or "",
                tas.agency_id or "",
                tas.beginning_period_of_availability or "",
                tas.ending_period_of_availability or "",
                tas.availability_type_code or "",
                tas.main_account_code or "",
                tas.sub_account_code or "",
            )
            for tas in TASAutocompleteMatview.objects.filter(where)
        )
        return queryset.extra(
            where=['"{}"."tas_ts_vector" @@ %s::tsquery'.format(model._meta.db_table)],
            params=[tases]
        )

    return queryset

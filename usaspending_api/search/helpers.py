from django.db.models import Q
from usaspending_api.accounts.helpers import TAS_COMPONENT_TO_FIELD_MAPPING
from usaspending_api.accounts.models import TreasuryAppropriationAccount


def build_tas_codes_filter(queryset, model, tas_filters):
    """Append a performant predicate using Django's extra to a queryset

    The materialized views contain a GIN indexed integer array of
    treasury_appropriation_account.treasury_account_identifiers.

    Step 1)  Get the list of all treasury_account_identifiers that match the filters provided.
    Step 2)  Assemble an integer array overlap (&&) query to search for those integers.
    """

    tas_qs = Q()
    for tas_filter in tas_filters:
        tas_qs |= Q(**{TAS_COMPONENT_TO_FIELD_MAPPING[k]: v for k, v in tas_filter.items()})

    # No sense continuing if this resulted in an empty Q statement for whatever reason.
    if not tas_qs:
        return queryset

    # Concatenate the list of integer treasury_account_identifiers into a string
    # suitable for a PostgreSQL integer array.
    tas_ids = TreasuryAppropriationAccount.objects.filter(tas_qs).values_list("treasury_account_identifier", flat=True)
    tas_sql_array = "ARRAY[{}]::integer[]".format(",".join([str(i) for i in tas_ids]))

    # Craft a where clause that performs an array overlap search (&&) on the
    # treasury_account_identifiers column.
    where = '"{}"."treasury_account_identifiers" && {}'.format(model._meta.db_table, tas_sql_array)

    return queryset.extra(where=[where])

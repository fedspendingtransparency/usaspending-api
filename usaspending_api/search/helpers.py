from django.db.models import Q
from usaspending_api.accounts.helpers import TAS_COMPONENT_TO_FIELD_MAPPING
from usaspending_api.accounts.models import TreasuryAppropriationAccount


def build_tas_codes_filter(queryset, model, tas_filters):
    """
    We're using a GIN indexed array of treasury_appropriation_account.treasury_account_identifiers
    to filter awards/transactions by TAS/FA.

        Step one)  Get the list of all treasury_account_identifiers that match the filters provided.
        Step bee)  Assemble an integer array overlap (&&) query to search for those integers.
    """

    # Build a queryset filter that contains all of the TAS request filters.
    where = Q()
    for tas_filter in tas_filters:
        where |= Q(**{TAS_COMPONENT_TO_FIELD_MAPPING[k]: v for k, v in tas_filter.items()})

    # No sense continuing if this resulted in an empty Q statement for whatever reason.
    if where:

        # Concatenate the list of integer treasury_account_identifiers into a string suitable for
        # stuffing into a PostgreSQL integer array.
        where = ",".join([
            str(i) for i in
            TreasuryAppropriationAccount.objects.filter(where).values_list("treasury_account_identifier", flat=True)
        ])

        # Craft a where clause that performs an array overlap search (&&) on the
        # treasury_account_identifiers column.
        where = '"%s"."treasury_account_identifiers" && ARRAY[%s]::integer[]' % (model._meta.db_table, where)

        # Finally, let's graft all of this nonsense into the provided queryset.
        return queryset.extra(where=[where])

    return queryset

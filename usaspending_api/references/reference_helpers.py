from django.db import connection
from django.db.models.functions import Coalesce
from django.db.models import Value

from usaspending_api.accounts.models import FederalAccount, TreasuryAppropriationAccount


def remove_empty_federal_accounts(tas_tuple=None):
    """
    Removes federal accounts who are no longer attached to a TAS
    """
    returns = FederalAccount.objects.filter(treasuryappropriationaccount__isnull=True).distinct().delete()
    return returns[0]


def update_federal_accounts(tas_tuple=None):
    """
    Update existing federal account records based on the latest information
    from the TreasuryAppropriationAccount (TAS) table. The account title
    for each federal account should reflect the account title of the
    a related TAS with the most recent ending period of availability.

    Returns:
        Number of rows updated
    """

    # add federal_account_code
    queryset = FederalAccount.objects.all()
    queryset = queryset.filter(federal_account_code=None)
    for fa in queryset:
        # fa.save() is overwritten to add federal_account_code
        fa.save()

    # Because orm doesn't support join updates, dropping down to raw SQL
    # to update existing federal_account FK relationships on the tas table.
    tas_fk_sql = " ".join(
        [
            "UPDATE treasury_appropriation_account t",
            "SET federal_account_id = fa.id",
            "FROM federal_account fa",
            "WHERE t.agency_id = fa.agency_identifier",
            "AND t.main_account_code = fa.main_account_code",
            "AND t.treasury_account_identifier IN %s" if tas_tuple is not None else "",
        ]
    )

    with connection.cursor() as cursor:
        cursor.execute(tas_fk_sql, [tas_tuple])
        rows = cursor.rowcount

    return rows


def insert_federal_accounts():
    """
    Insert new federal accounts records based on the TreasuryAppropriationAccount
    (TAS) table. Each TAS maps to a higher-level federal account, defined
    by a unique combination of TAS agency_id (AID) and TAS main account
    code (MAC).
    """

    # look for treasury_appropriation_accounts with no federal_account FK
    tas_no_federal_account = TreasuryAppropriationAccount.objects.filter(federal_account__isnull=True)

    if tas_no_federal_account.count() > 0:
        # there are tas records with no corresponding federal_account,
        # so insert the necessary federal_account records
        # to get the federal accounts title, we use the title from the tas
        # with the most recent ending period of availability (which is
        # coalesced to a string to ensure the descending sort works as expected)
        federal_accounts = (
            TreasuryAppropriationAccount.objects.values_list("agency_id", "main_account_code", "account_title")
            .annotate(epoa=Coalesce("ending_period_of_availability", Value("")))
            .distinct("agency_id", "main_account_code")
            .order_by("agency_id", "main_account_code", "-epoa")
            .filter(treasury_account_identifier__in=tas_no_federal_account)
        )

        # create a list of the new federal account objects and bulk insert them
        fa_objects = [
            FederalAccount(
                agency_identifier=f[0] or "",
                main_account_code=f[1] or "",
                account_title=f[2] or "",
                federal_account_code="{}-{}".format(f[0] or "", f[1] or ""),
            )
            for f in federal_accounts
        ]
        FederalAccount.objects.bulk_create(fa_objects)

        # now that the new account records are inserted, add federal_account
        # FKs to their corresponding treasury_appropriation_account records
        tas_update_list = [t.treasury_account_identifier for t in tas_no_federal_account]
        update_federal_accounts(tuple(tas_update_list))
        return len(fa_objects)
    else:
        return 0

from django.db import connection
from usaspending_api.common.etl.postgres import ETLQuery, ETLTable
from usaspending_api.common.etl.postgres.operations import (
    delete_obsolete_rows,
    insert_missing_rows,
    update_changed_rows,
)


# This is basically the desired final state of the federal_account table.  We can diff this against the
# actual federal_account table and make corrections as appropriate to bring the federal_account table
# into line.  Since the treasury_appropriation_account and federal_account tables are fairly small, we
# can perform full diffs with no noticeable performance impact.  This sort order is dictated by DEV-3495.
FEDERAL_ACCOUNTS_FROM_TREASURY_ACCOUNTS_SQL = """
    select
        distinct on (agency_id, main_account_code)
        agency_id as agency_identifier,
        main_account_code,
        concat(agency_id, '-', main_account_code) as federal_account_code,
        account_title
    from
        treasury_appropriation_account
    order by
        agency_id,
        main_account_code,
        beginning_period_of_availability desc nulls last,
        ending_period_of_availability desc nulls last,
        sub_account_code,
        allocation_transfer_agency_id,
        treasury_account_identifier desc
"""


source_federal_account_query = ETLQuery(FEDERAL_ACCOUNTS_FROM_TREASURY_ACCOUNTS_SQL)
destination_federal_account_table = ETLTable(
    "federal_account", key_overrides=["agency_identifier", "main_account_code"]
)


def remove_empty_federal_accounts():
    """
    Removes federal accounts that are no longer attached to a TAS.

    Returns:
        Number of rows updated
    """
    return delete_obsolete_rows(source_federal_account_query, destination_federal_account_table)


def update_federal_accounts():
    """
    Update existing federal account records based on the latest information
    from the TreasuryAppropriationAccount (TAS) table. The account title
    for each federal account should reflect the account title of the
    a related TAS with the most recent beginning period of availability.

    Returns:
        Number of rows updated
    """
    return update_changed_rows(source_federal_account_query, destination_federal_account_table)


def insert_federal_accounts():
    """
    Insert new federal accounts records based on the TreasuryAppropriationAccount
    (TAS) table. Each TAS maps to a higher-level federal account, defined
    by a unique combination of TAS agency_id (AID) and TAS main account
    code (MAC).
    """
    return insert_missing_rows(source_federal_account_query, destination_federal_account_table)


def link_treasury_accounts_to_federal_accounts():
    """
    Federal accounts are derived from AID (agency identifier) + MAIN (main account code) in treasury accounts.
    Using this information, we can link treasury accounts to their corresponding federal account and correct
    any accounts that may be mis-linked.  Since these tables are relatively small, we can simply perform full
    updates with little to no noticeable performance impact.
    """
    with connection.cursor() as cursor:
        cursor.execute(
            """
                update  treasury_appropriation_account as tu
                set     federal_account_id = fa.id
                from    treasury_appropriation_account as t
                        left outer join federal_account as fa on
                            t.agency_id = fa.agency_identifier and
                            t.main_account_code = fa.main_account_code
                where   tu.treasury_account_identifier = t.treasury_account_identifier and
                        tu.federal_account_id is distinct from fa.id;
            """
        )
    return cursor.rowcount

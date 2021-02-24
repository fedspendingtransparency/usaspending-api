from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.etl.broker_etl_helpers import dictfetchall


# This dictionary will hold a map of tas_id -> treasury_account to ensure we don't keep hitting the
# Broker DB for account data.
TAS_ID_TO_ACCOUNT = {}


def bulk_treasury_appropriation_account_tas_lookup(rows, db_cursor):

    # Eliminate nulls, TAS we already know about, and remove duplicates.
    tas_lookup_ids = tuple(set(r["tas_id"] for r in rows if (r["tas_id"] and r["tas_id"] not in TAS_ID_TO_ACCOUNT)))

    if not tas_lookup_ids:
        return

    db_cursor.execute(
        """
            select  distinct
                    account_num,
                    allocation_transfer_agency,
                    agency_identifier,
                    availability_type_code,
                    beginning_period_of_availa,
                    ending_period_of_availabil,
                    main_account_code,
                    sub_account_code
            from    tas_lookup
            where   account_num in %s
                    and financial_indicator2 IS DISTINCT FROM 'F'
        """,
        [tas_lookup_ids],
    )
    tas_data = dictfetchall(db_cursor)

    tas_rendering_labels = {
        tas["account_num"]: TreasuryAppropriationAccount.generate_tas_rendering_label(
            ata=tas["allocation_transfer_agency"],
            aid=tas["agency_identifier"],
            typecode=tas["availability_type_code"],
            bpoa=tas["beginning_period_of_availa"],
            epoa=tas["ending_period_of_availabil"],
            mac=tas["main_account_code"],
            sub=tas["sub_account_code"],
        )
        for tas in tas_data
    }

    taa_objects = {
        taa.tas_rendering_label: taa
        for taa in TreasuryAppropriationAccount.objects.filter(tas_rendering_label__in=tas_rendering_labels.values())
    }

    TAS_ID_TO_ACCOUNT.update(
        {tid: (taa_objects.get(tas_rendering_labels.get(tid)), tas_rendering_labels.get(tid)) for tid in tas_lookup_ids}
    )


def get_treasury_appropriation_account_tas_lookup(tas_lookup_id):
    tas = TAS_ID_TO_ACCOUNT.get(tas_lookup_id)
    if not tas or not tas[1]:
        return None, f"TAS Account Number (tas_lookup.account_num) '{tas_lookup_id}' not found in Broker"
    return tas

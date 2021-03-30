from usaspending_api.common.helpers.sql_helpers import execute_dml_sql
from usaspending_api.etl.operations.federal_account.update_agency import DOD_AID, DOD_SUBSUMED_AIDS
from usaspending_api.etl.operations.federal_account.update_agency import FEDERAL_ACCOUNT_PARENT_AGENCY_MAPPING


def update_treasury_appropriation_account_agencies():
    """
    Updates the awarding and funding toptier agencies in the TreasuryAppropriationAccount table.

    For ATA:
        - If awarding_toptier_agency_id is for a DOD subsumed agency (Army, Navy, Air Force), remap it to 097 (DOD).
        - Otherwise attempt to look up the awarding agency by allocation_transfer_agency_id (ATA) CGAC.  Nothing else
          we can really do here.

    For AID:
        - At the request of the PO, in order to reduce the amount of one-off logic caused by several edge cases
          we bucket all treasury accounts in the same bucket as their "parent" federal account.  This will also
          make the filter tree control experience better as we should no longer end up with some outlying
          treasury accounts not falling under their apparent parent federal account in the tree control.
        - Please look at the FEDERAL_ACCOUNT_PARENT_AGENCY_MAPPING documentation for more details.
    """
    sql = f"""
        with
        ata_mapping as (
            select
                taa.treasury_account_identifier,
                ata.toptier_agency_id as awarding_toptier_agency_id
            from
                treasury_appropriation_account as taa
                left outer join toptier_agency as ata on
                    ata.toptier_code = case
                        when taa.allocation_transfer_agency_id in {DOD_SUBSUMED_AIDS} then '{DOD_AID}'
                        else taa.allocation_transfer_agency_id
                    end
        ),
        aid_mapping as (
            {FEDERAL_ACCOUNT_PARENT_AGENCY_MAPPING}
        )
        update
            treasury_appropriation_account as taa
        set
            awarding_toptier_agency_id = ata_mapping.awarding_toptier_agency_id,
            funding_toptier_agency_id = aid_mapping.parent_toptier_agency_id
        from
            ata_mapping,
            aid_mapping
        where
            ata_mapping.treasury_account_identifier = taa.treasury_account_identifier and
            aid_mapping.agency_identifier = taa.agency_id and
            aid_mapping.main_account_code = taa.main_account_code and (
                ata_mapping.awarding_toptier_agency_id is distinct from taa.awarding_toptier_agency_id or
                aid_mapping.parent_toptier_agency_id is distinct from taa.funding_toptier_agency_id
            )
    """

    return execute_dml_sql(sql)

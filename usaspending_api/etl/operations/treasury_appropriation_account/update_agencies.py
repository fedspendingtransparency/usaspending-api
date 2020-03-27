from django.db import connections, DEFAULT_DB_ALIAS
from usaspending_api.references.constants import DOD_CGAC, DOD_SUBSUMED_CGAC


def update_treasury_appropriation_account_agencies():
    """
    Updates the awarding and funding toptier agencies in the TreasuryAppropriationAccount table.

    For ATA:
        - If awarding_toptier_agency_id is for a DOD subsumed agency (Army, Navy, Air Force), remap it to 097 (DOD).
        - Otherwise attempt to look up the awarding agency by awarding_toptier_agency_id.  Nothing else we can
          really do here.

    For AID:
        - If agency_id is for a DOD subsumed agency (Army, Navy, Air Force), remap it to 097 (DOD).
        - If agency_id is a valid CGAC toptier agency, map it to toptier agency using the agency_id.
        - If fr_entity_code is a valid FREC toptier agency, map it to toptier agency using the fr_entity_code.
        - If agency_id is a shared toptier agency (011, 033, etc), use the frec_agency_cgac_association field in
          the frec table to find the correct agency for the TAS.
    """
    subsumed = tuple(DOD_SUBSUMED_CGAC)
    sql = f"""
        with
        expected_agency_mapping as (
            select
                taa.treasury_account_identifier,
                ata.toptier_agency_id as awarding_toptier_agency_id,
                coalesce(
                    aid_cgac.toptier_agency_id,
                    aid_frec.toptier_agency_id,
                    frec_association.toptier_agency_id
                ) as funding_toptier_agency_id
            from
                treasury_appropriation_account as taa
                left outer join toptier_agency as ata on
                    ata.toptier_code = case
                        when taa.allocation_transfer_agency_id in {subsumed} then '{DOD_CGAC}'
                        else taa.allocation_transfer_agency_id
                    end
                left outer join toptier_agency as aid_cgac on
                    aid_cgac.toptier_code = case
                        when taa.agency_id in {subsumed} then '{DOD_CGAC}'
                        else taa.agency_id
                    end
                left outer join toptier_agency as aid_frec on
                    aid_frec.toptier_code = taa.fr_entity_code
                left outer join frec on
                    frec.frec_code = taa.fr_entity_code
                left outer join toptier_agency as frec_association on
                    frec_association.toptier_code = frec.frec_agency_cgac_association
        )
        update
            treasury_appropriation_account as taa
        set
            awarding_toptier_agency_id = e.awarding_toptier_agency_id,
            funding_toptier_agency_id = e.funding_toptier_agency_id
        from
            expected_agency_mapping as e
        where
            taa.treasury_account_identifier = e.treasury_account_identifier and (
                taa.awarding_toptier_agency_id is distinct from e.awarding_toptier_agency_id or
                taa.funding_toptier_agency_id is distinct from e.funding_toptier_agency_id
            )
    """

    connection = connections[DEFAULT_DB_ALIAS]
    with connection.cursor() as cursor:
        cursor.execute(sql)
        return cursor.rowcount

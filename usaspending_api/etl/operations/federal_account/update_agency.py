from django.db import connections, DEFAULT_DB_ALIAS
from usaspending_api.references.constants import DOD_CGAC, DOD_SUBSUMED_CGAC


def update_federal_account_agency():
    """
    Updates the parent toptier agency in the FederalAccount table.  This function duplicates some of the logic
    from usaspending_api/etl/operations/treasury_appropriation_account/update_agencies.py because we do not want
    to assume the treasury_appropriation_account agencies have already been mapped so we recalculate that bit
    here.

    The high level logic is basically to pick the most common funding agency for all the treasury accounts
    associated with a federal account.  At a lower level that logic translates to:

        - If agency_id is for a DOD subsumed agency (Army, Navy, Air Force), remap it to 097 (DOD).
        - If agency_id is a valid CGAC toptier agency, map it to toptier agency using the agency_id.
        - If fr_entity_code is a valid FREC toptier agency, map it to toptier agency using the fr_entity_code.
        - If agency_id is a shared toptier agency (011, 033, etc), choose the most common funding agency amongst
          treasury accounts belonging to the federal account.
    """
    subsumed = tuple(DOD_SUBSUMED_CGAC)
    sql = f"""
        with
        expected_agency_mapping as (
            select distinct on (taa.federal_account_id)
                taa.federal_account_id as id,
                coalesce(
                    aid_cgac.toptier_agency_id,
                    aid_frec.toptier_agency_id,
                    frec_association.toptier_agency_id
                ) as parent_toptier_agency_id
            from
                treasury_appropriation_account as taa
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
            group by
                taa.federal_account_id,
                parent_toptier_agency_id
            order by
                taa.federal_account_id,
                count(*) desc,
                parent_toptier_agency_id
        )
        update
            federal_account as fa
        set
            parent_toptier_agency_id = e.parent_toptier_agency_id
        from
            expected_agency_mapping as e
        where
            fa.id = e.id and
            fa.parent_toptier_agency_id is distinct from e.parent_toptier_agency_id;
    """

    connection = connections[DEFAULT_DB_ALIAS]
    with connection.cursor() as cursor:
        cursor.execute(sql)
        return cursor.rowcount

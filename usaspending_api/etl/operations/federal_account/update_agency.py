from usaspending_api.common.helpers.sql_helpers import execute_dml_sql


DOD_AID = "097"  # DOD's agency identifier.
DOD_SUBSUMED_AIDS = ("017", "021", "057")  # Air Force, Army, and Navy are to be reported under DOD.


# 1. If agency_id is for a DOD subsumed agency (Army, Navy, Air Force), remap it to 097 (DOD).
# 2. If agency_id is a valid CGAC toptier agency, map it to toptier agency using the agency_id.
# 3. If fr_entity_code is a valid FREC toptier agency, map it to toptier agency using the fr_entity_code.
# 4. If agency_id is a shared toptier agency (011, 033, etc) and does not have a toptier FREC, use
#    the associated_cgac_code to chose the toptier code.
# 5. In cases where we have multiple TAS with conflicting agencies, agency selection priority is
#    determined by the number of File A submissions for the agency followed by the number of TAS
#    associated with the agency.  In the event of ties, the smallest agency code value is chosen.  The
#    tiebreaker is largely irrelevant as it is just used to ensure deterministic results.
FEDERAL_ACCOUNT_PARENT_AGENCY_MAPPING = f"""
            select distinct on (taa.agency_id, taa.main_account_code)
                taa.agency_id as agency_identifier,
                taa.main_account_code,
                coalesce(
                    aid_cgac.toptier_agency_id,
                    aid_frec.toptier_agency_id,
                    aid_association.toptier_agency_id
                ) as parent_toptier_agency_id,
                coalesce(
                    aid_cgac.toptier_code,
                    aid_frec.toptier_code,
                    aid_association.toptier_code
                )::int as toptier_code_sorter
            from
                treasury_appropriation_account as taa
                left outer join toptier_agency as aid_cgac on
                    aid_cgac.toptier_code = case
                        when taa.agency_id in {DOD_SUBSUMED_AIDS} then '{DOD_AID}'
                        else taa.agency_id
                    end
                left outer join toptier_agency as aid_frec on
                    aid_frec.toptier_code = taa.fr_entity_code
                left outer join frec on
                    frec.frec_code = taa.fr_entity_code
                left outer join toptier_agency as aid_association on
                    aid_association.toptier_code = frec.associated_cgac_code
                left outer join appropriation_account_balances aab on
                    aab.treasury_account_identifier = taa.treasury_account_identifier
            group by
                taa.agency_id,
                taa.main_account_code,
                parent_toptier_agency_id,
                toptier_code_sorter
            order by
                taa.agency_id,
                taa.main_account_code,
                count(distinct aab.appropriation_account_balances_id) desc,
                count(distinct taa.treasury_account_identifier) desc,
                toptier_code_sorter
"""


def update_federal_account_agency():

    sql = f"""
        with
        federal_account_parent_agency_mapping as (
            {FEDERAL_ACCOUNT_PARENT_AGENCY_MAPPING}
        )
        update
            federal_account as fa
        set
            parent_toptier_agency_id = fapam.parent_toptier_agency_id
        from
            federal_account_parent_agency_mapping as fapam
        where
            fapam.agency_identifier = fa.agency_identifier and
            fapam.main_account_code = fa.main_account_code and
            fapam.parent_toptier_agency_id is distinct from fa.parent_toptier_agency_id
    """

    return execute_dml_sql(sql)

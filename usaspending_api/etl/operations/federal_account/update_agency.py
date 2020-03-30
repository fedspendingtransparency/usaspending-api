from usaspending_api.common.helpers.sql_helpers import execute_dml_sql
from usaspending_api.references.constants import DOD_CGAC, DOD_SUBSUMED_CGAC


# - If agency_id is for a DOD subsumed agency (Army, Navy, Air Force), remap it to 097 (DOD).
# - If agency_id is a valid CGAC toptier agency, map it to toptier agency using the agency_id.
# - If fr_entity_code is a valid FREC toptier agency, map it to toptier agency using the fr_entity_code.
# - If agency_id is a shared toptier agency (011, 033, etc), choose the most common funding agency amongst
#   treasury accounts belonging to the federal account using the associated_cgac.
FEDERAL_ACCOUNT_PARENT_AGENCY_MAPPING = f"""
            select distinct on (taa.agency_id, taa.main_account_code)
                taa.agency_id as agency_identifier,
                taa.main_account_code,
                coalesce(
                    aid_cgac.toptier_agency_id,
                    aid_frec.toptier_agency_id,
                    aid_association.toptier_agency_id
                ) as parent_toptier_agency_id
            from
                treasury_appropriation_account as taa
                left outer join toptier_agency as aid_cgac on
                    aid_cgac.toptier_code = case
                        when taa.agency_id in {tuple(DOD_SUBSUMED_CGAC)} then '{DOD_CGAC}'
                        else taa.agency_id
                    end
                left outer join toptier_agency as aid_frec on
                    aid_frec.toptier_code = taa.fr_entity_code
                left outer join frec on
                    frec.frec_code = taa.fr_entity_code
                left outer join toptier_agency as aid_association on
                    aid_association.toptier_code = frec.associated_cgac
            group by
                taa.agency_id,
                taa.main_account_code,
                parent_toptier_agency_id
            order by
                taa.agency_id,
                taa.main_account_code,
                count(*) desc,
                parent_toptier_agency_id
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

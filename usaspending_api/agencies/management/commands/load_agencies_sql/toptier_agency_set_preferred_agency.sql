/*
    We link treasury_appropriation_account records to toptier_agency via the ill named cgac_code.
    Unfortunately, we require an agency.id to display an agency on the website.  It is not trivial
    to retrieve an agency.id if all we have is a toptier agency, so to simplify downstream code we
    denormalize that link into preferred_agency_id.

    Know that every toptier agency is represented in the agency table as either having an equivalent
    subtier or no subtier in cases where they have no equivalent subtier.

    An equivalent subtier is a subtier code that represents the toptier agency as a whole and is
    therefore basically equivalent to the toptier.  Not all agencies have equivalent subtiers.  We
    denote this by setting the toptier_flag to true for the subtier.
*/
with cte as (
    select
        ta.toptier_agency_id,
        coalesce(
            (
                -- Get agency where we have a subtier agency that corresponds to the toptier agency (toptier_flag).
                select  id
                from    agency
                where   toptier_agency_id = ta.toptier_agency_id and
                        toptier_flag is true
                order   by id desc
                limit   1
            ),
            (
                -- Get agency where we have a toptier agency but no subtier.
                select  id
                from    agency
                where   toptier_agency_id = ta.toptier_agency_id and
                        subtier_agency_id is null
                order   by id desc
                limit   1
            )
        ) as preferred_agency_id
    from
        toptier_agency ta
)
update
    toptier_agency as ta
set
    preferred_agency_id = cte.preferred_agency_id
from
    cte
where
    cte.toptier_agency_id = ta.toptier_agency_id and
    cte.preferred_agency_id is distinct from ta.preferred_agency_id;

-- subawards are linked to agencies when they're created.  This will fix any
-- incorrect links caused by changes to agencies.
update
    subaward_search as s1

set
    awarding_agency_id = a.awarding_agency_id,
    awarding_subtier_agency_abbreviation = asa.abbreviation,
    awarding_subtier_agency_name = asa.name,
    awarding_toptier_agency_abbreviation = ata.abbreviation,
    awarding_toptier_agency_name = ata.name,

    funding_agency_id = a.funding_agency_id,
    funding_subtier_agency_abbreviation = fsa.abbreviation,
    funding_subtier_agency_name = fsa.name,
    funding_toptier_agency_abbreviation = fta.abbreviation,
    funding_toptier_agency_name = fta.name

from
    subaward_search as s
    left outer join vw_awards as a on a.id = s.award_id

    left outer join agency as aa on aa.id = a.awarding_agency_id
    left outer join subtier_agency as asa on asa.subtier_agency_id = aa.subtier_agency_id
    left outer join toptier_agency as ata on ata.toptier_agency_id = aa.toptier_agency_id

    left outer join agency as fa on fa.id = a.funding_agency_id
    left outer join subtier_agency as fsa on fsa.subtier_agency_id = fa.subtier_agency_id
    left outer join toptier_agency as fta on fta.toptier_agency_id = fa.toptier_agency_id

where
    s.broker_subaward_id = s1.broker_subaward_id and (
        s.awarding_agency_id is distinct from a.awarding_agency_id or
        s.awarding_subtier_agency_abbreviation is distinct from asa.abbreviation or
        s.awarding_subtier_agency_name is distinct from asa.name or
        s.awarding_toptier_agency_abbreviation is distinct from ata.abbreviation or
        s.awarding_toptier_agency_name is distinct from ata.name or

        s.funding_agency_id is distinct from a.funding_agency_id or
        s.funding_subtier_agency_abbreviation is distinct from fsa.abbreviation or
        s.funding_subtier_agency_name is distinct from fsa.name or
        s.funding_toptier_agency_abbreviation is distinct from fta.abbreviation or
        s.funding_toptier_agency_name is distinct from fta.name
    );

-- Enhance subawards with awarding agency.



update
    temp_load_subawards_subaward

set
    awarding_subtier_agency_abbreviation = saa.abbreviation,
    awarding_subtier_agency_name = saa.name,
    awarding_toptier_agency_abbreviation = taa.abbreviation,
    awarding_toptier_agency_name = taa.name

from
    agency aa
    left outer join toptier_agency as taa on taa.toptier_agency_id = aa.toptier_agency_id
    left outer join subtier_agency as saa on saa.subtier_agency_id = aa.subtier_agency_id

where
    aa.id = temp_load_subawards_subaward.awarding_agency_id and
    temp_load_subawards_subaward.awarding_agency_id is not null;

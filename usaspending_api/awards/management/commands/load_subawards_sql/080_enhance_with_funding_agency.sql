-- Enhance subawards with funding agency.


update
    temp_load_subawards_subaward

set
    funding_subtier_agency_abbreviation = sfa.abbreviation,
    funding_subtier_agency_name = sfa.name,
    funding_toptier_agency_abbreviation = tfa.abbreviation,
    funding_toptier_agency_name = tfa.name

from
    agency fa
    left outer join toptier_agency as tfa on tfa.toptier_agency_id = fa.toptier_agency_id
    left outer join subtier_agency as sfa on sfa.subtier_agency_id = fa.subtier_agency_id

where
    fa.id = temp_load_subawards_subaward.funding_agency_id and
    temp_load_subawards_subaward.funding_agency_id is not null;

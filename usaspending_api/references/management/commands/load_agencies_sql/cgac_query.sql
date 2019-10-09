select
    cgac_agency_code as cgac_code,
    max(agency_name) as agency_name,
    max(agency_abbreviation) as agency_abbreviation
from
    temp_load_agencies_raw_agency
where
    agency_name is not null and
    cgac_agency_code is not null
group by
    cgac_agency_code

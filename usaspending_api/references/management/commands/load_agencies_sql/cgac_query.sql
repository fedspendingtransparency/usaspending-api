select
    cgac_agency_code as cgac_code,
    max(agency_name) as agency_name,
    max(agency_abbreviation) as agency_abbreviation,
    bool_or(is_frec) as is_frec_agency
from
    "{temp_table}"
where
    agency_name is not null and
    cgac_agency_code is not null
group by
    cgac_agency_code

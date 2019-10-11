select
    subtier_code,
    max(subtier_abbreviation) as abbreviation,
    max(subtier_name) as name
from
    temp_load_agencies_raw_agency
where
    subtier_code is not null and
    subtier_name is not null
group by
    subtier_code

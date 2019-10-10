select
    frec as frec_code,
    max(frec_entity_description) as agency_name,
    max(frec_abbreviation) as agency_abbreviation
from
    temp_load_agencies_raw_agency
where
    frec is not null and
    frec_entity_description is not null
group by
    frec

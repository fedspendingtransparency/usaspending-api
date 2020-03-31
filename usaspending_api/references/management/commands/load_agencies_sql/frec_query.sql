select
    frec as frec_code,
    max(frec_entity_description) as agency_name,
    max(frec_abbreviation) as agency_abbreviation,
    max(case when frec_cgac_association then cgac_agency_code end) as associated_cgac_code
from
    "{temp_table}"
where
    frec is not null and
    frec_entity_description is not null
group by
    frec

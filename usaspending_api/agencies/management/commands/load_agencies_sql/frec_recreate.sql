-- FREC is always a full reload as it is just a lookup table and nothing should be attached to it via foreign keys.
delete from frec;


insert into
    frec (
        frec_code,
        agency_name,
        agency_abbreviation
    )
select
    frec,
    max(frec_entity_description),
    max(frec_abbreviation)
from
    temp_load_agencies_raw_agency
where
    frec is not null and
    frec_entity_description is not null
group by
    frec;

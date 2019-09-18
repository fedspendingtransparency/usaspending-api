-- CGAC is always a full reload as it is just a lookup table and nothing should be attached to it via foreign keys.
delete from cgac;


insert into
    cgac (
        cgac_code,
        agency_name,
        agency_abbreviation
    )
select
    cgac_agency_code,
    max(agency_name),
    max(agency_abbreviation)
from
    temp_load_agencies_raw_agency
where
    agency_name is not null and
    cgac_agency_code is not null
group by
    cgac_agency_code;

-- Insert new toptier agencies.
insert into
    toptier_agency (
        create_date,
        update_date,
        cgac_code,
        abbreviation,
        name,
        mission,
        website,
        justification,
        icon_filename
    )
select
    now(),
    now(),
    t.cgac_code,
    t.abbreviation,
    t.name,
    t.mission,
    t.website,
    t.justification,
    t.icon_filename
from
    temp_load_agencies_toptier_agency as t
    left outer join toptier_agency as ta on ta.cgac_code = t.cgac_code
where
    ta.cgac_code is null;

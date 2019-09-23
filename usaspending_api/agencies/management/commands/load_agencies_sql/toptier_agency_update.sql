-- Update toptier agencies where there are changes.
update
    toptier_agency as ta
set
    update_date = now(),
    abbreviation = t.abbreviation,
    name = t.name,
    mission = t.mission,
    website = t.website,
    justification = t.justification,
    icon_filename = t.icon_filename
from
    temp_load_agencies_toptier_agency as t
where
    t.cgac_code = ta.cgac_code and (
        t.abbreviation is distinct from ta.abbreviation or
        t.name is distinct from ta.name or
        t.mission is distinct from ta.mission or
        t.website is distinct from ta.website or
        t.justification is distinct from ta.justification or
        t.icon_filename is distinct from ta.icon_filename
    );

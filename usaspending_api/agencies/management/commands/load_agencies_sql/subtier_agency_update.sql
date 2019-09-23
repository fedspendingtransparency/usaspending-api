-- Update subtier agencies where there are changes.
update
    subtier_agency as sa
set
    update_date = now(),
    abbreviation = t.abbreviation,
    name = t.name
from
    temp_load_agencies_subtier_agency as t
where
    t.subtier_code = sa.subtier_code and (
        t.abbreviation is distinct from sa.abbreviation or
        t.name is distinct from sa.name
    );

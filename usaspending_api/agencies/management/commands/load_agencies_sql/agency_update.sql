-- Update agencies where there are changes.
update
    agency as a
set
    update_date = now(),
    toptier_flag = t.toptier_flag
from
    temp_load_agencies_agency as t
where
    t.toptier_agency_id = a.toptier_agency_id and
    t.subtier_agency_id is not distinct from a.subtier_agency_id and
    t.toptier_flag is distinct from a.toptier_flag;

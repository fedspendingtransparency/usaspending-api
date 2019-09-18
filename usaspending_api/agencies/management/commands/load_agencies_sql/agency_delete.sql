-- Remove obsolete agencies.
delete from
    agency
using
    agency as a
    left outer join temp_load_agencies_agency as t on
        t.toptier_agency_id is not distinct from a.toptier_agency_id and
        t.subtier_agency_id is not distinct from a.subtier_agency_id
where
    t.toptier_agency_id is null;

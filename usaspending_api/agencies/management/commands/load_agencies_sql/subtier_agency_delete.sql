-- Remove obsolete subtier agencies.
delete from
    subtier_agency
using
    subtier_agency as sa
    left outer join temp_load_agencies_subtier_agency as t on
        t.subtier_code is not distinct from t.subtier_code
where
    t.subtier_code is null;

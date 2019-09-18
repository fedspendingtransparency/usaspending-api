-- Insert new agencies.
insert into
    agency (
        create_date,
        update_date,
        toptier_agency_id,
        subtier_agency_id,
        toptier_flag,
        user_selectable
    )
select
    now(),
    now(),
    t.toptier_agency_id,
    t.subtier_agency_id,
    t.toptier_flag,
    t.user_selectable
from
    temp_load_agencies_agency as t
    left outer join agency as a on
        a.toptier_agency_id is not distinct from t.toptier_agency_id and
        a.subtier_agency_id is not distinct from t.subtier_agency_id
where
    a.id is null;

-- Insert new subtier agencies.
insert into
    subtier_agency (
        create_date,
        update_date,
        subtier_code,
        abbreviation,
        name
    )
select
    now(),
    now(),
    t.subtier_code,
    t.abbreviation,
    t.name
from
    temp_load_agencies_subtier_agency as t
    left outer join subtier_agency as sa on
        sa.subtier_code is not distinct from t.subtier_code
where
    sa.subtier_code is null;

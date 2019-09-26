drop table if exists temp_load_agencies_subtier_agency;


-- Create a temp table containing subtier agencies as we expect them to look in the final table.
create temporary table
    temp_load_agencies_subtier_agency
as select
    now() as create_date,
    now() as update_date,
    subtier_code,
    max(subtier_abbreviation) as abbreviation,
    max(subtier_name) as name
from
    temp_load_agencies_raw_agency
where
    subtier_code is not null and
    subtier_name is not null
group by
    subtier_code;

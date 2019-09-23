drop table if exists temp_load_agencies_agency;


-- Create a temp table containing agencies as we expect them to look in the final table.
create table
    temp_load_agencies_agency
as (

    -- Toptiers with subtiers.
    select
        now() as create_date,
        now() as update_date,
        ta.toptier_agency_id,
        sa.subtier_agency_id,
        tlara.toptier_flag
    from
        toptier_agency as ta
        inner join temp_load_agencies_raw_agency as tlara on
            case
                when tlara.is_frec is true then tlara.frec
                else tlara.cgac_agency_code
            end = ta.cgac_code
        inner join subtier_agency as sa on sa.subtier_code = tlara.subtier_code

    union all

    -- There are some toptiers without subtiers that we want to include in the agency table.
    select
        now() as create_date,
        now() as update_date,
        ta.toptier_agency_id,
        null, -- subtier_agency_id
        true  -- toptier_flag
    from
        toptier_agency as ta
        inner join temp_load_agencies_raw_agency as tlara on
            case
                when tlara.is_frec is true then tlara.frec
                else tlara.cgac_agency_code
            end = ta.cgac_code
        left outer join subtier_agency as sa on sa.subtier_code = tlara.subtier_code
    where
        sa.subtier_code is null and
        tlara.include_toptier_without_subtier is true

);

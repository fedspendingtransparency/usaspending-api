drop table if exists temp_load_agencies_agency;


-- Create a temp table containing agencies as we expect them to look in the final table.
create table
    temp_load_agencies_agency
as (

    -- Agencies with equivalent subtiers.
    select
        now() as create_date,
        now() as update_date,
        ta.toptier_agency_id,
        sa.subtier_agency_id,
        tlara.toptier_flag,
        tlara.user_selectable
    from
        subtier_agency as sa
        inner join temp_load_agencies_raw_agency as tlara on tlara.subtier_code = sa.subtier_code
        inner join toptier_agency as ta on ta.cgac_code = case
            when tlara.is_frec is true then tlara.frec
            else tlara.cgac_agency_code
        end

    union all

    -- Toptier agencies that have no equivalent subtier as indicated by toptier_flag.  Every toptier
    -- agency must be represented in the agency table as either having an equivalent subtier agency
    -- as denoted by toptier_flag or as a standalone entity so that treasury_appropriate_account
    -- records can be linked to them.
    --
    -- An equivalent subtier is a subtier code that represents the toptier agency as a whole and is
    -- therefore basically equivalent to the toptier.  Not all agencies have equivalent subtiers.  We
    -- denote this by setting the toptier_flag to true for the subtier.
    select
        now() as create_date,
        now() as update_date,
        ta.toptier_agency_id,
        null as subtier_agency_id,
        false as toptier_flag,
        bool_or(tlara.user_selectable) as user_selectable
    from
        toptier_agency as ta
        inner join temp_load_agencies_raw_agency as tlara on case
            when tlara.is_frec is true then tlara.frec
            else tlara.cgac_agency_code
        end = ta.cgac_code
        left outer join (
            select  sa.subtier_code
            from    subtier_agency as sa
                    inner join temp_load_agencies_raw_agency as tlara on
                        tlara.subtier_code = sa.subtier_code and
                        tlara.toptier_flag is true
        ) t on t.subtier_code = tlara.subtier_code
    where
        t.subtier_code is null
    group by
        ta.toptier_agency_id

);

select
    toptier_agency_id,
    subtier_agency_id,
    toptier_flag,
    user_selectable

from (

    -- Toptiers with subtiers.
    select
        ta.toptier_agency_id,
        sa.subtier_agency_id,
        tlara.toptier_flag,
        tlara.user_selectable
    from
        toptier_agency as ta
        inner join "{temp_table}" as tlara on
            case
                when tlara.is_frec is true then tlara.frec
                else tlara.cgac_agency_code
            end = ta.toptier_code
        inner join subtier_agency as sa on sa.subtier_code = tlara.subtier_code

    union all

    -- There are some toptiers without subtiers that we want to include in the agency table.
    select
        ta.toptier_agency_id,
        null, -- subtier_agency_id
        tlara.toptier_flag,
        tlara.user_selectable
    from
        toptier_agency as ta
        inner join "{temp_table}" as tlara on
            case
                when tlara.is_frec is true then tlara.frec
                else tlara.cgac_agency_code
            end = ta.toptier_code
        left outer join subtier_agency as sa on sa.subtier_code = tlara.subtier_code
    where
        sa.subtier_code is null

) t

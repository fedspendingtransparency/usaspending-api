select
    toptier_code,
    abbreviation,
    name,
    mission,
    website,
    justification,
    icon_filename

from (

    -- CGAC agencies
    select
        cgac_agency_code as toptier_code,
        max(agency_abbreviation) as abbreviation,
        max(agency_name) as name,
        max(mission) as mission,
        max(website) as website,
        max(congressional_justification) as justification,
        max(icon_filename) as icon_filename
    from
        "{temp_table}"
    where
        cgac_agency_code is not null and
        agency_name is not null and
        is_frec is false
    group by
        cgac_agency_code

    union all

    -- FREC agencies
    select
        frec as toptier_code,
        max(frec_abbreviation) as abbreviation,
        max(frec_entity_description) as name,
        max(mission) as mission,
        max(website) as website,
        max(congressional_justification) as justification,
        max(icon_filename) as icon_filename
    from
        "{temp_table}"
    where
        frec is not null and
        frec_entity_description is not null and
        is_frec is true
    group by
        frec

) t

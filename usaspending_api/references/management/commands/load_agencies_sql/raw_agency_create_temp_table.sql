drop table if exists "{temp_table}";

create temporary table "{temp_table}" (
    row_number int,
    cgac_agency_code text,
    agency_name text,
    agency_abbreviation text,
    frec text,
    frec_entity_description text,
    frec_abbreviation text,
    subtier_code text,
    subtier_name text,
    subtier_abbreviation text,
    toptier_flag boolean,
    is_frec boolean,
    frec_cgac_association boolean,
    user_selectable boolean,
    mission text,
    about_agency_data text,
    website text,
    congressional_justification text,
    icon_filename text
);

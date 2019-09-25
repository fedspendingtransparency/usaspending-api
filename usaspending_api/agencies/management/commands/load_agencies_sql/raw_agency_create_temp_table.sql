drop table if exists temp_load_agencies_raw_agency;


create table temp_load_agencies_raw_agency (
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
    user_selectable boolean,
    mission text,
    website text,
    congressional_justification text,
    icon_filename text,
    include_toptier_without_subtier boolean
);

drop table if exists temp_raw_agency_csv;
create table temp_raw_agency_csv (
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
    mission text,
    website text,
    congressional_justification text,
    icon_filename text
);

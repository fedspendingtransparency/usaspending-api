delete from cgac;
delete from frec;
delete from subtier_agency_new;
delete from toptier_agency_new;



insert into
    cgac (
        cgac_code,
        agency_name,
        agency_abbreviation
    )
select
    cgac_agency_code,
    max(agency_name),
    max(agency_abbreviation)
from
    temp_raw_agency_csv
where
    agency_name is not null and
    cgac_agency_code is not null
group by
    cgac_agency_code;



insert into
    frec (
        frec_code,
        agency_name,
        agency_abbreviation
    )
select
    frec,
    max(frec_entity_description),
    max(frec_abbreviation)
from
    temp_raw_agency_csv
where
    frec is not null and
    frec_entity_description is not null
group by
    frec;



insert into
    toptier_agency_new (
        toptier_code,
        agency_name,
        agency_abbreviation,
        mission,
        website,
        congressional_justification,
        icon_filename
    )
    (
        select
            cgac_agency_code,
            max(agency_name),
            max(agency_abbreviation),
            max(mission),
            max(website),
            max(congressional_justification),
            max(icon_filename)
        from
            temp_raw_agency_csv
        where
            cgac_agency_code is not null and
            agency_name is not null and
            is_frec is false
        group by
            cgac_agency_code
    )
    union all
    (
        select
            frec,
            max(frec_entity_description),
            max(frec_abbreviation),
            max(mission),
            max(website),
            max(congressional_justification),
            max(icon_filename)
        from
            temp_raw_agency_csv
        where
            frec is not null and
            frec_entity_description is not null and
            is_frec is true
        group by
            frec
    );



insert into
    subtier_agency_new (
        subtier_code,
        toptier_code,
        agency_name,
        agency_abbreviation,
        is_toptier
    )
select
    subtier_code,
    max(case when is_frec is true then frec else cgac_agency_code end),
    max(subtier_name),
    max(subtier_abbreviation),
    bool_or(toptier_flag)
from
    temp_raw_agency_csv trac
    inner join toptier_agency_new tan on tan.toptier_code = case when is_frec is true then frec else cgac_agency_code end
where
    subtier_code is not null and
    subtier_name is not null
group by
    subtier_code;

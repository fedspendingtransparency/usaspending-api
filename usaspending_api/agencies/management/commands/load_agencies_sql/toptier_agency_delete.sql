-- Remove obsolete toptier agencies.
delete from
    toptier_agency
using
    toptier_agency as ta
    left outer join temp_load_agencies_toptier_agency as t on t.cgac_code = ta.cgac_code
where
    t.cgac_code is null;

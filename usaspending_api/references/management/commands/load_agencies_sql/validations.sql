drop table if exists "{temp_table}_validations";


create temporary table "{temp_table}_validations" (message text);


insert into "{temp_table}_validations"
select      concat('Row number ', row_number, ' has a CGAC AGENCY CODE but no AGENCY NAME.')
from        "{temp_table}"
where       cgac_agency_code is not null and agency_name is null;


insert into "{temp_table}_validations"
select      concat('Row number ', row_number, ' has a FREC but no FREC Entity Description.')
from        "{temp_table}"
where       frec is not null and frec_entity_description is null;


insert into "{temp_table}_validations"
select      concat('Row number ', row_number, ' has a SUBTIER CODE but no SUBTIER NAME.')
from        "{temp_table}"
where       subtier_code is not null and subtier_name is null;


insert into "{temp_table}_validations"
select      concat('Row number ', row_number, ' is marked as IS_FREC but has no FREC.')
from        "{temp_table}"
where       is_frec is true and frec is null;


insert into "{temp_table}_validations"
select      concat('Row number ', row_number, ' is not marked as IS_FREC but has no CGAC AGENCY CODE.')
from        "{temp_table}"
where       is_frec is false and cgac_agency_code is null;


insert into "{temp_table}_validations"
select      concat('Row number ', row_number, ' is marked as FREC CGAC ASSOCIATION but has no CGAC AGENCY CODE.')
from        "{temp_table}"
where       frec_cgac_association is true and cgac_agency_code is null;


insert into "{temp_table}_validations"
select      concat('Row number ', row_number, ' is marked as FREC CGAC ASSOCIATION but has no FREC.')
from        "{temp_table}"
where       frec_cgac_association is true and frec is null;


insert into "{temp_table}_validations"
select      concat('Row number ', row_number, ' has a CGAC AGENCY CODE that is not 3 characters long.')
from        "{temp_table}"
where       cgac_agency_code is not null and length(cgac_agency_code) != 3;


insert into "{temp_table}_validations"
select      concat('Row number ', row_number, ' has a FREC that is not 4 characters long.')
from        "{temp_table}"
where       frec is not null and length(frec) != 4;


insert into "{temp_table}_validations"
select      concat('Row number ', row_number, ' has a SUBTIER CODE that is not 4 characters long.')
from        "{temp_table}"
where       subtier_code is not null and length(subtier_code) != 4;


insert into "{temp_table}_validations"
select      concat(cgac_agency_code, ' (CGAC) has more than one IS_FREC FREC.') as message
from        "{temp_table}"
group by    message
having      count(distinct is_frec) > 1;


insert into "{temp_table}_validations"
select      concat(
                case when is_frec is true then frec else cgac_agency_code end,
                ' (toptier) has more than one subtier marked with the TOPTIER_FLAG.'
            ) as message
from        "{temp_table}"
where       toptier_flag is true
group by    message
having      count(distinct subtier_code) > 1;


insert into "{temp_table}_validations"
select      concat(frec, ' (FREC) is associated with more than one CGAC via FREC CGAC ASSOCIATION.') as message
from        "{temp_table}"
where       frec_cgac_association is true
group by    message
having      count(distinct cgac_agency_code) > 1;


select * from "{temp_table}_validations" limit 20;

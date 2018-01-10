drop materialized view if exists agency_lookup;

create materialized view agency_lookup as
(     
	select
		agency.id as agency_id,
		toptier_agency.cgac_code as toptier_cgac,
		toptier_agency.name as toptier_name,
		toptier_agency.abbreviation as toptier_abbr,
		subtier_agency.subtier_code as subtier_code,
		subtier_agency.name as subtier_name,
		subtier_agency.abbreviation as subtier_abbr,
		office_agency.aac_code as office_code,
		office_agency.name as office_name
	from
		agency
	inner join
		toptier_agency on toptier_agency.toptier_agency_id=agency.toptier_agency_id
	inner join
		subtier_agency on subtier_agency.subtier_agency_id=agency.subtier_agency_id
	left outer join
		office_agency on office_agency.office_agency_id=agency.office_agency_id
);

create index agency_lookup_subtier_code_idx on agency_lookup using btree (subtier_code);
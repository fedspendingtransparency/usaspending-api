drop materialized view if exists agency_lookup;

create materialized view agency_lookup as
(     
	select
		agency.id as agency_id,
		subtier_agency.subtier_code as subtier_code     
	from
		agency         
	inner join
		subtier_agency on subtier_agency.subtier_agency_id=agency.subtier_agency_id
);

create index agency_lookup_subtier_code_idx on agency_lookup using btree (subtier_code);
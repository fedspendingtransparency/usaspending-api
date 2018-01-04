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

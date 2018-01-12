DROP MATERIALIZED VIEW if EXISTS agency_lookup;

CREATE MATERIALIZED VIEW agency_lookup AS
(     
	SELECT
		agency.id AS agency_id,
		toptier_agency.cgac_code AS toptier_cgac,
		toptier_agency.name AS toptier_name,
		toptier_agency.abbreviatiON AS toptier_abbr,
		subtier_agency.subtier_code AS subtier_code,
		subtier_agency.name AS subtier_name,
		subtier_agency.abbreviatiON AS subtier_abbr,
		office_agency.aac_code AS office_code,
		office_agency.name AS office_name
	FROM
		agency
	INNER JOIN
		toptier_agency ON toptier_agency.toptier_agency_id=agency.toptier_agency_id
	INNER JOIN
		subtier_agency ON subtier_agency.subtier_agency_id=agency.subtier_agency_id
	LEFT OUTER JOIN
		office_agency ON office_agency.office_agency_id=agency.office_agency_id
);

CREATE INDEX agency_lookup_subtier_code_idx ON agency_lookup USING btree (subtier_code);
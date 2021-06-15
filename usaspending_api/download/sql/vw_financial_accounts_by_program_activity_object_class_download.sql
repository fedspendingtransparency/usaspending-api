DROP VIEW IF EXISTS vw_financial_accounts_by_program_activity_object_class_download;
CREATE VIEW vw_financial_accounts_by_program_activity_object_class_download AS
SELECT
    FABPAOC.*,
	CGAC_AID.agency_name AS agency_identifier_name,
	CGAC_ATA.agency_name AS allocation_transfer_agency_identifier_name
FROM financial_accounts_by_program_activity_object_class AS FABPAOC
INNER JOIN submission_attributes AS SA USING (submission_id)
LEFT OUTER JOIN treasury_appropriation_account AS TAA ON (FABPAOC.treasury_account_id = TAA.treasury_account_identifier)
LEFT OUTER JOIN cgac AS CGAC_AID ON (TAA.agency_id = CGAC_AID.cgac_code)
LEFT OUTER JOIN cgac AS CGAC_ATA ON (TAA.allocation_transfer_agency_id = CGAC_ATA.cgac_code)
;

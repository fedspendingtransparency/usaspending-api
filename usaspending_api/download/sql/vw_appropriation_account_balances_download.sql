DROP VIEW IF EXISTS vw_appropriation_account_balances_download;
CREATE VIEW vw_appropriation_account_balances_download AS
SELECT
    AAB.*,
	CGAC_AID.agency_name AS agency_identifier_name,
	CGAC_ATA.agency_name AS allocation_transfer_agency_identifier_name
FROM appropriation_account_balances AS AAB
INNER JOIN submission_attributes AS SA USING (submission_id)
LEFT OUTER JOIN treasury_appropriation_account AS TAA USING (treasury_account_identifier)
LEFT OUTER JOIN cgac AS CGAC_AID ON (TAA.agency_id = CGAC_AID.cgac_code)
LEFT OUTER JOIN cgac AS CGAC_ATA ON (TAA.allocation_transfer_agency_id = CGAC_ATA.cgac_code)
;

DROP VIEW IF EXISTS vw_account_balances;
CREATE VIEW vw_account_balances AS
SELECT
    "appropriation_account_balances".*,
	"cgac_aid"."agency_name" AS "agency_identifier_name",
	"cgac_ata"."agency_name" AS "allocation_transfer_agency_identifier_name"
FROM "appropriation_account_balances"
INNER JOIN "submission_attributes" ON ("appropriation_account_balances"."submission_id" = "submission_attributes"."submission_id")
LEFT OUTER JOIN "treasury_appropriation_account" ON ("appropriation_account_balances"."treasury_account_identifier" = "treasury_appropriation_account"."treasury_account_identifier")
LEFT OUTER JOIN "cgac" AS "cgac_aid" on ("treasury_appropriation_account"."agency_id" = "cgac_aid"."cgac_code")
LEFT OUTER JOIN "cgac" AS "cgac_ata" on ("treasury_appropriation_account"."allocation_transfer_agency_id" = "cgac_ata"."cgac_code")
;

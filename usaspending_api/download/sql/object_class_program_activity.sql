DROP VIEW IF EXISTS vw_object_class_program_activity;
CREATE VIEW vw_object_class_program_activity AS
SELECT
    "financial_accounts_by_program_activity_object_class".*,
	"cgac_aid"."agency_name" AS "agency_identifier_name",
	"cgac_ata"."agency_name" AS "allocation_transfer_agency_identifier_name"
FROM "financial_accounts_by_program_activity_object_class"
INNER JOIN "submission_attributes" ON ("financial_accounts_by_program_activity_object_class"."submission_id" = "submission_attributes"."submission_id")
LEFT OUTER JOIN "treasury_appropriation_account" ON ("financial_accounts_by_program_activity_object_class"."treasury_account_id" = "treasury_appropriation_account"."treasury_account_identifier")
LEFT OUTER JOIN "cgac" AS "cgac_aid" on ("treasury_appropriation_account"."agency_id" = "cgac_aid"."cgac_code")
LEFT OUTER JOIN "cgac" AS "cgac_ata" on ("treasury_appropriation_account"."allocation_transfer_agency_id" = "cgac_ata"."cgac_code")
;

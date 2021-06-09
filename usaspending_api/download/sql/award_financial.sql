DROP VIEW IF EXISTS vw_award_financial;
CREATE VIEW vw_award_financial AS
SELECT
    "financial_accounts_by_awards".*,
	"cgac_aid"."agency_name" AS "agency_identifier_name",
	"cgac_ata"."agency_name" AS "allocation_transfer_agency_identifier_name"
FROM "financial_accounts_by_awards"
INNER JOIN "submission_attributes" ON ("financial_accounts_by_awards"."submission_id" = "submission_attributes"."submission_id")
LEFT OUTER JOIN "treasury_appropriation_account" ON ("financial_accounts_by_awards"."treasury_account_id" = "treasury_appropriation_account"."treasury_account_identifier")
LEFT OUTER JOIN "cgac" AS "cgac_aid" on ("treasury_appropriation_account"."agency_id" = "cgac_aid"."cgac_code")
LEFT OUTER JOIN "cgac" AS "cgac_ata" on ("treasury_appropriation_account"."allocation_transfer_agency_id" = "cgac_ata"."cgac_code")
;

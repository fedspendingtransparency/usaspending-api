UPDATE federal_account fa
SET
    account_title = tas.account_title
FROM treasury_appropriation_account tas
WHERE tas.agency_id=fa.agency_identifier and tas.main_account_code=fa.main_account_code;

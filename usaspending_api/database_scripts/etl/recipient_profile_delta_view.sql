DROP VIEW IF EXISTS recipient_profile_delta_view;

CREATE VIEW recipient_profile_delta_view AS
SELECT
  "recipient_name",
  "uei",
  "recipient_level",
FROM "recipient_profile"

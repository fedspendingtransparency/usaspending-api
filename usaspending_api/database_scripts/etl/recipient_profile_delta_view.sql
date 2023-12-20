DROP VIEW IF EXISTS recipient_profile_delta_view;

CREATE VIEW recipient_profile_delta_view AS
SELECT
  "recipient_hash",
  "recipient_name",
  "uei",
  ARRAY_AGG("recipient_level") AS "recipient_levels"
FROM "recipient_profile"
GROUP BY
  "recipient_hash",
  "recipient_name",
  "uei"

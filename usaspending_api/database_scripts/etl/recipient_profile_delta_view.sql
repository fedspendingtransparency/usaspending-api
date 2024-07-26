DROP VIEW IF EXISTS recipient_profile_delta_view;

CREATE VIEW recipient_profile_delta_view AS
SELECT
  id,
  recipient_hash,
  recipient_name,
  uei,
  recipient_level,
  recipient_unique_id AS duns
FROM
  rpt.recipient_profile;


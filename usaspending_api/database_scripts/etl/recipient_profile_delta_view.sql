DROP VIEW IF EXISTS recipient_profile_delta_view;

CREATE VIEW recipient_profile_delta_view AS
SELECT
  rp.id,
  rp.recipient_hash,
  rp.recipient_name,
  rp.uei,
  rp.recipient_level,
  rl.duns
FROM
  recipient_profile rp
JOIN
  recipient_lookup rl
ON
  rp.recipient_hash = rl.recipient_hash;
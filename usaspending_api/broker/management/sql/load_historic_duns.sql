DROP TABLE IF EXISTS historic_parent_duns_new;

CREATE TABLE historic_parent_duns_new AS (
    SELECT
        broker_historic_duns.awardee_or_recipient_uniqu AS awardee_or_recipient_uniqu,
        broker_historic_duns.legal_business_name AS legal_business_name,
        broker_historic_duns.ultimate_parent_unique_ide AS ultimate_parent_unique_ide,
        broker_historic_duns.ultimate_parent_legal_enti AS ultimate_parent_legal_enti,
        broker_historic_duns.duns_id AS broker_historic_duns_id,
        broker_historic_duns.year AS year
    FROM
        dblink ('broker_server', '(
            SELECT
                hduns.awardee_or_recipient_uniqu,
                hduns.legal_business_name,
                hduns.ultimate_parent_unique_ide,
                hduns.ultimate_parent_legal_enti,
                hduns.duns_id,
                hduns.year
            FROM
                historic_parent_duns as hduns)') AS broker_historic_duns
            (
                awardee_or_recipient_uniqu text,
                legal_business_name text,
                ultimate_parent_unique_ide text,
                ultimate_parent_legal_enti text,
                duns_id text,
                year int
            )
);
CREATE INDEX historic_parent_duns_awardee_idx_new ON historic_parent_duns_new USING btree (awardee_or_recipient_uniqu);
CREATE INDEX historic_parent_duns_year_idx_new ON historic_parent_duns_new USING btree (year);

BEGIN;
ALTER TABLE IF EXISTS historic_parent_duns RENAME TO historic_parent_duns_old;
ALTER TABLE historic_parent_duns_new RENAME TO historic_parent_duns;
ALTER INDEX historic_parent_duns_awardee_idx_new RENAME TO historic_parent_duns_awardee_idx;
ALTER INDEX historic_parent_duns_year_idx_new RENAME TO historic_parent_duns_year_idx;
DROP TABLE IF EXISTS historic_parent_duns_old CASCADE;
COMMIT;

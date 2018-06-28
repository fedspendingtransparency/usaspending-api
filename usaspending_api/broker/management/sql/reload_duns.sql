BEGIN;
DROP INDEX IF EXISTS duns_awardee_idx;
TRUNCATE TABLE duns;
INSERT INTO duns (
    SELECT
        broker_duns.awardee_or_recipient_uniqu AS awardee_or_recipient_uniqu,
        broker_duns.legal_business_name AS legal_business_name,
        broker_duns.ultimate_parent_unique_ide AS ultimate_parent_unique_ide,
        broker_duns.ultimate_parent_legal_enti AS ultimate_parent_legal_enti,
        broker_duns.duns_id AS broker_duns_id,
        NOW()::DATE AS update_date
    FROM
        dblink ('broker_server', '(
            SELECT
                DISTINCT
                    ON (duns.awardee_or_recipient_uniqu)
                    duns.awardee_or_recipient_uniqu,
                    duns.legal_business_name,
                    duns.ultimate_parent_unique_ide,
                    duns.ultimate_parent_legal_enti,
                    duns.duns_id
            FROM
                duns
            ORDER BY
                duns.awardee_or_recipient_uniqu,
                duns.activation_date DESC NULLS LAST)') AS broker_duns
            (
                awardee_or_recipient_uniqu text,
                legal_business_name text,
                ultimate_parent_unique_ide text,
                ultimate_parent_legal_enti text,
                duns_id text
            )
);
CREATE UNIQUE INDEX duns_awardee_idx ON duns USING btree (awardee_or_recipient_uniqu);
COMMIT;
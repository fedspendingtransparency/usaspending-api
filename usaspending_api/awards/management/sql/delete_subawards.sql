-- Get the legal entities associated with the subawards
CREATE MATERIALIZED VIEW le_subaward AS (
    SELECT le.legal_entity_id, le.location_id
    FROM legal_entity AS le
    WHERE EXISTS (
        SELECT 1
        FROM awards_subaward AS asub
        WHERE asub.recipient_id = le.legal_entity_id
    )
);
-- Get the locations associated with the subawards
CREATE MATERIALIZED VIEW sub_location AS (
    SELECT tmp.location_id
    FROM (
        (
            SELECT rl.location_id AS location_id
            FROM references_location AS rl
            WHERE EXISTS (
                    SELECT 1
                    FROM awards_subaward AS asub
                    WHERE asub.place_of_performance_id = rl.location_id
                )
        )
        UNION
        (
            SELECT rl.location_id AS location_id
            FROM references_location AS rl
            WHERE EXISTS (
                SELECT 1
                FROM le_subaward AS les
                WHERE les.location_id = rl.location_id
            )
        )
    ) AS tmp
);

-- Remove the subawards
DELETE FROM awards_subaward;

-- Remove the legal entities
DELETE FROM legal_entity AS le
USING le_subaward
WHERE le_subaward.legal_entity_id = le.legal_entity_id;

-- Delete teh subaward locations
DELETE FROM references_location as loc
USING sub_location
WHERE sub_location.location_id = loc.location_id;

DROP MATERIALIZED VIEW sub_location;
DROP MATERIALIZED VIEW le_subaward;

-- Remove the locations associated with the subawards
WITH le_subaward AS (
    SELECT le.legal_entity_id, le.location_id
    FROM legal_entity AS le
    WHERE EXISTS (
        SELECT 1
        FROM awards_subaward AS asub
        WHERE asub.recipient_id = le.legal_entity_id
    )
),
sub_location AS (
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
)
DELETE FROM references_location as loc
USING sub_location
WHERE sub_location.location_id = loc.location_id;

-- Remove the legal entities
WITH le_subaward AS (
    SELECT le.legal_entity_id, le.location_id
    FROM legal_entity AS le
    WHERE EXISTS (
        SELECT 1
        FROM awards_subaward AS asub
        WHERE asub.recipient_id = le.legal_entity_id
    )
)
DELETE FROM legal_entity AS le
USING le_subaward
WHERE le_subaward.legal_entity_id = le.legal_entity_id;

-- Remove the subawards
DELETE FROM awards_subaward;
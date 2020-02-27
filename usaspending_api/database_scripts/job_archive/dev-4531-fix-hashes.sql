-- Jira Ticket Number: DEV-4531
-- Expected CLI: psql -v ON_ERROR_STOP=1 -c '\timing' -f dev-4531-fix-hashes.sql $DATABASE_URL
--
-- Purpose: Remove the "b''" byte literal from hashes in the filter_hash table

DELETE FROM filter_hash where id in (SELECT b.id FROM filter_hash a INNER JOIN filter_hash b ON a.hash = trim(both '\''' from trim(leading 'b' from b.hash)) WHERE b.hash LIKE 'b\''%\''');

UPDATE filter_hash SET hash = TRIM(both '\''' from trim(leading 'b' from hash)) WHERE hash LIKE 'b\''%\'''; 
UPDATE transaction_fabs_new
SET transaction_id = tn_new.id
FROM transaction_normalized_new AS tn_new
WHERE tn_new.transaction_unique_id = transaction_fabs_new.afa_generated_unique;

UPDATE transaction_fpds_new
SET transaction_id = tn_new.id
FROM transaction_normalized_new AS tn_new
WHERE tn_new.transaction_unique_id = transaction_fpds_new.detached_award_proc_unique;

--UPDATE transaction_fabs_new
--SET transaction_id = (SELECT id FROM transaction_normalized_new WHERE transaction_normalized_new.transaction_unique_id = transaction_fabs_new.afa_generated_unique);
--
--UPDATE transaction_fpds_new
--SET transaction_id = (SELECT id FROM transaction_normalized_new WHERE transaction_normalized_new.transaction_unique_id = transaction_fpds_new.detached_award_proc_unique);
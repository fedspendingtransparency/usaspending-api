UPDATE transaction_normalized_new
SET award_id = (SELECT id FROM awards_new WHERE transaction_normalized_new.generated_unique_award_id = awards_new.generated_unique_award_id);

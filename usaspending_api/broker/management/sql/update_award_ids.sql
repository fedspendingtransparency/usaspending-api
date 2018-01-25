UPDATE transaction_normalized_new
SET award_id = awards_new.id
FROM awards_new
WHERE awards_new.generated_unique_award_id = transaction_normalized_new.generated_unique_award_id;

# Calculating Earliest and Latest Transaction 

## Background

On the Awards model, we keep track of the earliest and latest transaction associated with the Award. These fields (`earliest_transaction_id` and `latest_transaction_id`) must be recalculated whenever changes are made to transactions associated with the award.

## Sort Fields 
In order to perform these calculations, we sort by the following fields. Usually, `action_date` is sufficient, but in the case of a tie `modification_number` is used. If there also happens to be a tie there, `transaction_unique_id` is used.

* `action_date`
* `modification_number`
* `transaction_unique_id`

### Note on Modification Numbers

Because a modification number can be empty or `null`, it is important to understand how Postgres handles `null` values in sorts. An empty or `null` value is considered larger than any other numbers, so if two or more transactions share the same `action_date`, a transaction with a `modification_number` of `null` would be considered newer than one with a non-`null` `modification_number`.


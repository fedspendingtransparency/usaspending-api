# Calculating Earliest and Latest Transaction 

## Background

On the Awards model, we keep track of the earliest and latest transaction associated with the Award. These fields are calculated based on the `action_date` and use a series of other fields in the case of a tie. 

## Sort Fields 

The following fields are taken into account when calculating the `earliest_transaction_id` of an award:

* `action_date`
* `modification_number`
* `transaction_unique_id`

### Note on Modification Numbers

Because a modification number can be empty or `null`, it is important to understand how Postgres handles `null` values in sorts. An empty or `null` value is considered larger than any other numbers, so a transaction with a `modification_number` of `null` would be considered newer than one with a `modification_number` of `1`.


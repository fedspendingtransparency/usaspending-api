# Populate Awards IDV Columns

This is a quick and dirty script to populate three IDV related columns in the USAspending Awards table: `fpds_agency_id`, `fpds_parent_agency_id`, and `base_exercised_options_val`.  It was created in lieu of a pure SQL script that took over 24 hours to complete.  Chunking the updates proved to be significantly faster.

# References

- [Jira ticket this for this task (OPS-494)](https://federal-spending-transparency.atlassian.net/browse/OPS-494)
- [Jira ticket for related sub-task (DEV-2000)](https://federal-spending-transparency.atlassian.net/browse/DEV-2000)
- [Jira ticket for parent story (DEV-1916)](https://federal-spending-transparency.atlassian.net/browse/DEV-1916)

# Getting Started

This script was developed and tested under Python 3.5.6 and psycopg2 2.7.5.  It requires only a single environment variable `DATABASE_URL` that contains a connection string that points to the database to be updated.

And that's it.

```
# Obviously, use your, personal connection string
export DATABASE_URL="postgres://localhost:5432/data_store_api"
python populate_awards_idv_columns.py
```

Unfortunately, there's currently no way to start from a specific point, but re-running the script should cause no harm if the script fails for any reason.

# Processing

The general steps performed by this script are:

1. If the temp table doesn't already exist:
    1. Create temp table to hold interim results
    2. Populate temp table
    3. Create a primary key on the temp table
2. Spawn a bunch of processes to update chunks of the `Awards` table from the temp table
3. Drop the temp table

The script is very verbose.  Sorry!

# Original Query

The original SQL script that took over 24 hours to complete:

```
WITH txn_totals AS (
    SELECT
        tx.award_id,
        f.agency_id,
        f.referenced_idv_agency_iden,
        SUM(CAST(f.base_exercised_options_val AS double precision)) AS base_exercised_options_val
    FROM transaction_fpds AS f
    INNER JOIN transaction_normalized AS tx ON f.transaction_id = tx.id
    GROUP BY tx.award_id, f.agency_id, f.referenced_idv_agency_iden
)
UPDATE award_search a
SET
    base_exercised_options_val = t.base_exercised_options_val,
    fpds_agency_id = t.agency_id,
    fpds_parent_agency_id = t.referenced_idv_agency_iden
FROM txn_totals AS t
WHERE t.award_id = a.award_id
```

We tried dozens of alternatives, rewrites, indexes, and nothing seemed to help.  The problem seemed to be related to the sheer number of updates being performed.

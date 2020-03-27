# Overview

These files populate and update `recipient_lookup` using the latest SAM data (in `duns`) and transaction data from `transaction_fabs` and `transaction_fpds`

Useful single-line command to run all SQL files in correct order as a single file. Run from this directory.

    ls -v *.sql | xargs cat | psql $DATABASE_URL -v ON_ERROR_STOP=1 -c '\timing' -f -

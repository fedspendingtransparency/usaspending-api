--------------------------------------------------------------------------------
-- Step 1, create the temporary matview of recipients from transactions
--------------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS public.temporary_recipients_from_transactions_view;
DROP TABLE IF EXISTS public.temporary_restock_recipient_profile;
DROP INDEX IF EXISTS public.idx_recipients_in_transactions_view;
DROP INDEX IF EXISTS public.idx_recipient_profile_uniq_new;

DO $$ BEGIN RAISE NOTICE 'Step 1: Creating temp materialized view'; END $$;
CREATE MATERIALIZED VIEW public.temporary_recipients_from_transactions_view AS (
  SELECT
    recipient_hash,
    recipient_unique_id,
    parent_recipient_unique_id,
    COALESCE(award_category, 'contract') AS award_category,
    CASE
      WHEN parent_recipient_unique_id IS NOT NULL THEN 'C'
    ELSE 'R' END AS recipient_level,
    action_date,
    generated_pragmatic_obligation
  FROM
    universal_transaction_tableview
  WHERE action_date >= '2007-10-01' AND
    (award_category IS NOT NULL OR (award_category IS NULL AND pulled_from = 'IDV'))
);

CREATE INDEX idx_recipients_in_transactions_view ON public.temporary_recipients_from_transactions_view USING BTREE(recipient_hash, recipient_level);

--------------------------------------------------------------------------------
-- Step 2, Create the new table and populate with 100% of combinations
--------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 2: Creating new temp table'; END $$;
CREATE TABLE public.temporary_restock_recipient_profile (
  recipient_level character(1) NOT NULL,
  recipient_hash UUID,
  recipient_unique_id TEXT,
  recipient_name TEXT,
  unused BOOLEAN DEFAULT true,
  recipient_affiliations TEXT[] DEFAULT '{}'::text[],
  award_types TEXT[] DEFAULT '{}'::text[],
  last_12_months NUMERIC(23,2) DEFAULT 0.00,
  last_12_contracts NUMERIC(23,2) DEFAULT 0.00,
  last_12_grants NUMERIC(23,2) DEFAULT 0.00,
  last_12_direct_payments NUMERIC(23,2) DEFAULT 0.00,
  last_12_loans NUMERIC(23,2) DEFAULT 0.00,
  last_12_other NUMERIC(23,2) DEFAULT 0.00,
  last_12_months_count INT DEFAULT 0,

  CHECK (award_types <@ ARRAY['contract', 'loans', 'grant', 'direct payment', 'other']::text[])
);

INSERT INTO public.temporary_restock_recipient_profile (
  recipient_level,
  recipient_hash,
  recipient_unique_id,
  recipient_name
)
  SELECT
    'P' as recipient_level,
    recipient_hash,
    duns AS recipient_unique_id,
    legal_business_name AS recipient_name
  FROM
    public.recipient_lookup
UNION ALL
  SELECT
    'C' as recipient_level,
    recipient_hash,
    duns AS recipient_unique_id,
    legal_business_name AS recipient_name
  FROM
    public.recipient_lookup
UNION ALL
  SELECT
    'R' as recipient_level,
    recipient_hash,
    duns AS recipient_unique_id,
    legal_business_name AS recipient_name
  FROM
    public.recipient_lookup;


CREATE UNIQUE INDEX idx_recipient_profile_uniq_new ON public.temporary_restock_recipient_profile USING BTREE(recipient_hash, recipient_level);

--------------------------------------------------------------------------------
-- Step 3, Obligation for past 12 months
--------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 3: last 12 months obligation'; END $$;

WITH grouped_by_category AS (
  WITH grouped_by_category_inner AS (
    SELECT
      recipient_hash,
      recipient_level,
      CASE
        WHEN award_category NOT IN ('contract', 'grant', 'direct payment', 'loans')
        THEN 'other' ELSE award_category
      END AS award_category,
      CASE WHEN award_category = 'contract' THEN SUM(generated_pragmatic_obligation) ELSE 0::NUMERIC(23,2) END AS inner_contracts,
      CASE WHEN award_category = 'grant' THEN SUM(generated_pragmatic_obligation) ELSE 0::NUMERIC(23,2) END AS inner_grants,
      CASE WHEN award_category = 'direct payment' THEN SUM(generated_pragmatic_obligation) ELSE 0::NUMERIC(23,2) END AS inner_direct_payments,
      CASE WHEN award_category = 'loans' THEN SUM(generated_pragmatic_obligation) ELSE 0::NUMERIC(23,2) END AS inner_loans,
      CASE WHEN award_category NOT IN ('contract', 'grant', 'direct payment', 'loans') THEN SUM(generated_pragmatic_obligation) ELSE 0::NUMERIC(23,2) END AS inner_other,
     SUM(generated_pragmatic_obligation) AS inner_amount,
     COUNT(*) AS inner_count
    FROM
      public.temporary_recipients_from_transactions_view AS trft
    WHERE
      trft.action_date >= now() - INTERVAL '1 year'
    GROUP BY recipient_hash, recipient_level, award_category
  )
  SELECT
    recipient_hash,
    recipient_level,
    array_agg(award_category) AS award_types,
    SUM(inner_contracts) AS last_12_contracts,
    SUM(inner_grants) AS last_12_grants,
    SUM(inner_direct_payments) AS last_12_direct_payments,
    SUM(inner_loans) AS last_12_loans,
    SUM(inner_other) AS last_12_other,
    SUM(inner_amount) AS amount,
    SUM(inner_count) AS count
  FROM
    grouped_by_category_inner AS gbci
  GROUP BY recipient_hash, recipient_level
)
UPDATE public.temporary_restock_recipient_profile AS rpv
SET
  award_types = gbc.award_types || rpv.award_types,
  last_12_months = rpv.last_12_months + gbc.amount,
  last_12_contracts = rpv.last_12_contracts + gbc.last_12_contracts,
  last_12_grants = rpv.last_12_grants + gbc.last_12_grants,
  last_12_direct_payments = rpv.last_12_direct_payments + gbc.last_12_direct_payments,
  last_12_loans = rpv.last_12_loans + gbc.last_12_loans,
  last_12_other = rpv.last_12_other + gbc.last_12_other,
  last_12_months_count = rpv.last_12_months_count + gbc.count,
  unused = false
FROM
  grouped_by_category AS gbc
WHERE
  gbc.recipient_hash = rpv.recipient_hash AND
  gbc.recipient_level = rpv.recipient_level;

--------------------------------------------------------------------------------
-- Step 4, Populate the Parent Obligation for past 12 months
--------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 4: parent records obligation'; END $$;

WITH grouped_by_parent AS (
  WITH grouped_by_parent_inner AS (
    SELECT
      parent_recipient_unique_id,
      CASE
        WHEN award_category NOT IN ('contract', 'grant', 'direct payment', 'loans')
        THEN 'other' ELSE award_category
      END AS award_category,
      CASE WHEN award_category = 'contract' THEN SUM(generated_pragmatic_obligation) ELSE 0::NUMERIC(23,2) END AS inner_contracts,
      CASE WHEN award_category = 'grant' THEN SUM(generated_pragmatic_obligation) ELSE 0::NUMERIC(23,2) END AS inner_grants,
      CASE WHEN award_category = 'direct payment' THEN SUM(generated_pragmatic_obligation) ELSE 0::NUMERIC(23,2) END AS inner_direct_payments,
      CASE WHEN award_category = 'loans' THEN SUM(generated_pragmatic_obligation) ELSE 0::NUMERIC(23,2) END AS inner_loans,
      CASE WHEN award_category NOT IN ('contract', 'grant', 'direct payment', 'loans') THEN SUM(generated_pragmatic_obligation) ELSE 0::NUMERIC(23,2) END AS inner_other,
     SUM(generated_pragmatic_obligation) AS inner_amount,
     COUNT(*) AS inner_count
    FROM
      public.temporary_recipients_from_transactions_view AS trft
    WHERE
      trft.action_date >= now() - INTERVAL '1 year' AND
      parent_recipient_unique_id IS NOT NULL
    GROUP BY parent_recipient_unique_id, award_category
  )
  SELECT
    parent_recipient_unique_id AS duns,
    array_agg(award_category) AS award_types,
    SUM(inner_contracts) AS last_12_contracts,
    SUM(inner_grants) AS last_12_grants,
    SUM(inner_direct_payments) AS last_12_direct_payments,
    SUM(inner_loans) AS last_12_loans,
    SUM(inner_other) AS last_12_other,
    SUM(inner_amount) AS amount,
    SUM(inner_count) AS count
  FROM
    grouped_by_parent_inner AS gbpi
  GROUP BY parent_recipient_unique_id
)

UPDATE public.temporary_restock_recipient_profile AS rpv
SET
  award_types = gbp.award_types || rpv.award_types,
  last_12_months = rpv.last_12_months + gbp.amount,
  last_12_contracts = rpv.last_12_contracts + gbp.last_12_contracts,
  last_12_grants = rpv.last_12_grants + gbp.last_12_grants,
  last_12_direct_payments = rpv.last_12_direct_payments + gbp.last_12_direct_payments,
  last_12_loans = rpv.last_12_loans + gbp.last_12_loans,
  last_12_other = rpv.last_12_other + gbp.last_12_other,
  last_12_months_count = rpv.last_12_months_count + gbp.count,
  unused = false
FROM
  grouped_by_parent AS gbp
WHERE
  rpv.recipient_unique_id = gbp.duns AND
  rpv.recipient_level = 'P';

--------------------------------------------------------------------------------
-- Step 5, Populating children list in parents
--------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 5: populating children list in parent records'; END $$;

WITH parent_recipients AS (
  SELECT
    parent_recipient_unique_id,
    array_agg(DISTINCT recipient_unique_id) AS duns_list
  FROM
    public.temporary_recipients_from_transactions_view
  WHERE
    parent_recipient_unique_id IS NOT NULL
  GROUP BY
    parent_recipient_unique_id
)
UPDATE public.temporary_restock_recipient_profile AS rpv
SET
  recipient_affiliations = pr.duns_list,
  unused = false

FROM parent_recipients AS pr
WHERE rpv.recipient_unique_id = pr.parent_recipient_unique_id and rpv.recipient_level = 'P';

--------------------------------------------------------------------------------
-- Step 6, Populate parent DUNS in children
--------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 6: Populating parent duns in children'; END $$;

WITH all_recipients AS (
  SELECT
    recipient_unique_id,
    array_agg(DISTINCT parent_recipient_unique_id) AS parent_duns_list
  FROM
    public.temporary_recipients_from_transactions_view
  WHERE
    recipient_unique_id IS NOT NULL AND
    parent_recipient_unique_id IS NOT NULL
  GROUP BY recipient_unique_id
)
UPDATE public.temporary_restock_recipient_profile AS rpv
SET
  recipient_affiliations = ar.parent_duns_list,
  unused = false
FROM all_recipients AS ar
WHERE
  rpv.recipient_unique_id = ar.recipient_unique_id AND
  rpv.recipient_level = 'C';


--------------------------------------------------------------------------------
-- Step 7, Mark recipient profile rows older than 12 months  as valid
--------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 7: R & C Recipient profiles older than 12 months'; END $$;

WITH grouped_by_old_recipients AS (
  SELECT
    recipient_hash,
    recipient_level
  FROM
    public.temporary_recipients_from_transactions_view AS trft
  GROUP BY recipient_hash, recipient_level
)

UPDATE public.temporary_restock_recipient_profile AS rpv
SET
  unused = false
FROM
  grouped_by_old_recipients AS gbc
WHERE
  gbc.recipient_hash = rpv.recipient_hash AND
  gbc.recipient_level = rpv.recipient_level AND
  rpv.unused = true;


--------------------------------------------------------------------------------
-- Step 8, Mark Parent recipient profile rows older than 12 months  as valid
--------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 8: Parent Recipient profiles older than 12 months'; END $$;

WITH grouped_by_parent_old AS (
  SELECT
    parent_recipient_unique_id
  FROM
    public.temporary_recipients_from_transactions_view AS trft
  WHERE
    parent_recipient_unique_id IS NOT NULL
  GROUP BY parent_recipient_unique_id
)

UPDATE public.temporary_restock_recipient_profile AS rpv
SET
  unused = false
FROM
  grouped_by_parent_old AS gbp
WHERE
  rpv.recipient_unique_id = gbp.parent_recipient_unique_id AND
  rpv.recipient_level = 'P' AND
  rpv.unused = true;


--------------------------------------------------------------------------------
-- Step 9, Finalize new table
--------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 9: almost done'; END $$;
DELETE FROM public.temporary_restock_recipient_profile WHERE unused = true;

--------------------------------------------------------------------------------
-- Step 10, Drop unnecessary relations and standup new table as final
--------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 10: restocking destination table'; END $$;

BEGIN;
TRUNCATE TABLE public.recipient_profile RESTART IDENTITY;
INSERT INTO public.recipient_profile (
    recipient_level, recipient_hash, recipient_unique_id,
    recipient_name, recipient_affiliations, award_types, last_12_months,
    last_12_contracts, last_12_loans, last_12_grants, last_12_direct_payments, last_12_other,
    last_12_months_count
    )
  SELECT recipient_level, recipient_hash, recipient_unique_id,
    recipient_name, recipient_affiliations, award_types, last_12_months,
    last_12_contracts, last_12_loans, last_12_grants, last_12_direct_payments, last_12_other,
    last_12_months_count
  FROM public.temporary_restock_recipient_profile;
DROP TABLE public.temporary_restock_recipient_profile;
DROP MATERIALIZED VIEW public.temporary_recipients_from_transactions_view;
COMMIT;

VACUUM ANALYZE public.recipient_profile;

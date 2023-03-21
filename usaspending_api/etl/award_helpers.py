from typing import Optional

from django.db import connection

general_award_update_sql_string = """
WITH
txn_earliest AS (
  SELECT DISTINCT ON (tn.award_id)
    tn.award_id,
    tn.id,
    tn.action_date,
    tn.description,
    tn.period_of_performance_start_date
  FROM vw_transaction_normalized tn
  {predicate}
  ORDER BY tn.award_id, tn.action_date ASC, tn.modification_number ASC, tn.transaction_unique_id ASC
),
txn_latest AS (
  SELECT DISTINCT ON (tn.award_id)
    tn.award_id,
    tn.id,
    tn.type,
    tn.type_description,
    tn.awarding_agency_id,
    tn.action_date,
    tn.funding_agency_id,
    tn.last_modified_date,
    tn.period_of_performance_current_end_date,
    CASE
      WHEN tn.type IN ('A', 'B', 'C', 'D')      THEN 'contract'
      WHEN tn.type IN ('02', '03', '04', '05')  THEN 'grant'
      WHEN tn.type IN ('06', '10')              THEN 'direct payment'
      WHEN tn.type IN ('07', '08')              THEN 'loans'
      WHEN tn.type = '09'                       THEN 'insurance'
      WHEN tn.type = '11'                       THEN 'other'
      WHEN tn.type LIKE 'IDV%%'                 THEN 'idv'
      ELSE NULL
    END AS category
  FROM vw_transaction_normalized tn
  {predicate}
  ORDER BY tn.award_id, tn.action_date DESC, tn.modification_number DESC, tn.transaction_unique_id DESC
),
txn_totals AS (
  SELECT
    tn.award_id,
    SUM(tn.federal_action_obligation)   AS total_obligation,
    SUM(tn.original_loan_subsidy_cost)  AS total_subsidy_cost,
    SUM(tn.funding_amount)              AS total_funding_amount,
    SUM(tn.face_value_loan_guarantee)   AS total_loan_value,
    SUM(tn.non_federal_funding_amount)  AS non_federal_funding_amount,
    SUM(tn.indirect_federal_sharing)    AS total_indirect_federal_sharing
  FROM vw_transaction_normalized tn
  {predicate}
  GROUP BY tn.award_id
)
UPDATE award_search a
SET
  update_date                             = now(),

  earliest_transaction_id                 = e.id,
  date_signed                             = e.action_date,
  description                             = e.description,
  period_of_performance_start_date        = e.period_of_performance_start_date,

  latest_transaction_id                   = l.id,
  type                                    = l.type,
  type_description                        = l.type_description,
  awarding_agency_id                      = l.awarding_agency_id,
  category                                = l.category,
  certified_date                          = l.action_date,
  fiscal_year                             = fy(l.action_date),
  funding_agency_id                       = l.funding_agency_id,
  last_modified_date                      = l.last_modified_date,
  period_of_performance_current_end_date  = l.period_of_performance_current_end_date,

  non_federal_funding_amount              = t.non_federal_funding_amount,
  total_funding_amount                    = t.total_funding_amount,
  total_loan_value                        = t.total_loan_value,
  total_obligation                        = t.total_obligation,
  total_subsidy_cost                      = t.total_subsidy_cost,
  total_indirect_federal_sharing          = t.total_indirect_federal_sharing

FROM txn_earliest e
INNER JOIN txn_latest   l ON e.award_id = l.award_id
INNER JOIN txn_totals   t ON e.award_id = t.award_id

WHERE
  e.award_id = a.award_id
  AND (
       a.earliest_transaction_id                 IS DISTINCT FROM e.id
    OR a.date_signed                             IS DISTINCT FROM e.action_date
    OR a.description                             IS DISTINCT FROM e.description
    OR a.period_of_performance_start_date        IS DISTINCT FROM e.period_of_performance_start_date
    OR a.latest_transaction_id                   IS DISTINCT FROM l.id
    OR a.type                                    IS DISTINCT FROM l.type
    OR a.type_description                        IS DISTINCT FROM l.type_description
    OR a.awarding_agency_id                      IS DISTINCT FROM l.awarding_agency_id
    OR a.category                                IS DISTINCT FROM l.category
    OR a.certified_date                          IS DISTINCT FROM l.action_date
    OR a.fiscal_year                             IS DISTINCT FROM fy(l.action_date)
    OR a.funding_agency_id                       IS DISTINCT FROM l.funding_agency_id
    OR a.last_modified_date                      IS DISTINCT FROM l.last_modified_date
    OR a.period_of_performance_current_end_date  IS DISTINCT FROM l.period_of_performance_current_end_date
    OR a.non_federal_funding_amount              IS DISTINCT FROM t.non_federal_funding_amount
    OR a.total_funding_amount                    IS DISTINCT FROM t.total_funding_amount
    OR a.total_loan_value                        IS DISTINCT FROM t.total_loan_value
    OR a.total_obligation                        IS DISTINCT FROM t.total_obligation
    OR a.total_subsidy_cost                      IS DISTINCT FROM t.total_subsidy_cost
    or a.total_indirect_federal_sharing          IS DISTINCT FROM t.total_indirect_federal_sharing
  )
"""

fpds_award_update_sql_string = """
WITH
fpds_totals AS (
  SELECT
    tn.award_id,
    SUM(tf.base_and_all_options_value::NUMERIC(23,2)) AS total_base_and_options_value,
    SUM(tf.base_exercised_options_val::NUMERIC(23,2)) AS base_exercised_options_val
  FROM vw_transaction_normalized AS tn
  INNER JOIN vw_transaction_fpds AS tf ON tn.id = tf.transaction_id
  {predicate}
  GROUP BY tn.award_id
),
txn_latest AS (
  SELECT DISTINCT ON (tn.award_id)
    tn.award_id,
    tf.agency_id,
    tf.referenced_idv_agency_iden
  FROM vw_transaction_normalized AS tn
  INNER JOIN vw_transaction_fpds AS tf ON tn.id = tf.transaction_id
  {predicate}
  ORDER BY tn.award_id, tn.action_date DESC, tn.modification_number DESC, tn.transaction_unique_id DESC
),
executive_comp AS (
  WITH sub_cte_all_transactions AS (
    SELECT
      tn.award_id,
      fpds.officer_1_amount,
      fpds.officer_1_name,
      fpds.officer_2_amount,
      fpds.officer_2_name,
      fpds.officer_3_amount,
      fpds.officer_3_name,
      fpds.officer_4_amount,
      fpds.officer_4_name,
      fpds.officer_5_amount,
      fpds.officer_5_name
    FROM vw_transaction_normalized tn
    INNER JOIN vw_transaction_fpds AS fpds ON tn.id = fpds.transaction_id
    {predicate}
    ORDER BY tn.award_id, tn.action_date DESC, tn.modification_number DESC, tn.transaction_unique_id DESC
  )
  SELECT DISTINCT ON (award_id) * FROM sub_cte_all_transactions WHERE officer_1_name is not null
)
UPDATE award_search a
SET
  update_date                 = now(),
  base_and_all_options_value  = t.total_base_and_options_value,
  base_exercised_options_val  = t.base_exercised_options_val,

  fpds_agency_id              = l.agency_id,
  fpds_parent_agency_id       = l.referenced_idv_agency_iden,

  officer_1_amount            = ec.officer_1_amount,
  officer_1_name              = ec.officer_1_name,
  officer_2_amount            = ec.officer_2_amount,
  officer_2_name              = ec.officer_2_name,
  officer_3_amount            = ec.officer_3_amount,
  officer_3_name              = ec.officer_3_name,
  officer_4_amount            = ec.officer_4_amount,
  officer_4_name              = ec.officer_4_name,
  officer_5_amount            = ec.officer_5_amount,
  officer_5_name              = ec.officer_5_name
FROM fpds_totals AS t
INNER JOIN txn_latest AS l ON t.award_id = l.award_id
LEFT OUTER JOIN executive_comp AS ec ON t.award_id = ec.award_id
WHERE
  t.award_id = a.award_id
  AND (
        a.base_and_all_options_value IS DISTINCT FROM t.total_base_and_options_value
     OR a.base_exercised_options_val IS DISTINCT FROM t.base_exercised_options_val
     OR a.fpds_agency_id             IS DISTINCT FROM l.agency_id
     OR a.fpds_parent_agency_id      IS DISTINCT FROM l.referenced_idv_agency_iden
     OR a.officer_1_amount           IS DISTINCT FROM ec.officer_1_amount
     OR a.officer_1_name             IS DISTINCT FROM ec.officer_1_name
     OR a.officer_2_amount           IS DISTINCT FROM ec.officer_2_amount
     OR a.officer_2_name             IS DISTINCT FROM ec.officer_2_name
     OR a.officer_3_amount           IS DISTINCT FROM ec.officer_3_amount
     OR a.officer_3_name             IS DISTINCT FROM ec.officer_3_name
     OR a.officer_4_amount           IS DISTINCT FROM ec.officer_4_amount
     OR a.officer_4_name             IS DISTINCT FROM ec.officer_4_name
     OR a.officer_5_amount           IS DISTINCT FROM ec.officer_5_amount
     OR a.officer_5_name             IS DISTINCT FROM ec.officer_5_name
  )
"""

fabs_award_update_sql_string = """
WITH
  executive_comp AS (
    WITH sub_cte_all_transactions AS (
      SELECT
        tn.award_id,
        fabs.officer_1_amount,
        fabs.officer_1_name,
        fabs.officer_2_amount,
        fabs.officer_2_name,
        fabs.officer_3_amount,
        fabs.officer_3_name,
        fabs.officer_4_amount,
        fabs.officer_4_name,
        fabs.officer_5_amount,
        fabs.officer_5_name
      FROM vw_transaction_normalized tn
      INNER JOIN vw_transaction_fabs AS fabs ON tn.id = fabs.transaction_id
      {predicate}
      ORDER BY tn.award_id, tn.action_date DESC, tn.modification_number DESC, tn.transaction_unique_id DESC
    )
    SELECT DISTINCT ON (award_id) * FROM sub_cte_all_transactions WHERE officer_1_name is not null
)
UPDATE award_search a
SET
  update_date       = now(),
  officer_1_amount  = ec.officer_1_amount,
  officer_1_name    = ec.officer_1_name,
  officer_2_amount  = ec.officer_2_amount,
  officer_2_name    = ec.officer_2_name,
  officer_3_amount  = ec.officer_3_amount,
  officer_3_name    = ec.officer_3_name,
  officer_4_amount  = ec.officer_4_amount,
  officer_4_name    = ec.officer_4_name,
  officer_5_amount  = ec.officer_5_amount,
  officer_5_name    = ec.officer_5_name
FROM executive_comp AS ec
WHERE
  ec.award_id = a.award_id
  AND (
       a.officer_1_amount IS DISTINCT FROM ec.officer_1_amount
    OR a.officer_1_name   IS DISTINCT FROM ec.officer_1_name
    OR a.officer_2_amount IS DISTINCT FROM ec.officer_2_amount
    OR a.officer_2_name   IS DISTINCT FROM ec.officer_2_name
    OR a.officer_3_amount IS DISTINCT FROM ec.officer_3_amount
    OR a.officer_3_name   IS DISTINCT FROM ec.officer_3_name
    OR a.officer_4_amount IS DISTINCT FROM ec.officer_4_amount
    OR a.officer_4_name   IS DISTINCT FROM ec.officer_4_name
    OR a.officer_5_amount IS DISTINCT FROM ec.officer_5_amount
    OR a.officer_5_name   IS DISTINCT FROM ec.officer_5_name
  )
"""


subaward_award_update_sql_string = """
  WITH subaward_totals AS (
    SELECT
      award_id,
      SUM(amount) AS total_subaward_amount,
      COUNT(*)    AS subaward_count
    FROM subaward
    {predicate}
    GROUP BY award_id
  )
  UPDATE award_search a
    SET
      update_date           = now(),
      total_subaward_amount = subaward_totals.total_subaward_amount,
      subaward_count        = subaward_totals.subaward_count
    FROM subaward_totals
    WHERE
      subaward_totals.award_id = a.award_id
      AND (
           a.total_subaward_amount  IS DISTINCT FROM subaward_totals.total_subaward_amount
        OR a.subaward_count         IS DISTINCT FROM subaward_totals.subaward_count
      )
"""


def execute_database_statement(sql: str, values: Optional[list] = None) -> int:
    """Execute the SQL and return the UPDATE count"""
    with connection.cursor() as cursor:
        if values:
            cursor.execute(sql, values)
        else:
            cursor.execute(sql)

        rowcount = cursor.rowcount

    return rowcount


def convert_award_id_to_guai(award_tuple: tuple) -> tuple:
    """Scafolding code between award PK ids and unique award ids"""
    sql = "SELECT generated_unique_award_id FROM vw_awards WHERE id IN %s"
    values = [award_tuple]
    with connection.cursor() as cursor:
        cursor.execute(sql, values)
        return tuple([row[0] for row in cursor.fetchall()])


def update_awards(award_tuple: Optional[tuple] = None) -> int:
    """Update Award records using transaction data"""

    if award_tuple:
        values = [award_tuple, award_tuple, award_tuple]
        predicate = "WHERE tn.award_id IN %s"
    else:
        values = None
        predicate = ""

    return execute_database_statement(general_award_update_sql_string.format(predicate=predicate), values)


def prune_empty_awards(award_tuple: Optional[tuple] = None) -> int:
    _find_empty_awards_sql = """
        SELECT a.id
        FROM vw_awards a
        LEFT JOIN vw_transaction_normalized tn ON tn.award_id = a.id
        WHERE tn IS NULL {}
    """.format(
        "AND a.id IN %s" if award_tuple else ""
    )

    _modify_subawards_sql = "UPDATE subaward_search SET award_id = null WHERE award_id IN ({});".format(
        _find_empty_awards_sql
    )

    _modify_financial_accounts_sql = """
      UPDATE financial_accounts_by_awards
        SET
          update_date = now(),
          award_id = null
      WHERE award_id IN ({});
    """.format(
        _find_empty_awards_sql
    )

    _delete_parent_award_sql = "DELETE FROM parent_award WHERE award_id in ({});".format(_find_empty_awards_sql)

    _prune_empty_awards_sql = "DELETE FROM award_search WHERE award_id IN ({}) ".format(_find_empty_awards_sql)

    return execute_database_statement(
        _modify_subawards_sql + _modify_financial_accounts_sql + _delete_parent_award_sql + _prune_empty_awards_sql,
        [award_tuple, award_tuple, award_tuple, award_tuple],
    )


def update_assistance_awards(award_tuple: Optional[tuple] = None) -> int:
    """Update assistance-specific award data based on the info in child transactions."""
    if award_tuple:
        values = [award_tuple]
        predicate = "WHERE tn.award_id IN %s"
    else:
        values = None
        predicate = ""

    return execute_database_statement(fabs_award_update_sql_string.format(predicate=predicate), values)


def update_procurement_awards(award_tuple: Optional[tuple] = None) -> int:
    """Update procurement-specific award data based on the info in child transactions."""
    if award_tuple:
        values = [award_tuple, award_tuple, award_tuple]
        predicate = "WHERE tn.award_id IN %s"
    else:
        values = None
        predicate = ""

    return execute_database_statement(fpds_award_update_sql_string.format(predicate=predicate), values)


def update_award_subawards(award_tuple: Optional[tuple] = None) -> int:
    """Updates awards' subaward counts and totals"""

    if award_tuple:
        values = [award_tuple]
        predicate = "WHERE award_id IN %s"
    else:
        values = None
        predicate = ""

    return execute_database_statement(subaward_award_update_sql_string.format(predicate=predicate), values)

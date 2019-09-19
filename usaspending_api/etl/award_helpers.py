from typing import Optional

from django.db import connection


def execute_database_insert_statement(sql: str, values: Optional[list] = None) -> int:
    """Execute the SQL and return the UPDATE count"""
    with connection.cursor() as cursor:
        if values:
            cursor.execute(sql, values)
        else:
            cursor.execute(sql)
        rowcount = cursor.rowcount

    return rowcount


def update_awards(award_tuple: Optional[tuple] = None) -> int:
    """Update Award records in `awards` using transaction data

    Awards can have one or more transactions. We maintain some information on
    the award model that needs to be updated as its transactions change.
    For example, an award's total obligated amount represents the summary of its
    transaction's obligated amounts. Another example is a series of fields
    (award type, awarding agency, etc.) that will always be set to the value of
    the Award's most recent transaction.

    This function keeps those awards fields synced with transactions.
    Obviously the raw SQL is not ideal. That said, the complex update of award
    fields based on the earliest, latest, and aggregate values of the
    transactions was problematic to do in a set-based way via the ORM. These
    updates do need to be set-based, as looping through and updating individual
    award records would be an ETL bottleneck.
    """

    _earliest_transaction_cte = str(
        "txn_earliest AS ( "
        "  SELECT DISTINCT ON (award_id) "
        "    award_id, "
        "    id, "
        "    action_date, "
        "    description, "
        "    period_of_performance_start_date "
        "  FROM transaction_normalized "
        "  {} "
        "  ORDER BY award_id, action_date ASC, modification_number ASC "
        ")"
    )
    _latest_transaction_cte = str(
        "txn_latest AS ( "
        "  SELECT DISTINCT ON (award_id) "
        "    award_id, "
        "    id, "
        "    awarding_agency_id, "
        "    action_date, "
        "    funding_agency_id, "
        "    last_modified_date, "
        "    period_of_performance_current_end_date, "
        "    place_of_performance_id, "
        "    recipient_id, "
        "    type, "
        "    type_description, "
        "    CASE WHEN type IN ('A', 'B', 'C', 'D') THEN 'contract' "
        "      WHEN type IN ('02', '03', '04', '05') THEN 'grant' "
        "      WHEN type in ('06', '10') THEN 'direct payment' "
        "      WHEN type in ('07', '08') THEN 'loans' "
        "      WHEN type = '09' THEN 'insurance' "
        "      WHEN type = '11' THEN 'other' "
        "      WHEN type LIKE 'IDV%%' THEN 'idv' "
        "      ELSE NULL END AS category "
        "  FROM transaction_normalized "
        "  {} "
        "  ORDER BY award_id, action_date DESC, modification_number DESC "
        ")"
    )
    _aggregate_transaction_cte = str(
        "txn_totals AS ( "
        "  SELECT "
        "    award_id, "
        "    SUM(federal_action_obligation) AS total_obligation, "
        "    SUM(original_loan_subsidy_cost) AS total_subsidy_cost, "
        "    SUM(funding_amount) AS total_funding_amount, "
        "    SUM(face_value_loan_guarantee) AS total_loan_value, "
        "    SUM(non_federal_funding_amount) AS non_federal_funding_amount "
        "  FROM transaction_normalized "
        "  {} "
        "  GROUP BY award_id "
        ")"
    )

    if award_tuple:
        values = [award_tuple, award_tuple, award_tuple]
        earliest_transaction_cte = _earliest_transaction_cte.format(" WHERE award_id IN %s ")
        latest_transaction_cte = _latest_transaction_cte.format(" WHERE award_id IN %s ")
        aggregate_transaction_cte = _aggregate_transaction_cte.format(" WHERE award_id IN %s ")
    else:
        values = None
        earliest_transaction_cte = _earliest_transaction_cte.format("")
        latest_transaction_cte = _latest_transaction_cte.format("")
        aggregate_transaction_cte = _aggregate_transaction_cte.format("")

    # construct a sql query that uses the common table expressions defined above
    # and joins each of them to their corresopnding award.
    # The joined data from the CTEs are used to update awards fields as appropriate
    _sql_update = str(
        "WITH {}, {}, {} "
        "UPDATE awards a "
        "  SET "
        "    earliest_transaction_id = e.id, "
        "    date_signed = e.action_date, "
        "    description = e.description, "
        "    period_of_performance_start_date = e.period_of_performance_start_date, "
        ""
        "    latest_transaction_id = l.id, "
        "    awarding_agency_id = l.awarding_agency_id, "
        "    category = l.category, "
        "    certified_date = l.action_date, "
        "    funding_agency_id = l.funding_agency_id, "
        "    last_modified_date = l.last_modified_date, "
        "    period_of_performance_current_end_date = l.period_of_performance_current_end_date, "
        "    place_of_performance_id = l.place_of_performance_id, "
        "    recipient_id = l.recipient_id, "
        "    type = l.type, "
        "    type_description = l.type_description, "
        ""
        "    non_federal_funding_amount = t.non_federal_funding_amount, "
        "    total_funding_amount = t.total_funding_amount, "
        "    total_loan_value = t.total_loan_value, "
        "    total_obligation = t.total_obligation, "
        "    total_subsidy_cost = t.total_subsidy_cost "
        ""
        "  FROM txn_earliest e "
        "  JOIN txn_latest l ON e.award_id = l.award_id "
        "  JOIN txn_totals t ON e.award_id = t.award_id "
        "  WHERE e.award_id = a.id "
    )

    sql_update = _sql_update.format(earliest_transaction_cte, latest_transaction_cte, aggregate_transaction_cte)
    return execute_database_insert_statement(sql_update, values)


def update_assistance_awards(award_tuple: Optional[tuple] = None) -> int:
    _sql_update = str(
        "WITH executive_comp AS ( "
        "  SELECT DISTINCT ON (tn.award_id) "
        "    tn.award_id, "
        "    fabs.officer_1_amount, "
        "    fabs.officer_1_name, "
        "    fabs.officer_2_amount, "
        "    fabs.officer_2_name, "
        "    fabs.officer_3_amount, "
        "    fabs.officer_3_name, "
        "    fabs.officer_4_amount, "
        "    fabs.officer_4_name, "
        "    fabs.officer_5_amount, "
        "    fabs.officer_5_name "
        "  FROM transaction_normalized tn "
        "  INNER JOIN transaction_fabs AS fabs ON tn.id = fabs.transaction_id "
        "  WHERE fabs.officer_1_name IS NOT NULL {} "
        "  ORDER BY tn.award_id, tn.action_date DESC, tn.modification_number DESC "
        ") "
        "UPDATE awards a "
        "  SET "
        "    officer_1_amount = ec.officer_1_amount, "
        "    officer_1_name = ec.officer_1_name, "
        "    officer_2_amount = ec.officer_2_amount, "
        "    officer_2_name = ec.officer_2_name, "
        "    officer_3_amount = ec.officer_3_amount, "
        "    officer_3_name = ec.officer_3_name, "
        "    officer_4_amount = ec.officer_4_amount, "
        "    officer_4_name = ec.officer_4_name, "
        "    officer_5_amount = ec.officer_5_amount, "
        "    officer_5_name = ec.officer_5_name "
        "  FROM executive_comp AS ec "
        "  WHERE ec.award_id = a.id "
    )

    if award_tuple:
        values = [award_tuple]
        sql_update = _sql_update.format("AND award_id IN %s ")
    else:
        values = None
        sql_update = _sql_update.format("")

    return execute_database_insert_statement(sql_update, values)


def update_contract_awards(award_tuple: Optional[tuple] = None) -> int:
    """Update contract-specific award data based on the info in child transactions."""

    _aggregate_transaction_cte = str(
        "txn_totals AS ( "
        "  SELECT "
        "    tx.award_id, "
        "    SUM(CAST(f.base_and_all_options_value AS double precision)) AS total_base_and_options_value, "
        "    SUM(CAST(f.base_exercised_options_val AS double precision)) AS base_exercised_options_val "
        "  FROM transaction_normalized AS tx "
        "  INNER JOIN transaction_fpds AS f ON tx.id = f.transaction_id "
        "  {} "
        "  GROUP BY tx.award_id "
        ") "
    )

    # Gather additional fpds fields such as agency_ids and types
    _extra_fpds_fields = str(
        "extra_fpds_fields AS ( "
        "  SELECT "
        "    tx.award_id, "
        "    CASE "
        "      WHEN pulled_from IS DISTINCT FROM 'IDV' THEN contract_award_type "
        "      WHEN idv_type = 'B' AND type_of_idc IS NOT NULL THEN CONCAT('IDV_B_', type_of_idc::text) "
        "      WHEN idv_type = 'B' AND type_of_idc IS NULL and "
        "        type_of_idc_description = 'INDEFINITE DELIVERY / REQUIREMENTS' THEN 'IDV_B_A' "
        "      WHEN idv_type = 'B' AND type_of_idc IS NULL and "
        "        type_of_idc_description = 'INDEFINITE DELIVERY / INDEFINITE QUANTITY' THEN 'IDV_B_B' "
        "      WHEN idv_type = 'B' AND type_of_idc IS NULL and "
        "        type_of_idc_description = 'INDEFINITE DELIVERY / DEFINITE QUANTITY' THEN 'IDV_B_C' "
        "      ELSE CONCAT('IDV_', idv_type::text) END AS type, "
        "    CASE WHEN pulled_from IS DISTINCT FROM 'IDV' THEN contract_award_type_desc "
        "      WHEN idv_type = 'B' AND "
        "        (type_of_idc_description IS DISTINCT FROM NULL AND type_of_idc_description <> 'NAN') "
        "        THEN type_of_idc_description "
        "      WHEN idv_type = 'B' THEN 'INDEFINITE DELIVERY CONTRACT' "
        "      ELSE idv_type_description END AS type_description, "
        "    agency_id, "
        "    referenced_idv_agency_iden "
        "  FROM transaction_normalized AS tx "
        "  INNER JOIN transaction_fpds AS f ON tx.id = f.transaction_id "
        "  {}"
        ")"
    )

    _executive_comp_cte = str(
        "executive_comp AS ( "
        "  SELECT DISTINCT ON (tn.award_id) "
        "    tn.award_id, "
        "    fpds.officer_1_amount, "
        "    fpds.officer_1_name, "
        "    fpds.officer_2_amount, "
        "    fpds.officer_2_name, "
        "    fpds.officer_3_amount, "
        "    fpds.officer_3_name, "
        "    fpds.officer_4_amount, "
        "    fpds.officer_4_name, "
        "    fpds.officer_5_amount, "
        "    fpds.officer_5_name "
        "  FROM transaction_normalized tn "
        "  INNER JOIN transaction_fpds AS fpds ON tn.id = fpds.transaction_id "
        "  WHERE fpds.officer_1_name IS NOT NULL {} "
        "  ORDER BY tn.award_id, tn.action_date DESC, tn.modification_number DESC "
        ") "
    )

    if award_tuple:
        values = [award_tuple, award_tuple, award_tuple]
        aggregate_transaction_cte = _aggregate_transaction_cte.format(" WHERE tx.award_id IN %s ")
        extra_fpds_fields = _extra_fpds_fields.format(" WHERE tx.award_id IN %s ")
        executive_comp_cte = _executive_comp_cte.format(" AND tn.award_id IN %s ")
    else:
        values = None
        aggregate_transaction_cte = _aggregate_transaction_cte.format("")
        extra_fpds_fields = _extra_fpds_fields.format("")
        executive_comp_cte = _executive_comp_cte.format("")
    # construct a sql query that uses the latest txn contract common table expression above and joins it to the
    # corresponding award. that joined data is used to update awards fields as appropriate (currently, there's only one
    # trasnaction_contract field that trickles up and updates an award record: base_and_all_options_value)
    _sql_update = str(
        "WITH {}, {}, {} "
        "UPDATE awards a "
        "  SET "
        "    base_and_all_options_value = t.total_base_and_options_value, "
        "    base_exercised_options_val = t.base_exercised_options_val, "
        ""
        "    type = eff.type, "
        "    type_description = eff.type_description, "
        "    fpds_agency_id = eff.agency_id, "
        "    fpds_parent_agency_id = eff.referenced_idv_agency_iden, "
        ""
        "    officer_1_amount = ec.officer_1_amount, "
        "    officer_1_name = ec.officer_1_name, "
        "    officer_2_amount = ec.officer_2_amount, "
        "    officer_2_name = ec.officer_2_name, "
        "    officer_3_amount = ec.officer_3_amount, "
        "    officer_3_name = ec.officer_3_name, "
        "    officer_4_amount = ec.officer_4_amount, "
        "    officer_4_name = ec.officer_4_name, "
        "    officer_5_amount = ec.officer_5_amount, "
        "    officer_5_name = ec.officer_5_name "
        "  FROM txn_totals AS t "
        "  INNER JOIN extra_fpds_fields AS eff ON t.award_id = eff.award_id "
        "  LEFT JOIN executive_comp AS ec ON t.award_id = ec.award_id "
        "  WHERE t.award_id = a.id "
    )

    sql_update = _sql_update.format(aggregate_transaction_cte, extra_fpds_fields, executive_comp_cte)
    return execute_database_insert_statement(sql_update, values)


def update_award_subawards(award_tuple: Optional[tuple] = None) -> int:
    """Updates awards' subaward counts and totals"""

    _sql_sub_totals = str(
        "subaward_totals AS ( "
        "  SELECT "
        "    award_id, "
        "    SUM(amount) AS total_subaward_amount, "
        "    COUNT(*) AS subaward_count "
        "  FROM subaward "
        "  {} "
        "  GROUP BY award_id"
        ") "
    )
    if award_tuple:
        values = [award_tuple]
        sql_sub_totals = _sql_sub_totals.format("WHERE award_id IN %s ")
    else:
        values = None
        sql_sub_totals = _sql_sub_totals.format("")

    _sql_update = str(
        "WITH {} "
        "UPDATE awards "
        "  SET "
        "    total_subaward_amount = subaward_totals.total_subaward_amount, "
        "    subaward_count = subaward_totals.subaward_count "
        "  FROM subaward_totals "
        "  WHERE subaward_totals.award_id = id "
    )

    sql_update = _sql_update.format(sql_sub_totals)
    return execute_database_insert_statement(sql_update, values)

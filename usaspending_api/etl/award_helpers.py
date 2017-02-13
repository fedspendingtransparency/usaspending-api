from django.db import connection


def update_awards():
    """
    Awards can have one or more transactions. We maintain some information
    on the award model that needs to be updated as its child transactions
    change. For example, an award's total obligated amount represents the
    summary of its transaction's obligated amounts. Another example is a
    series of fields (award type, awarding agency, etc.) that will always
    be set to the value of the Award's most recent transaction.

    This function keeps those awards fields synced with child transactions.
    Obviously the raw SQL is not ideal. That said, the complex update
    of award fields based on the earliest, latest, and aggregate values
    of the child transactions was problematic to do in a set-based way
    via the ORM. These updates do need to be set-based, as looping through
    and updating individual award records would be an ETL bottleneck.

    TODO: modify this to work with specific awards, so we aren't updating
    every award, all the time
    """

    sql_update = (
        # The first part of this sql statement creates a series of common
        # table expressions that will be used to update their corresponding
        # award records. These could also have been written as sub-queries
        # inline with the update SQL, but this syntax is more readable since
        # it separates the the "tables" from the update part of the SQL

        # create a table of the most recent txn for each award
        'WITH txn_latest AS ('
        'SELECT DISTINCT ON (award_id) * '
        'FROM transaction '
        'ORDER BY award_id, action_date DESC '
        # create a table of the earliest txn for each award
        '), txn_earliest AS ('
        'SELECT DISTINCT ON (award_id) * '
        'FROM transaction '
        'ORDER BY award_id, action_date'
        # create a table that summarizes txn values for each award
        # (currently, the only value we aggregate across txns is obligated $)
        '), txn_totals AS ('
        'SELECT award_id, SUM(federal_action_obligation) AS total_obligation '
        'FROM transaction '
        'GROUP BY award_id'
        ')'
        # now join awards to each item above. in other words, each award will be joined
        # to its most recent transaction, its earliest transaction, and
        # its summarized transaction values, and that information will be used
        # to update the award record.
        'UPDATE awards a '
        'SET awarding_agency_id = l.awarding_agency_id, '
        'certified_date = l.certified_date, '
        'data_source = l.data_source, '
        'date_signed = e.action_date, '
        'description = l.description, '
        'funding_agency_id = l.funding_agency_id, '
        'last_modified_date = l.last_modified_date, '
        'latest_submission_id = l.submission_id, '
        'period_of_performance_current_end_date = l.period_of_performance_current_end_date, '
        'period_of_performance_start_date = e.period_of_performance_start_date, '
        'place_of_performance_id = l.place_of_performance_id, '
        'recipient_id = l.recipient_id, '
        'total_obligation = t.total_obligation, '
        'type = l.type, '
        'type_description = l.type_description '
        'FROM txn_earliest e '
        'JOIN txn_latest l '
        'ON e.award_id = l.award_id '
        'JOIN txn_totals t '
        'ON l.award_id = t.award_id '
        'WHERE t.award_id = a.id'
    )

    with connection.cursor() as cursor:
        cursor.execute(sql_update)
        rows = cursor.rowcount

    return rows


def update_contract_awards():
    """Update contract-specific award data based on the info in child transactions."""
    sql_update = (
        # the next four lines create a common table expression of each
        # award's most recent TransactionContract record.
        'WITH txn_latest AS ('
        'SELECT DISTINCT ON (award_id) award_id, potential_total_value_of_award '
        'FROM transaction AS t JOIN transaction_contract AS tc ON t.id = tc.transaction_id '
        'ORDER BY award_id, action_date DESC )'
        # update award's potential_total_value_of_award based on the value
        # of its most recent txn contract record from the txn_latest table above
        'UPDATE awards a '
        'SET potential_total_value_of_award = l.potential_total_value_of_award '
        'FROM txn_latest l '
        'WHERE l.award_id = a.id'
    )

    with connection.cursor() as cursor:
        cursor.execute(sql_update)
        rows = cursor.rowcount

    return rows

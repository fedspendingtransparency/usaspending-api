from django.db import connection

from usaspending_api.awards.models import Transaction


def update_awards(award_tuple=None):
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
    """

    # common table expression for each award's latest transaction
    sql_txn_latest = (
        'txn_latest AS ('
        'SELECT DISTINCT ON (award_id) * '
        'FROM transaction ')
    if award_tuple:
        sql_txn_latest += 'WHERE award_id IN %s '
    sql_txn_latest += 'ORDER BY award_id, action_date DESC) '

    # common table expression for each award's earliest transaction
    sql_txn_earliest = (
        'txn_earliest AS ('
        'SELECT DISTINCT ON (award_id) * '
        'FROM transaction ')
    if award_tuple:
        sql_txn_earliest += 'WHERE award_id IN %s '
    sql_txn_earliest += 'ORDER BY award_id, action_date) '

    # common table expression for each award's summarized data
    # (currently the only we summarize is federal_actio_obligation,
    # but we can add more as necessar)
    sql_txn_totals = (
        'txn_totals AS ('
        'SELECT award_id, SUM(federal_action_obligation) AS total_obligation '
        'FROM transaction ')
    if award_tuple:
        sql_txn_totals += 'WHERE award_id IN %s '
    sql_txn_totals += 'GROUP BY award_id) '

    # construct a sql query that uses the common table expressions
    # defined above and joins each of them to their corresopnding
    # award. the joined data from earliest, latest, and summarized
    # transactions are used to update awards fields as appropriate
    sql_update = 'WITH {}, {}, {}'.format(sql_txn_latest, sql_txn_earliest, sql_txn_totals)
    sql_update += (
        'UPDATE awards a '
        'SET awarding_agency_id = l.awarding_agency_id, '
        'certified_date = l.certified_date, '
        'data_source = l.data_source, '
        'date_signed = e.action_date, '
        'description = e.description, '
        'funding_agency_id = l.funding_agency_id, '
        'last_modified_date = l.last_modified_date, '
        'latest_submission_id = l.submission_id, '
        'period_of_performance_current_end_date = l.period_of_performance_current_end_date, '
        'period_of_performance_start_date = e.period_of_performance_start_date, '
        'place_of_performance_id = l.place_of_performance_id, '
        'recipient_id = l.recipient_id, '
        'total_obligation = t.total_obligation, '
        'latest_transaction_id = l.id, '
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
        # If another expression is added and includes %s, you must add the tuple
        # for that string interpolation to this list (even if it uses the same one!)
        cursor.execute(sql_update, [award_tuple, award_tuple, award_tuple])
        rows = cursor.rowcount

    return rows


def update_contract_awards(award_tuple=None):
    """Update contract-specific award data based on the info in child transactions."""

    # sum the potential_total_value_of_award from contract_data for an award
    sql_txn_totals = (
        'txn_totals AS ('
        'SELECT tx.award_id, SUM(potential_total_value_of_award) AS total_potential_award '
        'FROM transaction_contract INNER JOIN transaction as tx on '
        'transaction_contract.transaction_id = tx.id ')
    if award_tuple:
        sql_txn_totals += 'WHERE tx.award_id IN %s '
    sql_txn_totals += 'GROUP BY tx.award_id) '

    # construct a sql query that uses the latest txn contract common table
    # expression above and joins it to the corresopnding
    # award. that joined data is used to update awards fields as appropriate
    # (currently, there's only one trasnaction_contract field that trickles
    # up and updates an award record: potential_total_value_of_award)
    sql_update = 'WITH {}'.format(sql_txn_totals)
    sql_update += (
        'UPDATE awards a '
        'SET potential_total_value_of_award = t.total_potential_award '
        'FROM txn_totals t '
        'WHERE t.award_id = a.id'
    )

    with connection.cursor() as cursor:
        # If another expression is added and includes %s, you must add the tuple
        # for that string interpolation to this list (even if it uses the same one!)
        cursor.execute(sql_update, [award_tuple])
        rows = cursor.rowcount

    return rows


def get_award_financial_transaction(
        toptier_agency_cgac, piid=None, parent_award_id=None, fain=None, uri=None):
    """
    For specified award financial (aka "File C") data, try to find a matching
    transaction (aka "File D"). We sometimes need to do this  because File C
    doesn't always have the level of award/transaction specificity that we
    want, so we try to find a matching File D record to grab the additional
    information.

    For example, when trying to match award financial information to an
    award record, we need the awarding subtier agency, which isn't supplied
    on File C. Thus, we'll use this function to find a File D record and
    use the subtier agency information supplied there.

    If we find more than one match, return the record with this most
    recent action date.

    Args:
        toptier_agency_cgac: top tier agency code (aka CGAC code) from File C
        piid: piid from File C (contract awards only)
        parent_award_id: parent award id from File C (contract awards only)
        fain: fain from File C (assistance awards only)
        uri: uri from File C (assistance awards only)

    Returns:
        A Transaction model instance
    """
    # if both fain and uri are supplied as paramaters, look up by fain first
    incoming_fain = fain
    incoming_uri = uri
    if incoming_fain is not None and incoming_uri is not None:
        uri = None

    txn = Transaction.objects.filter(
        awarding_agency__toptier_agency__cgac_code=toptier_agency_cgac,
        contract_data__piid=piid,
        contract_data__parent_award_id=parent_award_id,
        assistance_data__fain=fain,
        assistance_data__uri=uri) \
        .order_by('-action_date').first()

    if txn is None and incoming_fain is not None and incoming_uri is not None:
        # we didn't find a match and both fain and uri were supplied
        # as parameters, now try searching by uri
        uri = incoming_uri
        txn = Transaction.objects.filter(
            awarding_agency__toptier_agency__cgac_code=toptier_agency_cgac,
            contract_data__piid=piid,
            contract_data__parent_award_id=parent_award_id,
            assistance_data__fain=None,
            assistance_data__uri=uri) \
            .order_by('-action_date').first()

    return txn

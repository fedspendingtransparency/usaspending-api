from django.db import connection
from usaspending_api.awards.models import Award, Agency
from usaspending_api.awards.models import TransactionNormalized
from django.db.models import Case, Value, When, TextField


def update_awards(award_tuple=None):
    """
    Awards can have one or more transactions. We maintain some information on the award model that needs to be updated
    as its child transactions change. For example, an award's total obligated amount represents the summary of its
    transaction's obligated amounts. Another example is a series of fields (award type, awarding agency, etc.) that
    will always be set to the value of the Award's most recent transaction.

    This function keeps those awards fields synced with child transactions. Obviously the raw SQL is not ideal.
    That said, the complex update of award fields based on the earliest, latest, and aggregate values of the child
    transactions was problematic to do in a set-based way via the ORM. These updates do need to be set-based, as
    looping through and updating individual award records would be an ETL bottleneck.
    """

    # common table expression for each award's latest transaction
    sql_txn_latest = (
        'txn_latest AS ('
        'SELECT DISTINCT ON (award_id) * '
        'FROM transaction_normalized ')
    if award_tuple:
        sql_txn_latest += 'WHERE award_id IN %s '
    sql_txn_latest += 'ORDER BY award_id, action_date DESC) '

    # common table expression for each award's earliest transaction
    sql_txn_earliest = (
        'txn_earliest AS ('
        'SELECT DISTINCT ON (award_id) * '
        'FROM transaction_normalized ')
    if award_tuple:
        sql_txn_earliest += 'WHERE award_id IN %s '
    sql_txn_earliest += 'ORDER BY award_id, action_date) '

    # common table expression for each award's summarized data (currently the only we summarize is
    # federal_actio_obligation, but we can add more as necessary)
    sql_txn_totals = (
        'txn_totals AS ('
        'SELECT award_id, SUM(federal_action_obligation) AS total_obligation '
        'FROM transaction_normalized ')
    if award_tuple:
        sql_txn_totals += 'WHERE award_id IN %s '
    sql_txn_totals += 'GROUP BY award_id) '

    # construct a sql query that uses the common table expressions defined above and joins each of them to their
    # corresopnding award. the joined data from earliest, latest, and summarized transactions are used to update awards
    # fields as appropriate
    sql_update = 'WITH {}, {}, {}'.format(sql_txn_latest, sql_txn_earliest, sql_txn_totals)
    sql_update += (
        'UPDATE awards a '
        'SET awarding_agency_id = l.awarding_agency_id, '
        'certified_date = l.certified_date, '
        'date_signed = e.action_date, '
        'description = e.description, '
        'funding_agency_id = l.funding_agency_id, '
        'last_modified_date = l.last_modified_date, '
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
        # If another expression is added and includes %s, you must add the tuple for that string interpolation to this
        # list (even if it uses the same one!)
        cursor.execute(sql_update, [award_tuple, award_tuple, award_tuple])
        rows = cursor.rowcount

    return rows


def update_contract_awards(award_tuple=None):
    """Update contract-specific award data based on the info in child transactions."""

    # sum the base_and_all_options_value from contract_data for an award
    sql_txn_totals = (
        'txn_totals AS ('
        'SELECT tx.award_id, SUM(CAST(base_and_all_options_value as double precision)) AS total_base_and_options_value '
        'FROM transaction_fpds INNER JOIN transaction_normalized as tx on '
        'transaction_fpds.transaction_id = tx.id ')
    if award_tuple:
        sql_txn_totals += 'WHERE tx.award_id IN %s '
    sql_txn_totals += 'GROUP BY tx.award_id) '

    # construct a sql query that uses the latest txn contract common table expression above and joins it to the
    # corresponding award. that joined data is used to update awards fields as appropriate (currently, there's only one
    # trasnaction_contract field that trickles up and updates an award record: base_and_all_options_value)
    sql_update = 'WITH {}'.format(sql_txn_totals)
    sql_update += (
        'UPDATE awards a '
        'SET base_and_all_options_value = t.total_base_and_options_value '
        'FROM txn_totals t '
        'WHERE t.award_id = a.id'
    )

    with connection.cursor() as cursor:
        # If another expression is added and includes %s, you must add the tuple for that string interpolation to this
        # list (even if it uses the same one!)
        cursor.execute(sql_update, [award_tuple])
        rows = cursor.rowcount

    return rows


def update_award_subawards(award_tuple=None):
    """
    Updates awards' subaward counts and totals
    """
    # Alternative Django implementation for possible speedup/simplicity
    # Sum and count subaward_amounts
    # for a_id in award_tuple:
    #     a = Award.objects.filter(id=a_id).first()
    #     rows = 0
    #     sas = Subaward.objects.filter(award=a)
    #     count = sas.count()
    #     if a.subaward_count != count:
    #         a.subaward_count = count
    #         rows += 1
    #     a.total_subaward_amount = 0
    #     for sa in sas:
    #         a.total_subaward_amount += sa.amount
    #     a.save()
    # return rows

    sql_sub_totals = (
        'subaward_totals AS ('
        'SELECT award_id, SUM(amount) AS total_subaward_amount, COUNT(*) AS subaward_count '
        'FROM awards_subaward ')
    if award_tuple:
        sql_sub_totals += 'WHERE award_id IN %s '
    sql_sub_totals += 'GROUP BY award_id) '

    # Construct the SQL update
    sql_update = 'WITH {}'.format(sql_sub_totals)
    sql_update += (
        'UPDATE awards '
        'SET total_subaward_amount = subaward_totals.total_subaward_amount, '
        'subaward_count = subaward_totals.subaward_count '
        'FROM subaward_totals '
        'WHERE subaward_totals.award_id = id'
    )

    with connection.cursor() as cursor:
        # If another expression is added and includes %s, you must add the tuple for that string interpolation to this
        # list (even if it uses the same one!)
        cursor.execute(sql_update, [award_tuple])
        rows = cursor.rowcount

    return rows


def update_award_categories(award_tuple=None):
    """
    This sets the category variable for an award.
    """
    awards = Award.objects.all()
    if award_tuple:
        awards = awards.filter(id__in=list(award_tuple))
    awards.update(
        category=Case(
            When(type__in=['A', 'B', 'C', 'D'], then=Value('contract')),
            When(type__in=['02', '03', '04', '05'], then=Value('grant')),
            When(type__in=['06', '10'], then=Value('direct payment')),
            When(type__in=['07', '08'], then=Value('loans')),
            When(type__in=['09'], then=Value('insurance')),
            When(type__in=['11'], then=Value('other')),
            default=None,
            output_field=TextField()
        )
    )


def get_award_financial_transaction(row):
    """
    For specified award financial (aka "File C") data, try to find a matching transaction (aka "File D"). We sometimes
    need to do this  because File C doesn't always have the level of award/transaction specificity that we want, so we
    try to find a matching File D record to grab the additional information.

    For example, when trying to match award financial information to an award record, we need the awarding subtier
    agency, which isn't supplied on File C. Thus, we'll use this function to find a File D record and use the subtier
    agency information supplied there.

    If we find more than one match, return the record with this most recent action date.

    Args:
        row: an object containing these attributes:

        row.toptier_agency_cgac: top tier agency code (aka CGAC code) from File C
        row.piid: piid from File C (contract awards only)
        row.parent_award_id: parent award id from File C (contract awards only)
        row.fain: fain from File C (assistance awards only)
        row.uri: uri from File C (assistance awards only)

    Returns:
        A TransactionNormalized model instance
    """
    # @todo: refactor this into methods on the TransactionFABS
    # and TransactionFPDS models

    if row.fain is not None and row.uri is not None:
        # this is an assistance award id'd by fain
        txn = TransactionNormalized.objects.filter(
            awarding_agency__toptier_agency__cgac_code=row.agency_identifier,
            assistance_data__fain=row.fain,
            assistance_data__uri=row.uri) \
            .order_by('-action_date').values("awarding_agency").first()

    elif row.fain is not None:
        # this is an assistance award id'd by fain
        txn = TransactionNormalized.objects.filter(
            awarding_agency__toptier_agency__cgac_code=row.agency_identifier,
            assistance_data__fain=row.fain) \
            .order_by('-action_date').values("awarding_agency").first()

    elif row.uri is not None:
        # this is an assistance award id'd by uri
        txn = TransactionNormalized.objects.filter(
            awarding_agency__toptier_agency__cgac_code=row.agency_identifier,
            assistance_data__uri=row.uri) \
            .order_by('-action_date').values("awarding_agency").first()

    else:
        # this is a contract award
        txn = TransactionNormalized.objects.filter(
            awarding_agency__toptier_agency__cgac_code=row.agency_identifier,
            contract_data__piid=row.piid,
            contract_data__parent_award_id=row.parent_award_id) \
            .order_by('-action_date').values("awarding_agency").first()

    return str(txn["awarding_agency"]) if txn else None


def get_awarding_agency(row):
    if row.txn:
        # We found a matching transaction, so grab its awarding agency info and pass it get_or_create_summary_award
        return Agency.objects.get(id=int(row.txn))
    else:
        # No matching transaction found, so find/create Award by using toptier agency only, since CGAC code is the only
        # piece of awarding agency info that we have.
        return Agency.get_by_toptier(row.agency_identifier)

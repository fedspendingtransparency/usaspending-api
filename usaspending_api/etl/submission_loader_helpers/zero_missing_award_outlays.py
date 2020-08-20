import logging

from django.db import connection
from django.conf import settings
from elasticsearch_dsl import UpdateByQuery, Q as ES_Q

from usaspending_api.common.elasticsearch.client import instantiate_elasticsearch_client

logger = logging.getLogger("script")

MISSING_COVID_AWARD_SQL = """
-- Find ALL/ANY award_id of awards linked to File C records since COVID began (2020-04-01)
-- Of those, keep the ones that are NOT linked to any File C record in the latest closed periods
-- ... so that we can explicitly update their Outlay to $0 in Elasticsearch
WITH closed_covid_periods AS (
    SELECT submission_fiscal_year, submission_fiscal_month, is_quarter
    FROM dabs_submission_window_schedule
    WHERE period_start_date >= '2020-04-01'
    AND submission_reveal_date < now()
),
quarterlies_from_monthly_submitters AS (
    SELECT q.submission_id
    FROM submission_attributes q
    WHERE q.reporting_period_start >= '2020-04-01'
    AND q.quarter_format_flag = TRUE
    AND q.toptier_code IN (
        -- Any Agency with a quarterly COVID submission that (later) submitted a MONTHLY submissions for closed COVID periods
        SELECT toptier_code
        FROM submission_attributes
        INNER JOIN closed_covid_periods
            ON closed_covid_periods.submission_fiscal_year = reporting_fiscal_year
            AND closed_covid_periods.submission_fiscal_month = reporting_fiscal_period
            AND closed_covid_periods.is_quarter = quarter_format_flag
        WHERE quarter_format_flag = FALSE
    )
),
latest_closed_covid_periods AS (
    -- Latest closed monthly period and quarterly period
    SELECT DISTINCT ON (is_quarter) submission_fiscal_year, submission_fiscal_month, is_quarter
    FROM closed_covid_periods
    ORDER BY is_quarter, submission_fiscal_year DESC, submission_fiscal_month DESC
),
covid_submissions AS (
    SELECT submission_id, reporting_fiscal_year, reporting_fiscal_period, quarter_format_flag, toptier_code, reporting_agency_name
    FROM submission_attributes
    INNER JOIN closed_covid_periods
        ON closed_covid_periods.submission_fiscal_year = reporting_fiscal_year
        AND closed_covid_periods.submission_fiscal_month = reporting_fiscal_period
        AND closed_covid_periods.is_quarter = quarter_format_flag
    WHERE reporting_period_start >= '2020-04-01'
    AND submission_id NOT IN (SELECT submission_id from quarterlies_from_monthly_submitters)
),
latest_covid_submissions AS (
    SELECT submission_id, reporting_fiscal_year, reporting_fiscal_period, quarter_format_flag, toptier_code, reporting_agency_name
    FROM covid_submissions
    INNER JOIN latest_closed_covid_periods
        ON latest_closed_covid_periods.submission_fiscal_year = reporting_fiscal_year
        AND latest_closed_covid_periods.submission_fiscal_month = reporting_fiscal_period
        AND latest_closed_covid_periods.is_quarter = quarter_format_flag
),
covid_awards_from_covid_submissions AS (
    SELECT
        DISTINCT ON (faba.award_id) faba.award_id,
        faba.submission_id,
        covid_submissions.reporting_fiscal_year,
        covid_submissions.reporting_fiscal_period,
        covid_submissions.quarter_format_flag,
        covid_submissions.toptier_code,
        covid_submissions.reporting_agency_name
    FROM financial_accounts_by_awards faba
    INNER JOIN disaster_emergency_fund_code defc
        ON defc.code = faba.disaster_emergency_fund_code
        AND defc.group_name = 'covid_19'
    INNER JOIN covid_submissions ON faba.submission_id = covid_submissions.submission_id
    WHERE faba.award_id IS NOT NULL
    ORDER BY faba.award_id, faba.submission_id DESC
),
covid_awards_from_latest_covid_submisions AS (
    SELECT
        DISTINCT ON (faba.award_id) faba.award_id,
        faba.submission_id,
        latest_covid_submissions.reporting_fiscal_year,
        latest_covid_submissions.reporting_fiscal_period,
        latest_covid_submissions.quarter_format_flag,
        latest_covid_submissions.toptier_code,
        latest_covid_submissions.reporting_agency_name
    FROM financial_accounts_by_awards faba
    INNER JOIN disaster_emergency_fund_code defc
        ON defc.code = faba.disaster_emergency_fund_code
        AND defc.group_name = 'covid_19'
    INNER JOIN latest_covid_submissions ON faba.submission_id = latest_covid_submissions.submission_id
    WHERE faba.award_id IS NOT NULL
    ORDER BY faba.award_id, faba.submission_id DESC
)
-- Awards linked in prior COVID submissions,
-- that are not linked in any Agencies' submissions for the latest closed periods (up to one monthly and one quarterly period)
SELECT
    awards.generated_unique_award_id,
    dropped_awards.award_id,
    dropped_awards.toptier_code,
    dropped_awards.reporting_agency_name,
    dropped_awards.submission_id as last_reported_submission_id,
    dropped_awards.reporting_fiscal_year,
    dropped_awards.reporting_fiscal_period,
    dropped_awards.quarter_format_flag
FROM covid_awards_from_covid_submissions dropped_awards
INNER JOIN awards on awards.id = dropped_awards.award_id
LEFT JOIN covid_awards_from_latest_covid_submisions
    ON dropped_awards.award_id = covid_awards_from_latest_covid_submisions.award_id
WHERE covid_awards_from_latest_covid_submisions.award_id IS NULL
ORDER BY dropped_awards.toptier_code, dropped_awards.submission_id DESC
;
"""

FETCH_COUNT = 50000


def set_missing_award_outlays_to_zero():
    """
    This method checks for awards with Covid data that are not present in the latest
    File C submissions. The outlay for these awards is then set to zero in elasticsearch.
    """

    # Initialize client to connect to Elasticsearch
    es_client = instantiate_elasticsearch_client()

    # Open connection to database
    with connection.cursor() as cursor:

        # Queries for Covid Awards not present in latest File C Submission
        cursor.execute(MISSING_COVID_AWARD_SQL)

        logger.info("Found {} Covid awards without entry in latest File C Submission".format(cursor.rowcount))

        rows = cursor.fetchmany(FETCH_COUNT)
        while len(rows) > 0:
            award_ids = [row[1] for row in rows]

            # Sets the outlays of these awards to zero in Elasticsearch
            set_elasticsearch_covid_outlays_to_zero(es_client, award_ids)
            rows = cursor.fetchmany(FETCH_COUNT)


def set_elasticsearch_covid_outlays_to_zero(es_client, award_ids: list):
    """
    Sets 'total_covid_outlay' to zero in Elasticsearch (when not zero) for a provided
    list of award_ids.
    :param es_client: Client used to connect to Elasticsearch
    :param award_ids: List of award_ids to set outlays to zero in Elasticsearch
    """

    # Creates an Elasticsearch Query criteria for the UpdateByQuery call
    query = (
        ES_Q("range", **{"total_covid_outlay": {"gt": 0}}) | ES_Q("range", **{"total_covid_outlay": {"lt": 0}})
    ) & ES_Q("terms", **{"award_id": award_ids})

    # Sets total_covid_outlay to zero based on the above Query criteria
    ubq = (
        UpdateByQuery(using=es_client, index=settings.ES_AWARDS_WRITE_ALIAS)
        .script(source="ctx._source['total_covid_outlay'] = 0", lang="painless")
        .query(query)
    )
    response = ubq.execute()
    logger.info("Updated {} Awards in Elasticsearch, setting 'total_covid_outlay' to zero".format(response["updated"]))

import logging

from django.db import connection
from django.conf import settings
from django.core.management.base import BaseCommand
from elasticsearch_dsl import UpdateByQuery, Q as ES_Q

from usaspending_api.common.elasticsearch.client import instantiate_elasticsearch_client

logger = logging.getLogger("script")

"""
WARNING: This SQL will only correctly identify awards during the first fiscal
year of Covid submissions. A flaw has been found that will cause some awards in
FY 21 to be incorrectly identified. This should be replaced before the close of
the Fiscal Year
"""

MISSING_COVID_AWARD_SQL = """
SELECT award_id
FROM (
    SELECT
        DISTINCT ON
        (faba.award_id) faba.award_id,
        sa.submission_id,
        sa.is_final_balances_for_fy,
        sa.reporting_fiscal_year,
        sa.reporting_fiscal_period,
        sa.quarter_format_flag
    FROM
        financial_accounts_by_awards faba
    INNER JOIN disaster_emergency_fund_code defc ON
        defc.code = faba.disaster_emergency_fund_code
        AND defc.group_name = 'covid_19'
    INNER JOIN submission_attributes sa ON
        sa.reporting_period_start >= '2020-04-01'
        AND faba.submission_id = sa.submission_id
    INNER JOIN dabs_submission_window_schedule dabs ON
        dabs.id = sa.submission_window_id
        AND dabs.submission_reveal_date <= now()
    WHERE
        faba.award_id IS NOT NULL
    ORDER BY
        faba.award_id, submission_reveal_date DESC, is_quarter
) AS covid_awards
WHERE covid_awards.is_final_balances_for_fy = FALSE
;
"""


class Command(BaseCommand):

    FETCH_COUNT = 50000

    help = (
        "This command checks for awards with Covid data that are not present in the latest "
        "File C submissions or are present without a Covid DEFC. The Covid outlay for these "
        "awards is then set to zero in Elasticsearch."
    )

    def handle(self, *args, **options):

        # Initialize client to connect to Elasticsearch
        es_client = instantiate_elasticsearch_client()

        # Open connection to database
        with connection.cursor() as cursor:

            # Queries for Covid Awards not present in latest File C Submission
            cursor.execute(MISSING_COVID_AWARD_SQL)

            logger.info("Found {} Covid awards without entry in latest File C Submission".format(cursor.rowcount))

            rows = cursor.fetchmany(self.FETCH_COUNT)
            while len(rows) > 0:
                award_ids = [row[0] for row in rows]

                # Sets the outlays of these awards to zero in Elasticsearch
                self.set_elasticsearch_covid_outlays_to_zero(es_client, award_ids)
                rows = cursor.fetchmany(self.FETCH_COUNT)

    def set_elasticsearch_covid_outlays_to_zero(self, es_client, award_ids: list):
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
        logger.info(
            "Updated {} Awards in Elasticsearch, setting 'total_covid_outlay' to zero".format(response["updated"])
        )

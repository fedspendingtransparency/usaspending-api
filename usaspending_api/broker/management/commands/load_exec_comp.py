from datetime import datetime
from django.core.management.base import BaseCommand
from django.db import connection
import logging

logger = logging.getLogger('console')


exec_comp_sql = """
DROP TABLE IF EXISTS references_legalentityofficers_new;

CREATE TABLE references_legalentityofficers_new AS (
    SELECT
        legal_entity.legal_entity_id AS legal_entity_id,
        broker_exec_comp.officer_1_name AS officer_1_name,
        broker_exec_comp.officer_2_name AS officer_2_name,
        broker_exec_comp.officer_3_name AS officer_3_name,
        broker_exec_comp.officer_4_name AS officer_4_name,
        broker_exec_comp.officer_5_name AS officer_5_name,
        broker_exec_comp.officer_1_amount AS officer_1_amount,
        broker_exec_comp.officer_2_amount AS officer_2_amount,
        broker_exec_comp.officer_3_amount AS officer_3_amount,
        broker_exec_comp.officer_4_amount AS officer_4_amount,
        broker_exec_comp.officer_5_amount AS officer_5_amount,
        NOW()::DATE AS update_date
    FROM
        dblink ('broker_server', '(
            SELECT
                DISTINCT ON (e.awardee_or_recipient_uniqu)
                e.awardee_or_recipient_uniqu AS duns,
                high_comp_officer1_full_na AS officer_1_name,
                high_comp_officer2_full_na AS officer_2_name,
                high_comp_officer3_full_na AS officer_3_name,
                high_comp_officer4_full_na AS officer_4_name,
                high_comp_officer5_full_na AS officer_5_name,
                NULLIF(high_comp_officer1_amount, '''')::NUMERIC(20, 2) AS officer_1_amount,
                NULLIF(high_comp_officer2_amount, '''')::NUMERIC(20, 2) AS officer_2_amount,
                NULLIF(high_comp_officer3_amount, '''')::NUMERIC(20, 2) AS officer_3_amount,
                NULLIF(high_comp_officer4_amount, '''')::NUMERIC(20, 2) AS officer_4_amount,
                NULLIF(high_comp_officer5_amount, '''')::NUMERIC(20, 2) AS officer_5_amount
            FROM executive_compensation e
            INNER JOIN (
                SELECT awardee_or_recipient_uniqu, max(created_at) as MaxDate
                FROM executive_compensation ex
                GROUP BY awardee_or_recipient_uniqu
            ) ex ON e.awardee_or_recipient_uniqu = ex.awardee_or_recipient_uniqu
                AND e.created_at = ex.MaxDate
            WHERE
            TRIM(high_comp_officer1_full_na) != '''' OR
            TRIM(high_comp_officer2_full_na) != '''' OR
            TRIM(high_comp_officer3_full_na) != '''' OR
            TRIM(high_comp_officer4_full_na) != '''' OR
            TRIM(high_comp_officer5_full_na) != '''' OR
            TRIM(high_comp_officer1_amount) != '''' OR
            TRIM(high_comp_officer2_amount) != '''' OR
            TRIM(high_comp_officer3_amount) != '''' OR
            TRIM(high_comp_officer4_amount) != '''' OR
            TRIM(high_comp_officer5_amount) != '''')') AS broker_exec_comp
            (
            	duns text,
                officer_1_name text,
                officer_2_name text,
                officer_3_name text,
                officer_4_name text,
                officer_5_name text,
                officer_1_amount numeric(20, 2),
                officer_2_amount numeric(20, 2),
                officer_3_amount numeric(20, 2),
                officer_4_amount numeric(20, 2),
                officer_5_amount numeric(20, 2)
            )
            INNER JOIN
            legal_entity ON legal_entity.recipient_unique_id = broker_exec_comp.duns
);

ALTER TABLE references_legalentityofficers RENAME TO references_legalentityofficers_old;

ALTER TABLE references_legalentityofficers_new RENAME TO references_legalentityofficers;

TRUNCATE references_legalentityofficers_old;

DROP TABLE references_legalentityofficers_old;

ALTER TABLE references_legalentityofficers ADD PRIMARY KEY(legal_entity_id);
"""


class Command(BaseCommand):

    @staticmethod
    def run_sql_file(file_path):
        with connection.cursor() as cursor:
            with open(file_path) as infile:
                for raw_sql in infile.read().split('\n\n\n'):
                    if raw_sql.strip():
                        cursor.execute(raw_sql)

    def handle(self, *args, **options):
        logger.info('Running exec comp SQL to recreate (aka reload) ALL exec comp data')
        total_start = datetime.now()

        with connection.cursor() as cursor:
            cursor.execute(exec_comp_sql)

        logger.info('Finished exec comp SQL in %s seconds' % str(datetime.now()-total_start))

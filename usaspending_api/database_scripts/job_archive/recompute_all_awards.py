"""
Jira Ticket Number(s): DEV-3318, DEV-3442, DEV-3443

    Adding new earliest_transaction to awards
    Improved logic for determining earliest and latest transactions for awards which requires a reload

Expected CLI:

    $ python usaspending_api/database_scripts/job_archive/recompute_all_awards.py

Purpose:

    Updates award records which need to be updated using the new award_helper logic

"""
import asyncpg
import asyncio
import re
import math
import psycopg2
import time
# import argparse

from os import environ


# DEFINE THIS ENVIRONMENT VARIABLE BEFORE RUNNING!
CONNECTION_STRING = environ["DATABASE_URL"]
CHUNK_SIZE = 10000
DEBUG = False
earliest_transaction_cte = (
    "txn_earliest AS ( "
    "SELECT DISTINCT ON (award_id) "
    "award_id, "
    "id, "
    "action_date, "
    "description, "
    "period_of_performance_start_date "
    "FROM transaction_normalized "
    "WHERE award_id IN ({award_ids}) "
    "ORDER BY award_id, action_date ASC, modification_number ASC "
    ")"
)
latest_transaction_cte = (
    "txn_latest AS ( "
    "SELECT DISTINCT ON (award_id) "
    "award_id, "
    "id, "
    "awarding_agency_id, "
    "action_date, "
    "funding_agency_id, "
    "last_modified_date, "
    "period_of_performance_current_end_date, "
    "place_of_performance_id, "
    "recipient_id, "
    "type, "
    "type_description, "
    "CASE WHEN type IN ('A', 'B', 'C', 'D') THEN 'contract' "
    "  WHEN type IN ('02', '03', '04', '05') THEN 'grant' "
    "  WHEN type in ('06', '10') THEN 'direct payment' "
    "  WHEN type in ('07', '08') THEN 'loans' "
    "  WHEN type = '09' THEN 'insurance' "
    "  WHEN type = '11' THEN 'other' "
    "  WHEN type LIKE 'IDV%%' THEN 'idv' "
    "  ELSE NULL END AS category "
    "FROM transaction_normalized "
    "WHERE award_id IN ({award_ids}) "
    "ORDER BY award_id, action_date DESC, modification_number DESC "
    ")"
)
aggregate_transaction_cte = (
    "txn_totals AS ( "
    "SELECT award_id, SUM(federal_action_obligation) AS total_obligation, "
    "SUM(original_loan_subsidy_cost) AS total_subsidy_cost, "
    "SUM(funding_amount) AS total_funding_amount, "
    "SUM(face_value_loan_guarantee) AS total_loan_value, "
    "SUM(non_federal_funding_amount) AS non_federal_funding_amount "
    "FROM transaction_normalized "
    "WHERE award_id IN ({award_ids}) "
    " GROUP BY award_id "
    ")"
)
executive_comp_cte = (
    "executive_comp AS ( "
    "SELECT DISTINCT ON (tn.award_id) "
    "tn.award_id, "
    "tf.officer_1_amount, "
    "tf.officer_1_name, "
    "tf.officer_2_amount, "
    "tf.officer_2_name, "
    "tf.officer_3_amount, "
    "tf.officer_3_name, "
    "tf.officer_4_amount, "
    "tf.officer_4_name, "
    "tf.officer_5_amount, "
    "tf.officer_5_name "
    ""
    "FROM transaction_normalized tn "
    "INNER JOIN {transaction_table} AS tf ON tn.id = tf.transaction_id "
    "WHERE tf.officer_1_name IS NOT NULL AND award_id IN ({award_ids}) "
    "ORDER BY tn.award_id, tn.action_date DESC "
    ") "
)

UPDATE_AWARDS_SQL = (
    "WITH {}, {}, {}, {} "
    "UPDATE awards a "
    "SET "
    "earliest_transaction_id = e.id, "
    "date_signed = e.action_date, "
    "description = e.description, "
    "period_of_performance_start_date = e.period_of_performance_start_date, "
    ""
    "latest_transaction_id = l.id, "
    "awarding_agency_id = l.awarding_agency_id, "
    "category = l.category, "
    "certified_date = l.action_date, "
    "funding_agency_id = l.funding_agency_id, "
    "last_modified_date = l.last_modified_date, "
    "period_of_performance_current_end_date = l.period_of_performance_current_end_date, "
    "place_of_performance_id = l.place_of_performance_id, "
    "recipient_id = l.recipient_id, "
    "type = l.type, "
    "type_description = l.type_description, "
    ""
    "non_federal_funding_amount = t.non_federal_funding_amount, "
    "total_funding_amount = t.total_funding_amount, "
    "total_loan_value = t.total_loan_value, "
    "total_obligation = t.total_obligation, "
    "total_subsidy_cost = t.total_subsidy_cost, "
    ""
    "officer_1_amount = ec.officer_1_amount, "
    "officer_1_name = ec.officer_1_name, "
    "officer_2_amount = ec.officer_2_amount, "
    "officer_2_name = ec.officer_2_name, "
    "officer_3_amount = ec.officer_3_amount, "
    "officer_3_name = ec.officer_3_name, "
    "officer_4_amount = ec.officer_4_amount, "
    "officer_4_name = ec.officer_4_name, "
    "officer_5_amount = ec.officer_5_amount, "
    "officer_5_name = ec.officer_5_name "
    ""
    "FROM txn_earliest e "
    "JOIN txn_latest l ON e.award_id = l.award_id "
    "JOIN txn_totals t ON e.award_id = t.award_id "
    "LEFT JOIN executive_comp AS ec ON e.award_id = ec.award_id "
    "WHERE "
    "  a.id = e.award_id AND ("
    "    a.earliest_transaction_id IS DISTINCT FROM e.id "
    "    OR a.date_signed IS DISTINCT FROM e.action_date "
    "    OR a.description IS DISTINCT FROM e.description "
    "    OR a.period_of_performance_start_date IS DISTINCT FROM e.period_of_performance_start_date "
    "    OR a.latest_transaction_id IS DISTINCT FROM l.id "
    "    OR a.awarding_agency_id IS DISTINCT FROM l.awarding_agency_id "
    "    OR a.category IS DISTINCT FROM l.category "
    "    OR a.certified_date IS DISTINCT FROM l.action_date "
    "    OR a.funding_agency_id IS DISTINCT FROM l.funding_agency_id "
    "    OR a.last_modified_date IS DISTINCT FROM l.last_modified_date "
    "    OR a.period_of_performance_current_end_date IS DISTINCT FROM l.period_of_performance_current_end_date "
    "    OR a.place_of_performance_id IS DISTINCT FROM l.place_of_performance_id "
    "    OR a.recipient_id IS DISTINCT FROM l.recipient_id "
    "    OR a.type IS DISTINCT FROM l.type "
    "    OR a.type_description IS DISTINCT FROM l.type_description "
    "    OR a.non_federal_funding_amount IS DISTINCT FROM t.non_federal_funding_amount "
    "    OR a.total_funding_amount IS DISTINCT FROM t.total_funding_amount "
    "    OR a.total_loan_value IS DISTINCT FROM t.total_loan_value "
    "    OR a.total_obligation IS DISTINCT FROM t.total_obligation "
    "    OR a.total_subsidy_cost IS DISTINCT FROM t.total_subsidy_cost "
    "    OR a.officer_1_amount IS DISTINCT FROM ec.officer_1_amount "
    "    OR a.officer_1_name IS DISTINCT FROM ec.officer_1_name "
    "    OR a.officer_2_amount IS DISTINCT FROM ec.officer_2_amount "
    "    OR a.officer_2_name IS DISTINCT FROM ec.officer_2_name "
    "    OR a.officer_3_amount IS DISTINCT FROM ec.officer_3_amount "
    "    OR a.officer_3_name IS DISTINCT FROM ec.officer_3_name "
    "    OR a.officer_4_amount IS DISTINCT FROM ec.officer_4_amount "
    "    OR a.officer_4_name IS DISTINCT FROM ec.officer_4_name "
    "    OR a.officer_5_amount IS DISTINCT FROM ec.officer_5_amount "
    "    OR a.officer_5_name IS DISTINCT FROM ec.officer_5_name "
    "  )"
)
UPDATE_AWARDS_SQL = str(UPDATE_AWARDS_SQL).format(earliest_transaction_cte, latest_transaction_cte, aggregate_transaction_cte, executive_comp_cte)


GET_MIN_MAX_SQL = "SELECT MIN(id), MAX(id) FROM awards"
GET_FPDS_AWARDS = "SELECT id FROM awards where is_fpds = TRUE AND id BETWEEN {minid} AND {maxid}"
GET_FABS_AWARDS = "SELECT id FROM awards where is_fpds = FALSE AND id BETWEEN {minid} AND {maxid}"


class Timer:
    def __init__(self, msg, pipe_output=print):
        self.print_func = pipe_output
        self.msg = msg

    def __enter__(self):
        self.start = time.perf_counter()
        self.print_func("{} starting...".format(self.msg))
        return self

    def __exit__(self, *args, **kwargs):
        self.end = time.perf_counter()
        self.elapsed = self.end - self.start
        self.print_func("{} completed in {}".format(self.msg, self.pretty_print(self.elapsed)))

    @staticmethod
    def pretty_print(elapsed):
        f, s = math.modf(elapsed)
        m, s = divmod(s, 60)
        h, m = divmod(m, 60)
        return "%d:%02d:%02d.%04d" % (h, m, s, f * 10000)


async def async_run_create(sql):
    conn = await asyncpg.connect(dsn=CONNECTION_STRING)
    try:
        insert_msg = await conn.execute(sql)
        await conn.close()
    except Exception as e:
        print("========= ERROR: {}\n".format(e))
        print("{}\n=========".format(sql))
        raise SystemExit

    if DEBUG:
        print(sql)
        print(insert_msg)
    count = re.findall(r"\d+", insert_msg)  # Example msg: UPDATE 10
    return int(count[0])


def run_update_query(fabs_awards, fpds_awards):
    loop = asyncio.new_event_loop()
    statements = []
    if fabs_awards:
        fabs_sql = UPDATE_AWARDS_SQL.format(award_ids=', '.join(fabs_awards), transaction_table="transaction_fabs")
        statements.append(asyncio.ensure_future(async_run_create(fabs_sql), loop=loop))
    if fpds_awards:
        fpds_sql = UPDATE_AWARDS_SQL.format(award_ids=', '.join(fpds_awards), transaction_table="transaction_fpds")
        statements.append(asyncio.ensure_future(async_run_create(fpds_sql), loop=loop))

    all_statements = asyncio.gather(*statements)
    loop.run_until_complete(all_statements)
    loop.close()
    return sum([stmt.result() for stmt in statements])


def main():
    with psycopg2.connect(dsn=CONNECTION_STRING) as connection:
        connection.autocommit = True
        connection.readonly = True

        with connection.cursor() as cursor:
            print("Finding min/max IDs...")
            cursor.execute(GET_MIN_MAX_SQL)
            results = cursor.fetchall()
            min_id, max_id = results[0]

        print("Min ID: {:,}".format(min_id))
        print("Max ID: {:,}".format(max_id))
        print("Total in range: {:,}".format(max_id - min_id + 1), flush=True)

        batch_min = min_id
        while batch_min <= max_id:
            batch_max = min(batch_min + CHUNK_SIZE - 1, max_id)
            with Timer("[Awards {:,} - {:,}]".format(batch_min, batch_max)):
                with connection.cursor() as cursor:
                    cursor.execute(GET_FABS_AWARDS.format(minid=batch_min, maxid=batch_max))
                    fabs = [str(row[0]) for row in cursor.fetchall()]
                    cursor.execute(GET_FPDS_AWARDS.format(minid=batch_min, maxid=batch_max))
                    fpds = [str(row[0]) for row in cursor.fetchall()]

                if fabs or fpds:
                    row_count = run_update_query(fabs_awards=fabs, fpds_awards=fpds)
                    print("UPDATED {:,} records".format(row_count), flush=True)
                else:
                    print("#### No awards to update in range ###", flush=True)
            batch_min = batch_max + 1


def setup():
    with psycopg2.connect(dsn=CONNECTION_STRING) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            print("### Disabling autovacuum ###")
            cursor.execute("ALTER TABLE awards SET (autovacuum_enabled = false, toast.autovacuum_enabled = false)")
            print(cursor.statusmessage)


def teardown(successful_run=False):
    with psycopg2.connect(dsn=CONNECTION_STRING) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            with Timer("teardown"):
                if successful_run:
                    print("### running full vacuum ###")
                    cursor.execute("VACUUM (FULL, ANALYZE, VERBOSE) awards")
                    print(cursor.statusmessage)
                print("### Resetting autovacuum ###")
                cursor.execute("ALTER TABLE awards SET (autovacuum_enabled = true, toast.autovacuum_enabled = true)")
                print(cursor.statusmessage)


if __name__ == "__main__":
    successful_run = False
    with Timer("recompute_all_awards"):
        setup()
        try:
            main()
            successful_run = True
        except KeyboardInterrupt:
            pass
        except Exception:
            print("ERROR ENCOUNTERED!!!!!")
            raise SystemExit(1)
        finally:
            print("Cleaning up table and restoring autovacuum")
            teardown(successful_run)

    print("Finished.")

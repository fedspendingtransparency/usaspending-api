import subprocess
from datetime import datetime
import csv
import os
from time import perf_counter, sleep
import pandas as pd

from elasticsearch import helpers
from django.db import connection

AWARD_DESC_CATEGORIES = {
    'loans': 'loans',
    'grant': 'grants',
    'insurance': 'other',
    'other': 'other',
    'contract': 'contracts',
    'direct payment': 'directpayments'
}


class DataJob:
    def __init__(self, *args):
        self.name = args[0]
        self.index = args[1]
        self.fy = args[2]
        self.category = args[3]
        self.csv = args[4]
        self.count = None


def configure_sql_strings(config, filename, deleted_ids):
    '''
    Populates the formatted strings defined globally in this file to create the desired SQL
    '''
    update_date_str = UPDATE_DATE_SQL.format(config['starting_date'].strftime('%Y-%m-%d'))
    award_type_str = ''
    if config['award_category']:
        if config['award_category'] == 'contract':
            award_type_str = CONTRACTS_IDV_SQL.format(config['award_category'])
        else:
            award_type_str = CATEGORY_SQL.format(config['award_category'])

    copy_sql = COPY_SQL.format(
        fy=config['fiscal_year'],
        update_date=update_date_str,
        award_category=award_type_str,
        filename=filename)

    count_sql = COUNT_SQL.format(
        fy=config['fiscal_year'],
        update_date=update_date_str,
        award_category=award_type_str,
    )

    if deleted_ids and config['provide_deleted']:
        id_list = ','.join(["('{}')".format(x) for x in deleted_ids.keys()])
        id_sql = CHECK_IDS_SQL.format(id_list=id_list)
    else:
        id_sql = None

    return copy_sql, id_sql, count_sql


def execute_sql_statement(cmd, results=False, verbose=False):
    ''' Simple function to execute SQL using the Django DB connection'''
    rows = None
    if verbose:
        print(cmd)
    with connection.cursor() as cursor:
        cursor.execute(cmd)
        if results:
            rows = db_rows_to_dict(cursor)
    return rows


def db_rows_to_dict(cursor):
    ''' Return a dictionary of all row results from a database connection cursor '''
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]


def download_db_records(fetch_jobs, done_jobs, config):
    while not fetch_jobs.empty():
        start = perf_counter()
        job = fetch_jobs.get_nowait()
        printf({'msg': 'Preparing to Download a new CSV', 'job': job.name, 'f': 'Download'})

        sql_config = {
            'starting_date': config['starting_date'],
            'fiscal_year': job.fy,
            'award_category': job.category,
            'provide_deleted': False
        }
        copy_sql, _, count_sql = configure_sql_strings(sql_config, job.csv, [])

        if os.path.isfile(job.csv):
            os.remove(job.csv)

        job.count = download_csv(count_sql, copy_sql, job.csv, job.name, config['verbose'])
        done_jobs.put(job)
        printf({
            'msg': 'Data fetch job took {} seconds'.format(perf_counter() - start),
            'job': job.name,
            'f': 'Download'
        })
        sleep(1)

    done_jobs.put(DataJob(None, None, None, None, None))
    printf({'msg': 'All downloads from Postgres completed', 'f': 'Download'})
    return


def download_csv(count_sql, copy_sql, filename, job_id, verbose):
    count = execute_sql_statement(count_sql, True, verbose)[0]['count']
    printf({'msg': 'Writing {} transactions to this file: {}'.format(count, filename), 'job': job_id, 'f': 'Download'})
    # It is preferable to not use shell=True, but this command works. Limited user-input so risk is low
    subprocess.Popen('psql "${{DATABASE_URL}}" -c {}'.format(copy_sql), shell=True).wait()

    download_count = csv_row_count(filename)
    if count != download_count:
        msg = 'Mismatch between CSV and DB rows! Expected: {} | Actual {} in: {}'
        printf({
            'msg': msg.format(count, download_count, filename),
            'job': job_id,
            'f': 'Download'})

    return count


def csv_chunk_gen(filename, header, chunksize, job_id):
    printf({'msg': 'Opening {} batch size: {}'.format(filename, chunksize), 'job': job_id, 'f': 'ES Ingest'})
    # Panda's data type guessing causes issues for Elasticsearch. Set all cols to str
    dtype = {x: str for x in header}

    for file_df in pd.read_csv(filename, dtype=dtype, header=0, chunksize=chunksize):
        file_df = file_df.where(cond=(pd.notnull(file_df)), other=None)
        yield file_df.to_dict(orient='records')


def streaming_post_to_es(client, chunk, index_name, job_id):
    success, failed = 0, 0
    try:
        for ok, item in helpers.streaming_bulk(client, chunk, index=index_name, doc_type='transaction_mapping'):
            success = [success, success + 1][ok]
            failed = [failed + 1, failed][ok]

    except Exception as e:
        print('MASSIVE FAIL!!!\n\n{}\n\n{}'.format(e, '*' * 80))
        raise SystemExit

    printf({'msg': 'Success: {} | Fails: {}'.format(success, failed), 'job': job_id, 'f': 'ES Ingest'})
    return success, failed


def printf(items):
    t = datetime.utcnow().strftime('%H:%M:%S.%f')
    msg = items['msg']
    if 'error' in items:
        template = '[{time}] [ERROR] {msg}'
        print_msg = template.format(time=t, msg=msg)
    else:
        template = '[{time}] {complex:<20} | {msg}'
        func = '[' + items.get('f', 'main') + ']'
        job = items.get('job', None)
        j = ''
        if job:
            j = ' (#{})'.format(job)
    print_msg = template.format(time=t, complex=func + j, msg=msg)

    print(print_msg)


def csv_row_count(filename, has_header=True):
    with open(filename, 'r') as f:
        row_count = sum(1 for row in csv.reader(f))
    if has_header:
        row_count -= 1
    return row_count


# ==============================================================================
# SQL Template Strings for Postgres Statements
# ==============================================================================
VIEW_COLUMNS = [
    'transaction_id', 'modification_number', 'award_id', 'piid', 'fain', 'uri',
    'award_description', 'product_or_service_code', 'product_or_service_description',
    'naics_code', 'naics_description', 'type_description', 'award_category',
    'recipient_unique_id', 'parent_recipient_unique_id', 'recipient_name',
    'action_date', 'period_of_performance_start_date', 'period_of_performance_current_end_date',
    'transaction_fiscal_year', 'award_fiscal_year', 'award_amount',
    'transaction_amount', 'face_value_loan_guarantee', 'original_loan_subsidy_cost',
    'awarding_agency_id', 'funding_agency_id', 'awarding_toptier_agency_name',
    'funding_toptier_agency_name', 'awarding_subtier_agency_name',
    'funding_subtier_agency_name', 'awarding_toptier_agency_abbreviation',
    'funding_toptier_agency_abbreviation', 'awarding_subtier_agency_abbreviation',
    'funding_subtier_agency_abbreviation', 'cfda_title', 'cfda_popular_name',
    'type_of_contract_pricing', 'type_set_aside', 'extent_competed', 'pulled_from',
    'type', 'pop_country_code', 'pop_country_name', 'pop_state_code', 'pop_county_code',
    'pop_county_name', 'pop_zip5', 'pop_congressional_code',
    'recipient_location_country_code', 'recipient_location_country_name',
    'recipient_location_state_code', 'recipient_location_county_code',
    'recipient_location_zip5', 'detached_award_proc_unique', 'afa_generated_unique',
    'generated_unique_transaction_id', 'update_date',
]

UPDATE_DATE_SQL = ' AND update_date >= \'{}\''

CATEGORY_SQL = ' AND award_category = \'{}\''

CONTRACTS_IDV_SQL = ' AND (award_category = \'{}\' OR award_category IS NULL AND pulled_from = \'IDV\')'

COUNT_SQL = '''SELECT COUNT(*) AS count
FROM transaction_delta_view
WHERE transaction_fiscal_year={fy}{update_date}{award_category};'''

COPY_SQL = '''"COPY (
    SELECT *
    FROM transaction_delta_view
    WHERE transaction_fiscal_year={fy}{update_date}{award_category}
) TO STDOUT DELIMITER ',' CSV HEADER" > '{filename}'
'''

CHECK_IDS_SQL = '''
WITH temp_transaction_ids AS (
  SELECT * FROM (VALUES {id_list}) AS unique_id_list (generated_unique_transaction_id)
)
SELECT transaction_id, generated_unique_transaction_id, update_date FROM transaction_delta_view
WHERE EXISTS (
  SELECT *
  FROM temp_transaction_ids
  WHERE transaction_delta_view.generated_unique_transaction_id = temp_transaction_ids.generated_unique_transaction_id
);
'''

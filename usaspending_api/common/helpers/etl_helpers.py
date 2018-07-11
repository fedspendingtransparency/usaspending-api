from datetime import datetime
import logging

from django.db import connection

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.sql_helpers import read_sql_file


logger = logging.getLogger('console')
ETL_SQL_FILE_PATH = 'usaspending_api/etl/management/sql/'


def get_unlinked_count(file_name):

    file_path = ETL_SQL_FILE_PATH + file_name
    sql_commands = read_sql_file(file_path=file_path)

    if len(sql_commands) != 1:
        raise InvalidParameterException('Invalid number of commands in specified file. File should contain 1 SQL '
                                        'command to get the count of unlinked records.')

    with connection.cursor() as cursor:
        cursor.execute(sql_commands[0])
        result = cursor.fetchall()[0][0]

    return int(result)


def update_assistance_c_to_d_linkages():
    logger.info('Starting File C to D linkage updates for Assistance records')
    file_names = ['update_file_c_linkages_fain.sql', 'update_file_c_linkages_uri.sql',
                  'update_file_c_linkages_fain_and_uri.sql']
    file_paths = [ETL_SQL_FILE_PATH + file_name for file_name in file_names]

    starting_unlinked_count = get_unlinked_count(file_name='check_assistance_file_c_linkages.sql')
    logger.info('Current count of unlinked assistance records: %s' % str(starting_unlinked_count))

    total_start = datetime.now()
    for file_name in file_paths:
        start = datetime.now()
        logger.info('Running %s' % file_name)
        sql_commands = read_sql_file(file_path=file_name)
        for command in sql_commands:
            with connection.cursor() as cursor:
                cursor.execute(command)
        logger.info('Finished %s in %s seconds' % (file_name, str(datetime.now() - start)))

    ending_unlinked_count = get_unlinked_count(file_name='check_assistance_file_c_linkages.sql')
    logger.info('Count of unlinked assistance records after updates: %s' % str(ending_unlinked_count))

    logger.info('Finished all queries in %s seconds' % str(datetime.now() - total_start))


def update_contract_c_to_d_linkages():
    logger.info('Starting File C to D linkage updates for Contract records')
    file_name = ETL_SQL_FILE_PATH + 'update_file_c_linkages_piid.sql'

    starting_unlinked_count = get_unlinked_count(file_name='check_contract_file_c_linkages.sql')
    logger.info('Current count of unlinked contract records: %s' % str(starting_unlinked_count))

    total_start = datetime.now()
    start = datetime.now()
    logger.info('Running %s' % file_name)
    sql_commands = read_sql_file(file_path=file_name)
    for command in sql_commands:
        with connection.cursor() as cursor:
            cursor.execute(command)
    logger.info('Finished %s in %s seconds' % (file_name, str(datetime.now() - start)))

    ending_unlinked_count = get_unlinked_count(file_name='check_contract_file_c_linkages.sql')
    logger.info('Count of unlinked contract records after updates: %s' % str(ending_unlinked_count))

    logger.info('Finished all queries in %s seconds' % str(datetime.now() - total_start))

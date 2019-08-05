from datetime import datetime
import logging

from django.db import connection

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.sql_helpers import read_sql_file


logger = logging.getLogger("console")
ETL_SQL_FILE_PATH = "usaspending_api/etl/management/sql/c_file_linkage/"


def get_unlinked_count(file_name):

    file_path = ETL_SQL_FILE_PATH + file_name
    sql_commands = read_sql_file(file_path=file_path)

    if len(sql_commands) != 1:
        raise InvalidParameterException(
            "Invalid number of commands in specified file. File should contain 1 SQL "
            "command to get the count of unlinked records."
        )

    with connection.cursor() as cursor:
        cursor.execute(sql_commands[0])
        result = cursor.fetchall()[0][0]

    return int(result)


def update_c_to_d_linkages(type):
    logger.info("Starting File C to D linkage updates for %s records" % type)

    if type.lower() == "contract":
        file_names = ["update_file_c_linkages_piid.sql"]
        unlinked_count_file_name = "check_contract_file_c_linkages.sql"
    elif type.lower() == "assistance":
        file_names = [
            "update_file_c_linkages_fain.sql",
            "update_file_c_linkages_uri.sql",
            "update_file_c_linkages_fain_and_uri.sql",
        ]
        unlinked_count_file_name = "check_assistance_file_c_linkages.sql"
    else:
        raise InvalidParameterException("Invalid type provided to process C to D linkages.")

    file_paths = [ETL_SQL_FILE_PATH + file_name for file_name in file_names]

    starting_unlinked_count = get_unlinked_count(file_name=unlinked_count_file_name)
    logger.info("Current count of unlinked %s records: %s" % (type, str(starting_unlinked_count)))

    total_start = datetime.now()
    for file_name in file_paths:
        start = datetime.now()
        logger.info("Running %s" % file_name)
        sql_commands = read_sql_file(file_path=file_name)
        for command in sql_commands:
            with connection.cursor() as cursor:
                cursor.execute(command)
        logger.info("Finished %s in %s seconds" % (file_name, str(datetime.now() - start)))

    ending_unlinked_count = get_unlinked_count(file_name=unlinked_count_file_name)
    logger.info("Count of unlinked %s records after updates: %s" % (type, str(ending_unlinked_count)))

    logger.info("Finished all queries in %s seconds" % str(datetime.now() - total_start))

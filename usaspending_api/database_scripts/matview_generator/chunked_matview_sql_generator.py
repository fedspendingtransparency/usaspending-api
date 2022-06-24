#!/usr/bin/env python3

import argparse
import glob
import hashlib
import os
import copy
import re

from usaspending_api.database_scripts.matview_generator.shared_sql_generator import (
    COMPONENT_DIR,
    generate_uid,
    HERE,
    ingest_json,
    make_modification_sql,
    make_table_drops,
    make_matview_refresh,
    TEMPLATE,
)
from usaspending_api.etl.broker_etl_helpers import dictfetchall

# Usage: python chunked_matview_sql_generator.py --file <file_name> (from usaspending_api/database_scripts/matview_generator)
#        ^--- Will clobber files in usaspending_api/database_scripts/matviews

"""
POSTGRES INDEX FORMAT
    CREATE [ UNIQUE ] INDEX [ name ] ON table_name [ USING method ]
    ( { column_name | ( expression ) } [ COLLATE collation ]
        [ opclass ] [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [, ...] )
    [ WITH ( storage_parameter = value [, ... ] ) ]
    [ WHERE predicate ]

EXAMPLE SQL DESCRIPTION JSON FILE:

{   "schema_name": "public",
    "final_name": "example_matview",
    "matview_sql": [
    "SELECT",
    "  action_date,",
    "  fiscal_year,",
    "  awards.type,",
    "  awards.category,",
    "FROM",
    "  awards",
    "LEFT OUTER JOIN",
    "  transaction_normalized ON (awards.latest_transaction_id = id)",
    "WHERE",
    "  action_date >= '2000-10-01'",
    "ORDER BY",
    "  action_date DESC"
    ]
}
"""

GLOBAL_ARGS = None
UNIQUE_STRING = None


def make_matview_drops(final_matview_name):
    return [TEMPLATE["drop_matview"].format(final_matview_name)]


def make_matview_create(final_matview_name, sql):
    matview_sql = "\n".join(sql)
    return [TEMPLATE["create_matview"].format(final_matview_name, matview_sql, "")]


def make_temp_table_create(table_name, table_temp_name):
    """Creates sql to create a temporary table based off an existing table definition.
    Includes sql to drop the temporary table in case it already exists.
    """
    return [
        TEMPLATE["drop_table"].format(table_temp_name),
        TEMPLATE["create_table"].format(table_temp_name, table_name),
    ]


def make_read_indexes(table_name):
    if '.' in table_name:
        schema_name, table_name = table_name[:table_name.index('.')], table_name[table_name.index('.')+1:]
    else:
        schema_name = 'public'

    return [TEMPLATE["read_indexes"].format(schema_name, table_name)]


def make_read_constraints(table_name):
    return [TEMPLATE["read_constraints"].format(table_name)]


def make_rename_sql(table_name):
    table_temp_name = table_name + "_temp"
    table_archive_name = table_name + "_old"
    sql_strings = []
    sql_strings.append(TEMPLATE["drop_table"].format(table_archive_name))
    sql_strings.append(TEMPLATE["rename_table"].format("IF EXISTS ", table_name, table_archive_name))
    sql_strings.append("")
    sql_strings.append(TEMPLATE["rename_table"].format("", table_temp_name, table_name))
    sql_strings.append("")
    return sql_strings


def make_copy_constraints(cursor, source_table, dest_table, drop_foreign_keys=False):
    # read the existing indexes
    cursor.execute(make_read_constraints(source_table)[0])
    src_constrs = dictfetchall(cursor)

    # build the destination index sql
    dest_constr_sql = []
    for constr_dict in src_constrs:
        create_constr_name = constr_dict["conname"]
        create_constr_content = constr_dict["pg_get_constraintdef"]
        if "FOREIGN KEY" in create_constr_content and drop_foreign_keys:
            continue
        dest_constr_sql.append(
            f"ALTER TABLE {dest_table} ADD CONSTRAINT {create_constr_name}_temp" f" {create_constr_content}"
        )
    return dest_constr_sql


def make_copy_indexes(cursor, source_table, dest_table):
    # read the existing indexes of source table
    cursor.execute(make_read_indexes(source_table)[0])
    src_indexes = dictfetchall(cursor)

    # reading the existing indexes of destination table (to not duplicate anything)
    cursor.execute(make_read_indexes(dest_table)[0])
    dest_indexes = [ix_dict["indexname"] for ix_dict in dictfetchall(cursor)]

    # build the destination index sql
    dest_ix_sql = []
    for ix_dict in src_indexes:
        ix_name = ix_dict["indexname"]
        dest_ix_name = f"{ix_name}_temp"
        if dest_ix_name in dest_indexes:
            # if it's already made, ignore
            continue

        create_ix_sql = ix_dict["indexdef"]
        ix_regex = r"CREATE\s.*INDEX\s\S+\sON\s(\S+)\s.*"
        # this *should* match source_table, but can get funky with/without the schema included and regex
        # for example, a table 'x' in the public schema could be provided and the string will include `public.x'
        src_table = re.findall(ix_regex, create_ix_sql)[0]
        create_ix_sql = create_ix_sql.replace(ix_name, dest_ix_name)
        create_ix_sql = create_ix_sql.replace(src_table, dest_table)
        dest_ix_sql.append(create_ix_sql)
    return dest_ix_sql


def create_all_sql_strings(sql_json):
    """Desired ordering of steps for final SQL:
    1. Drop existing "_temp" and "_old" matviews
    2. Create new matview
    3. analyze verbose <matview>
    """
    final_sql_strings = []

    matview_name = sql_json["final_name"]
    matview_schema = sql_json.get("chunked_schema_name", "public")

    final_sql_strings.extend(make_matview_drops(matview_name))
    final_sql_strings.append("")
    final_sql_strings.extend(make_matview_create(f"{matview_schema}.{matview_name}", sql_json["matview_sql"]))

    final_sql_strings.append("")
    final_sql_strings.extend(make_modification_sql(matview_name, GLOBAL_ARGS.quiet))
    return final_sql_strings


def write_sql_file(str_list, filename):
    fname = filename + ".sql"

    print_debug("Creating file: {}".format(fname))
    with open(fname, "w") as f:
        fstring = "\n".join(str_list)
        f.write(fstring)
        f.write("\n")


def make_matview_empty(matview_name, chunk_count):
    sql_strings = []
    for i in range(chunk_count):
        chunk_name = f"{matview_name}_{i}"
        sql_strings.append(TEMPLATE["empty_matview"].format(chunk_name))

    return sql_strings


def create_componentized_files(sql_json):
    table_schema = sql_json.get("schema_name", "public")
    matview_name = sql_json["final_name"]
    matview_temp_name = matview_name + "_temp"
    filename_base = os.path.join(DEST_FOLDER, COMPONENT_DIR, sql_json["final_name"])

    create_table = make_temp_table_create(f"{table_schema}.{matview_name}", f"{table_schema}.{matview_temp_name}")
    write_sql_file(create_table, filename_base + "__create")

    sql_strings = make_rename_sql(matview_name)
    write_sql_file(sql_strings, filename_base + "__renames")

    sql_strings = make_modification_sql(matview_name, GLOBAL_ARGS.quiet)
    write_sql_file(sql_strings, filename_base + "__mods")

    sql_strings = make_table_drops(matview_name)
    write_sql_file(sql_strings, filename_base + "__drops")

    sql_strings = make_matview_empty(matview_name, GLOBAL_ARGS.chunk_count)
    write_sql_file(sql_strings, filename_base + "__empty")

    sql_strings = make_read_indexes(matview_name)
    write_sql_file(sql_strings, filename_base + "__indexes")

    sql_strings = make_read_constraints(matview_name)
    write_sql_file(sql_strings, filename_base + "__constraints")


def create_chunked_componentized_files(sql_json):
    table_name = sql_json["final_name"]
    matview_schema_name = sql_json.get("chunked_schema_name", "public")
    filename_base = os.path.join(DEST_FOLDER, COMPONENT_DIR, sql_json["final_name"])

    sql_strings = make_matview_drops(table_name)
    write_sql_file(sql_strings, filename_base + "__drops")

    sql_strings = make_matview_refresh(table_name, "")
    write_sql_file(sql_strings, filename_base + "__refresh")

    sql_strings = make_matview_create(f"{matview_schema_name}.{table_name}", sql_json["matview_sql"])
    write_sql_file(sql_strings, filename_base + "__matview")


def create_monolith_file(sql_json):
    sql_strings = create_all_sql_strings(sql_json)
    print_debug('Preparing to store "{}" in sql file'.format(sql_json["final_name"]))
    write_sql_file(sql_strings, os.path.join(DEST_FOLDER, sql_json["final_name"]))


def add_chunk_strings(sql_json, chunk):
    chunked_sql_json = copy.deepcopy(sql_json)

    chunk_count = GLOBAL_ARGS.chunk_count

    if chunk_count > 1:
        chunked_sql_json["final_name"] = f"{chunked_sql_json['final_name']}_{chunk}"
        chunked_sql_json["matview_sql"].append("  AND transaction_normalized.id % {} = {}".format(chunk_count, chunk))

    return chunked_sql_json


def print_debug(msg):
    if not GLOBAL_ARGS.quiet:
        print(msg)


def main(source_file):
    global UNIQUE_STRING
    commit_hash = generate_uid(9, source_file)
    random_chars = hashlib.md5(source_file.encode("utf-8")).hexdigest()[:3]
    UNIQUE_STRING = commit_hash + random_chars

    try:
        sql_json = ingest_json(source_file)
    except Exception as e:
        print("Error on Matview source JSON file: {}".format(source_file))
        print(e)
        raise SystemExit(1)

    create_componentized_files(sql_json)
    for chunk in range(0, GLOBAL_ARGS.chunk_count):
        chunked_sql_json = add_chunk_strings(sql_json, chunk)

        create_monolith_file(chunked_sql_json)
        create_chunked_componentized_files(chunked_sql_json)

    print_debug("Done")


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(
        prog="matview_sql_generator.py", description="Generates all of the necessary SQL files for jenkins scripts"
    )
    arg_parser.add_argument(
        "--dest", type=str, default="../chunked_matviews/", help="Destination folder for all generated sql files"
    )
    arg_parser.add_argument(
        "--file", type=str, default=None, help="filepath to the json file containing the sql description"
    )
    arg_parser.add_argument(
        "-q", "--quiet", action="store_true", help="Flag to suppress stdout when there are no errors"
    )
    arg_parser.add_argument(
        "-c", "--chunk-count", type=int, default=1, help="When value >=2, split matview into multiple SQL files"
    )
    GLOBAL_ARGS = arg_parser.parse_args()

    DEST_FOLDER = GLOBAL_ARGS.dest
    if not os.path.exists(os.path.join(DEST_FOLDER, COMPONENT_DIR)):
        os.makedirs(os.path.join(DEST_FOLDER, COMPONENT_DIR))

    if GLOBAL_ARGS.file is not None:
        if os.path.isfile(GLOBAL_ARGS.file):
            print_debug("Creating matview SQL using {}".format(GLOBAL_ARGS.file))
            main(GLOBAL_ARGS.file)
    else:
        all_files = glob.glob(os.path.join(HERE, "*.json"))
        for f in all_files:
            print_debug("\n==== {}".format(f))
            main(f)

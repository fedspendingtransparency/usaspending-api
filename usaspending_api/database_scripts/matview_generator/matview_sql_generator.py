#!/usr/bin/env python3

import argparse
import glob
import hashlib
import os

from shared_sql_generator import (
    COMPONENT_DIR,
    generate_uid,
    HERE,
    ingest_json,
    make_indexes_sql,
    make_matview_refresh,
    make_modification_sql,
    make_stats_sql,
    split_indexes_chunks,
    TEMPLATE,
)

# Usage: python matview_sql_generator.py (from usaspending_api/database_scripts/matview_generator)
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

    ],
    "index": {
        "name": "<name>",
        "columns": [
            {
                "name": "<col name>",
                "order": "DESC|ASC NULLS FIRST|LAST",
                "collation": "<collation>",
                "opclass": "<opclass"
            }
        ],
        "where": "<where clause>",
        "unique": true,
        "method": "<method>"
    }
}
"""


def make_matview_drops(final_matview_name):
    matview_temp_name = final_matview_name + "_temp"
    matview_archive_name = final_matview_name + "_old"

    return [TEMPLATE["drop_matview"].format(matview_temp_name), TEMPLATE["drop_matview"].format(matview_archive_name)]


def make_matview_create(final_matview_name, final_matview_schema_name, sql):
    matview_sql = "\n".join(sql)
    matview_temp_name = final_matview_name + "_temp"
    with_or_without_data = ""
    if GLOBAL_ARGS.no_data:
        with_or_without_data = "NO "
    matview_name_with_schema = f"{final_matview_schema_name}.{matview_temp_name}"
    return [TEMPLATE["create_matview"].format(matview_name_with_schema, matview_sql, with_or_without_data)]


def make_rename_sql(matview_name, old_indexes, old_stats, new_indexes, new_stats):
    matview_temp_name = matview_name + "_temp"
    matview_archive_name = matview_name + "_old"
    sql_strings = []
    sql_strings.append(TEMPLATE["rename_matview"].format("IF EXISTS ", matview_name, matview_archive_name))
    sql_strings += old_indexes
    sql_strings.append("")
    sql_strings += old_stats
    sql_strings.append("")
    sql_strings.append(TEMPLATE["rename_matview"].format("", matview_temp_name, matview_name))
    sql_strings += new_indexes
    sql_strings.append("")
    sql_strings += new_stats
    return sql_strings


def create_all_sql_strings(sql_json):
    """Desired ordering of steps for final SQL:
    1. Drop existing "_temp" and "_old" matviews
    2. Create new matview
    3. Create indexes for new matview
    4. (optional) Cluster matview on index
    5. analyze verbose <matview>
    6. Rename existing matview, append with _old
    7. Rename all existing matview indexes to avoid name collisions
    8. Rename new matview
    9. Rename new matview indexes
    """
    final_sql_strings = []

    matview_name = sql_json["final_name"]
    matview_schema_name = sql_json.get("schema_name", "public")
    matview_temp_name = matview_name + "_temp"

    create_indexes, rename_old_indexes, rename_new_indexes = make_indexes_sql(
        sql_json, matview_temp_name, UNIQUE_STRING, True, GLOBAL_ARGS.quiet
    )
    create_stats, rename_old_stats, rename_new_stats = make_stats_sql(sql_json, matview_temp_name, UNIQUE_STRING)

    final_sql_strings.extend(make_matview_drops(matview_name))
    final_sql_strings.append("")
    final_sql_strings.extend(make_matview_create(matview_name, matview_schema_name, sql_json["matview_sql"]))

    final_sql_strings.append("")
    final_sql_strings += create_indexes
    final_sql_strings.append("")
    final_sql_strings += create_stats
    final_sql_strings.append("")
    if GLOBAL_ARGS.no_data:
        final_sql_strings.extend([TEMPLATE["refresh_matview"].format("", matview_name), ""])
    final_sql_strings.extend(
        make_rename_sql(matview_name, rename_old_indexes, rename_old_stats, rename_new_indexes, rename_new_stats)
    )
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


def create_componentized_files(sql_json):
    filename_base = os.path.join(DEST_FOLDER, COMPONENT_DIR, sql_json["final_name"])
    index_dir_path = os.path.join(filename_base, "batch_indexes/")

    matview_name = sql_json["final_name"]
    matview_schema_name = sql_json.get("schema_name", "public")
    matview_temp_name = matview_name + "_temp"

    create_indexes, rename_old_indexes, rename_new_indexes = make_indexes_sql(
        sql_json, matview_temp_name, UNIQUE_STRING, True, GLOBAL_ARGS.quiet
    )
    create_stats, rename_old_stats, rename_new_stats = make_stats_sql(sql_json, matview_temp_name, UNIQUE_STRING)

    sql_strings = make_matview_drops(matview_name)
    write_sql_file(sql_strings, filename_base + "__drops")

    sql_strings = make_matview_create(matview_name, matview_schema_name, sql_json["matview_sql"])
    write_sql_file(sql_strings, filename_base + "__matview")

    indexes_and_stats = create_indexes + create_stats
    write_sql_file(indexes_and_stats, filename_base + "__indexes")

    if GLOBAL_ARGS.batch_indexes > 1:
        if not os.path.exists(index_dir_path):
            os.makedirs(index_dir_path)
        for i, index_block in enumerate(split_indexes_chunks(indexes_and_stats, GLOBAL_ARGS.batch_indexes)):
            write_sql_file(index_block, index_dir_path + "group_{}".format(i))

    sql_strings = make_modification_sql(matview_name, GLOBAL_ARGS.quiet)
    write_sql_file(sql_strings, filename_base + "__mods")

    sql_strings = make_rename_sql(
        matview_name, rename_old_indexes, rename_old_stats, rename_new_indexes, rename_new_stats
    )
    write_sql_file(sql_strings, filename_base + "__renames")

    if "refresh" in sql_json and sql_json["refresh"] is True:
        if GLOBAL_ARGS.no_data:
            sql_strings = make_matview_refresh(matview_temp_name, "")
        else:
            sql_strings = make_matview_refresh(matview_name)
        write_sql_file(sql_strings, filename_base + "__refresh")


def create_monolith_file(sql_json):
    sql_strings = create_all_sql_strings(sql_json)
    print_debug('Preparing to store "{}" in sql file'.format(sql_json["final_name"]))
    write_sql_file(sql_strings, os.path.join(DEST_FOLDER, sql_json["final_name"]))


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

    create_monolith_file(sql_json)
    create_componentized_files(sql_json)

    print_debug("Done")


def print_debug(msg):
    if not GLOBAL_ARGS.quiet:
        print(msg)


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(
        prog="matview_sql_generator.py", description="Generates all of the necessary SQL files for jenkins scripts"
    )
    arg_parser.add_argument(
        "--dest", type=str, default="../matviews/", help="Destination folder for all generated sql files"
    )
    arg_parser.add_argument(
        "--file", type=str, default=None, help="filepath to the json file containing the sql description"
    )
    arg_parser.add_argument(
        "-q", "--quiet", action="store_true", help="Flag to suppress stdout when there are no errors"
    )
    arg_parser.add_argument(
        "-b",
        "--batch_indexes",
        type=int,
        choices=range(1, 8),
        default=1,
        help="When value >=2, distribute the index SQL across that file count",
    )
    arg_parser.add_argument(
        "-n", "--no-data", action="store_true", help="Delay populating matview with data until indexes are created"
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

#!/usr/bin/env python3

import argparse
import glob
import hashlib
import json
import os
import subprocess
import copy

from uuid import uuid4

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

{   "final_name": "example_matview",
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

TEMPLATE = {
    "create_matview": "CREATE MATERIALIZED VIEW {} AS\n{} WITH {}DATA;",
    "drop_matview": "DROP MATERIALIZED VIEW IF EXISTS {} CASCADE;",
    "rename_matview": "ALTER MATERIALIZED VIEW {}{} RENAME TO {};",
    "cluster_matview": "CLUSTER VERBOSE {} USING {};",
    "refresh_matview": "REFRESH MATERIALIZED VIEW {}{} WITH DATA;",
    "analyze": "ANALYZE VERBOSE {};",
    "vacuum": "VACUUM ANALYZE VERBOSE {};",
    "create_index": "CREATE {}INDEX {} ON {} USING {}({}){}{};",
    "create_stats": "CREATE STATISTICS {} ON {} FROM {};",
    "rename_index": "ALTER INDEX {}{} RENAME TO {};",
    "rename_stats": "ALTER STATISTICS {} RENAME TO {};",
    "grant_select": "GRANT SELECT ON {} TO {};",
    "sql_print_output": "DO $$ BEGIN RAISE NOTICE '{}'; END $$;",
}

CLUSTERING_INDEX = None
COMMIT_HASH = ""
COMPONENT_DIR = "componentized/"
DEST_FOLDER = "../matviews/"
GLOBAL_ARGS = None
HERE = os.path.abspath(os.path.dirname(__file__))
MAX_NAME_LENGTH = 45  # postgres max 63 ascii chars
RANDOM_CHARS = ""


def ingest_json(path):
    with open(path) as f:
        doc = json.load(f)
    return doc


def generate_uid(characters=8, filename=None):
    git_hash = None
    try:
        git_hash = get_git_commit(characters - 1, filename)
    except Exception as e:
        print("Error: [{}]. Continuing...".format(e))
    if git_hash is None:
        return str(uuid4())[:characters]
    else:
        return git_hash + "$"


def get_git_commit(characters=8, filename=None):
    cmd = 'git log -n 1 --pretty=format:"%H"'
    cmd_args = cmd.split(" ")
    if filename and os.path.isfile(filename):
        cmd_args.append(filename)

    shell = subprocess.run(cmd_args, stdout=subprocess.PIPE, check=True)
    if shell.stdout:
        # First character is a '#' so skip it
        return shell.stdout[1 : characters + 1].decode()
    return None


def create_index_string(matview_name, index_name, idx):
    if idx.get("cluster_on_this", False):
        global CLUSTERING_INDEX
        CLUSTERING_INDEX = index_name
    idx_method = idx.get("method", "BTREE")  # if missing, defaults to BTREE
    idx_unique = "UNIQUE " if idx.get("unique", False) else ""
    idx_where = " WHERE " + idx["where"] if idx.get("where", None) else ""
    idx_with = ""
    if idx_method.upper() == "BTREE":
        idx_with = " WITH (fillfactor = 97)"  # reduce btree index size by 7% from the default 10% free-space

    idx_cols = []
    for col in idx["columns"]:
        index_def = [col["name"]]  # Critial to have col or expression. Exception if missing
        if col.get("order", None):  # if missing, skip and let postgres default to ASC
            index_def.append(col["order"])
        if col.get("collation", None):  # if missing, skip and let postgres use default
            index_def.append("COLLATE " + col["collation"])
        if col.get("opclass", None):  # if missing, skip and let postgres choose
            index_def.append(col["opclass"])
        idx_cols.append(" ".join(index_def))
    idx_str = TEMPLATE["create_index"].format(
        idx_unique, index_name, matview_name, idx_method, ", ".join(idx_cols), idx_with, idx_where
    )
    return idx_str


def split_indexes_chunks(index_list, file_count):
    """
        loop through all index strings (only the lines which start with "CREATE") and populate
        sublists with the full-set in a round-robin ordering to spread-out similar indexes
        to different workers as a poorman's averaging of the execution times
    """
    results = [list() for _ in range(file_count)]  # create empty lists for the index SQL strings
    for i, index in enumerate([index for index in index_list if index.startswith("CREATE")]):
        results[i % file_count].append(index)
    for result in results:
        yield result


def make_matview_drops(final_matview_name):
    matview_temp_name = final_matview_name + "_temp"
    matview_archive_name = final_matview_name + "_old"

    return [TEMPLATE["drop_matview"].format(matview_temp_name), TEMPLATE["drop_matview"].format(matview_archive_name)]


def make_matview_create(final_matview_name, sql):
    matview_sql = "\n".join(sql)
    matview_temp_name = final_matview_name + "_temp"
    with_or_without_data = ""
    if GLOBAL_ARGS.no_data:
        with_or_without_data = "NO "

    return [TEMPLATE["create_matview"].format(matview_temp_name, matview_sql, with_or_without_data)]


def make_matview_refresh(matview_name, concurrently="CONCURRENTLY "):
    statement_list = [TEMPLATE["refresh_matview"].format(concurrently, matview_name)]
    if concurrently == "CONCURRENTLY ":
        statement_list.append(TEMPLATE["vacuum"].format(matview_name))
    return statement_list


def make_indexes_sql(sql_json, matview_name):
    unique_name_list = []
    create_indexes = []
    rename_old_indexes = []
    rename_new_indexes = []
    for idx in sql_json["indexes"]:
        if len(idx["name"]) > MAX_NAME_LENGTH:
            raise Exception("Desired index name is too long. Keep under {} chars".format(MAX_NAME_LENGTH))

        final_index = "idx_" + COMMIT_HASH + RANDOM_CHARS + "_" + idx["name"]
        final_index = final_index.replace("-", "")
        unique_name_list.append(final_index)
        tmp_index = final_index + "_temp"
        old_index = final_index + "_old"

        idx_str = create_index_string(matview_name, tmp_index, idx)

        create_indexes.append(idx_str)
        rename_old_indexes.append(TEMPLATE["rename_index"].format("IF EXISTS ", final_index, old_index))
        rename_new_indexes.append(TEMPLATE["rename_index"].format("", tmp_index, final_index))

    if len(unique_name_list) != len(set(unique_name_list)):
        raise Exception("Name collision detected. Examine JSON file")
    total = len(create_indexes)
    print_debug("There are {} index creations".format(total))
    indexes_and_msg = []
    for n, index in enumerate(create_indexes):
        if n % 10 == 0 and n > 0:
            console = TEMPLATE["sql_print_output"].format("{} indexes created, {} remaining".format(n, total - n))
            indexes_and_msg.append(console)
        indexes_and_msg.append(index)

    return indexes_and_msg, rename_old_indexes, rename_new_indexes


def make_stats_sql(sql_json, matview_name):
    unique_name_list = []
    create_stats = []
    rename_new_stats = []
    rename_old_stats = []
    if not sql_json.get("stats"):
        return create_stats, rename_old_stats, rename_new_stats

    rename_old_stats = ["DO $$ BEGIN"]

    for stat in sql_json["stats"]:
        if len(stat["name"]) > MAX_NAME_LENGTH:
            raise Exception("Desired stat name is too long. Keep under {} chars".format(MAX_NAME_LENGTH))

        final_stat = "st_" + COMMIT_HASH + RANDOM_CHARS + "_" + stat["name"]
        final_stat = final_stat.replace("-", "")
        unique_name_list.append(final_stat)
        tmp_stat = final_stat + "_temp"
        old_stat = final_stat + "_old"

        columns = ", ".join(stat["columns"])
        stat_str = TEMPLATE["create_stats"].format(tmp_stat, columns, matview_name)

        create_stats.append(stat_str)
        rename_new_stats.append(TEMPLATE["rename_stats"].format(tmp_stat, final_stat))
        rename_old_stats.append(TEMPLATE["rename_stats"].format(final_stat, old_stat))

    rename_old_stats.append("EXCEPTION WHEN undefined_object THEN")
    rename_old_stats.append("RAISE NOTICE 'Skipping statistics renames, no conflicts'; END; $$ language 'plpgsql';")

    if len(unique_name_list) != len(set(unique_name_list)):
        raise Exception("Name collision detected. Examine JSON file")

    return create_stats, rename_old_stats, rename_new_stats


def make_modification_sql(matview_name):
    global CLUSTERING_INDEX
    sql_strings = []
    if CLUSTERING_INDEX:
        print_debug("*** This matview will be clustered on {} ***".format(CLUSTERING_INDEX))
        sql_strings.append(TEMPLATE["cluster_matview"].format(matview_name, CLUSTERING_INDEX))
    sql_strings.append(TEMPLATE["analyze"].format(matview_name))
    sql_strings.append(TEMPLATE["grant_select"].format(matview_name, "readonly"))
    return sql_strings


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
    """ Desired ordering of steps for final SQL:
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
    matview_temp_name = matview_name + "_temp"

    create_indexes, rename_old_indexes, rename_new_indexes = make_indexes_sql(sql_json, matview_temp_name)
    create_stats, rename_old_stats, rename_new_stats = make_stats_sql(sql_json, matview_temp_name)

    final_sql_strings.extend(make_matview_drops(matview_name))
    final_sql_strings.append("")
    final_sql_strings.extend(make_matview_create(matview_name, sql_json["matview_sql"]))

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
    final_sql_strings.extend(make_modification_sql(matview_name))
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
    matview_temp_name = matview_name + "_temp"

    create_indexes, rename_old_indexes, rename_new_indexes = make_indexes_sql(sql_json, matview_temp_name)
    create_stats, rename_old_stats, rename_new_stats = make_stats_sql(sql_json, matview_temp_name)

    sql_strings = make_matview_drops(matview_name)
    write_sql_file(sql_strings, filename_base + "__drops")

    sql_strings = make_matview_create(matview_name, sql_json["matview_sql"])
    write_sql_file(sql_strings, filename_base + "__matview")

    indexes_and_stats = create_indexes + create_stats
    write_sql_file(indexes_and_stats, filename_base + "__indexes")

    if GLOBAL_ARGS.batch_indexes > 1:
        if not os.path.exists(index_dir_path):
            os.makedirs(index_dir_path)
        for i, index_block in enumerate(split_indexes_chunks(indexes_and_stats, GLOBAL_ARGS.batch_indexes)):
            write_sql_file(index_block, index_dir_path + "group_{}".format(i))

    sql_strings = make_modification_sql(matview_name)
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


def add_chunk_strings(sql_json, chunk):
    chunked_sql_json = copy.deepcopy(sql_json)

    chunk_count = GLOBAL_ARGS.chunks

    if chunk_count > 1:
        chunked_sql_json["final_name"] += "_{}".format(chunk)
        chunked_sql_json["matview_sql"].append("  AND transaction_normalized.id % {} = {}".format(chunk_count, chunk))

    return chunked_sql_json


def main(source_file):
    global COMMIT_HASH
    global RANDOM_CHARS
    COMMIT_HASH = generate_uid(9, source_file)
    RANDOM_CHARS = hashlib.md5(source_file.encode("utf-8")).hexdigest()[:3]

    try:
        sql_json = ingest_json(source_file)
    except Exception as e:
        print("Error on Matview source JSON file: {}".format(source_file))
        print(e)
        raise SystemExit(1)

    for chunk in range(0, GLOBAL_ARGS.chunks):
        chunked_sql_json = add_chunk_strings(sql_json, chunk)

        create_monolith_file(chunked_sql_json)
        create_componentized_files(chunked_sql_json)

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
    arg_parser.add_argument("-c", "--chunks", type=int, default=1, help="When value >=2, split matview into multiple SQL files")
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

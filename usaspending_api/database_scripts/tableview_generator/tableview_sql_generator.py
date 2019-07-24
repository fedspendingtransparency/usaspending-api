"""
A TableView is
"""
import argparse
import glob
import hashlib
import json
import os
import subprocess

from uuid import uuid4

# Usage: python tableview_sql_generator.py (from usaspending_api/database_scripts/tableview_generator)
#        ^--- Will clobber files in usaspending_api/database_scripts/tableviews

"""
POSTGRES INDEX FORMAT
    CREATE [ UNIQUE ] INDEX [ name ] ON table_name [ USING method ]
    ( { column_name | ( expression ) } [ COLLATE collation ]
        [ opclass ] [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [, ...] )
    [ WITH ( storage_parameter = value [, ... ] ) ]
    [ WHERE predicate ]

EXAMPLE SQL DESCRIPTION JSON FILE:

{
    "name": "example_tableview",
    "clean_up": ["truncate table transaction_normalized_change_tracker;"],
    "update_where": ["and tn.id in (select transaction_id from transaction_normalized_change_tracker)"],
    "update": [
        "delete from",
        "",
        "    {table_name}",
        "",
        "where",
        "",
        "    transaction_id in (select transaction_id from transaction_normalized_change_tracker);",
        "",
        "",
        "insert into",
        "",
        "    {table_name}",
        "",
        "{select};"
    ],
    "create": [
        "create table",
        "",
        "    {table_name} as",
        "",
        "{select};"
    ],
    "select": [
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
    "drop": "DROP TABLE IF EXISTS {} CASCADE;",
    "rename": "ALTER TABLE {}{} RENAME TO {};",
    "cluster": "CLUSTER VERBOSE {} USING {};",
    "analyze": "ANALYZE VERBOSE {};",
    "vacuum": "VACUUM ANALYZE VERBOSE {};",
    "create_index": "CREATE {}INDEX {} ON {} USING {}({}){}{};",
    "rename_index": "ALTER INDEX {}{} RENAME TO {};",
    "grant_select": "GRANT SELECT ON {} TO {};",
    "sql_print_output": "DO $$ BEGIN RAISE NOTICE '{}'; END $$;",
}

CLUSTERING_INDEX = None
COMMIT_HASH = ""
COMPONENT_DIR = "componentized/"
DEST_FOLDER = "../tableviews/"
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


def create_index_string(name, index_name, idx):
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
        idx_unique, index_name, name, idx_method, ", ".join(idx_cols), idx_with, idx_where
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


def make_drops(final_name):
    temp_name = final_name + "_temp"
    archive_name = final_name + "_old"

    return [TEMPLATE["drop"].format(temp_name), TEMPLATE["drop"].format(archive_name)]


def make_create(final_name, sql_json):
    temp_name = final_name + "_temp"
    select = "\n".join(sql_json["select"]).format(update_where="")
    create = "\n".join(sql_json["create"]).format(table_name=temp_name, select=select)

    return [create]


def make_update(final_name, sql_json):
    update_where = "\n".join(sql_json["update_where"])
    select = "\n".join(sql_json["select"]).format(update_where=update_where)
    update = "\n".join(sql_json["update"]).format(table_name=final_name, select=select)
    update += "\n\n\n" + TEMPLATE["vacuum"].format(final_name)
    update += "\n\n\n" + "\n".join(make_clean_up(sql_json))

    return [update]


def make_clean_up(sql_json):
    """
    We will likely need to do something different with this if/when we start
    adding more matview -> tableview conversions since we may not want to
    clean up until after all tableviews have been created/updated.
    """
    return ["\n".join(sql_json["clean_up"])]


def make_indexes_sql(sql_json, name):
    unique_name_list = []
    create_indexes = []
    rename_old_indexes = []
    rename_new_indexes = []
    for idx in sql_json["indexes"]:
        if len(idx["name"]) > MAX_NAME_LENGTH:
            raise Exception("Desired index name is too long. Keep under {} chars".format(MAX_NAME_LENGTH))

        final_index = "idx_tv_" + COMMIT_HASH + RANDOM_CHARS + "_" + idx["name"]
        final_index = final_index.replace("-", "")
        unique_name_list.append(final_index)
        tmp_index = final_index + "_temp"
        old_index = final_index + "_old"

        idx_str = create_index_string(name, tmp_index, idx)

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


def make_modification_sql(name):
    global CLUSTERING_INDEX
    sql_strings = []
    if CLUSTERING_INDEX:
        print_debug("*** This tableview will be clustered on {} ***".format(CLUSTERING_INDEX))
        sql_strings.append(TEMPLATE["cluster"].format(name, CLUSTERING_INDEX))
    sql_strings.append(TEMPLATE["analyze"].format(name))
    sql_strings.append(TEMPLATE["grant_select"].format(name, "readonly"))
    return sql_strings


def make_rename_sql(name, old_indexes, new_indexes):
    temp_name = name + "_temp"
    archive_name = name + "_old"
    sql_strings = []
    sql_strings.append(TEMPLATE["rename"].format("IF EXISTS ", name, archive_name))
    sql_strings += old_indexes
    sql_strings.append("")
    sql_strings.append(TEMPLATE["rename"].format("", temp_name, name))
    sql_strings += new_indexes
    return sql_strings


def create_all_sql_strings(sql_json):
    """ Desired ordering of steps for final SQL:
        1. Drop existing "_temp" and "_old" tableviews
        2. Create new tableview
        3. Create indexes for new tableview
        4. (optional) Cluster tableview on index
        5. analyze verbose <tableview>
        6. Rename existing tableview, append with _old
        7. Rename all existing tableview indexes to avoid name collisions
        8. Rename new tableview
        9. Rename new tableview indexes
       10. Clean up
    """
    final_sql_strings = []

    name = sql_json["name"]
    temp_name = name + "_temp"

    create_indexes, rename_old_indexes, rename_new_indexes = make_indexes_sql(sql_json, temp_name)

    final_sql_strings.extend(make_drops(name))
    final_sql_strings.append("")
    final_sql_strings.extend(make_create(name, sql_json))

    final_sql_strings.append("")
    final_sql_strings += create_indexes
    final_sql_strings.append("")
    final_sql_strings.extend(make_rename_sql(name, rename_old_indexes, rename_new_indexes))
    final_sql_strings.append("")
    final_sql_strings.extend(make_modification_sql(name))
    final_sql_strings.append("")
    final_sql_strings.extend(make_clean_up(sql_json))

    return final_sql_strings


def write_sql_file(str_list, filename):
    fname = filename + ".sql"

    print_debug("Creating file: {}".format(fname))
    with open(fname, "w") as f:
        fstring = "\n".join(str_list)
        f.write(fstring)
        f.write("\n")


def create_componentized_files(sql_json):
    filename_base = os.path.join(DEST_FOLDER, COMPONENT_DIR, sql_json["name"])
    index_dir_path = os.path.join(filename_base, "batch_indexes/")

    name = sql_json["name"]
    temp_name = name + "_temp"

    create_indexes, rename_old_indexes, rename_new_indexes = make_indexes_sql(sql_json, temp_name)

    sql_strings = make_drops(name)
    write_sql_file(sql_strings, filename_base + "__drops")

    sql_strings = make_create(name, sql_json)
    write_sql_file(sql_strings, filename_base + "__tableview")

    write_sql_file(create_indexes, filename_base + "__indexes")

    if GLOBAL_ARGS.batch_indexes > 1:
        if not os.path.exists(index_dir_path):
            os.makedirs(index_dir_path)
        for i, index_block in enumerate(split_indexes_chunks(create_indexes, GLOBAL_ARGS.batch_indexes)):
            write_sql_file(index_block, index_dir_path + "group_{}".format(i))

    sql_strings = make_modification_sql(name)
    write_sql_file(sql_strings, filename_base + "__mods")

    sql_strings = make_rename_sql(name, rename_old_indexes, rename_new_indexes)
    write_sql_file(sql_strings, filename_base + "__renames")

    sql_strings = make_update(name, sql_json)
    write_sql_file(sql_strings, filename_base + "__update")

    sql_strings = make_clean_up(sql_json)
    write_sql_file(sql_strings, filename_base + "__clean_up")


def create_monolith_file(sql_json):
    sql_strings = create_all_sql_strings(sql_json)
    print_debug('Preparing to store "{}" in sql file'.format(sql_json["name"]))
    write_sql_file(sql_strings, os.path.join(DEST_FOLDER, sql_json["name"]))


def main(source_file):
    global COMMIT_HASH
    global RANDOM_CHARS
    COMMIT_HASH = generate_uid(9, source_file)
    RANDOM_CHARS = hashlib.md5(source_file.encode("utf-8")).hexdigest()[:3]

    try:
        sql_json = ingest_json(source_file)
    except Exception as e:
        print("Error on tableview source JSON file: {}".format(source_file))
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
        prog="tableview_sql_generator.py",
        description="Generates all of the necessary SQL files for jenkins scripts to work with our 'table views' "
        "(our old materialized views that are now tables)",
    )
    arg_parser.add_argument(
        "--dest", type=str, default="../tableviews/", help="Destination folder for all generated sql files"
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
    GLOBAL_ARGS = arg_parser.parse_args()

    DEST_FOLDER = GLOBAL_ARGS.dest
    if not os.path.exists(os.path.join(DEST_FOLDER, COMPONENT_DIR)):
        os.makedirs(os.path.join(DEST_FOLDER, COMPONENT_DIR))

    if GLOBAL_ARGS.file is not None:
        if os.path.isfile(GLOBAL_ARGS.file):
            print_debug("Creating tableview SQL using {}".format(GLOBAL_ARGS.file))
            main(GLOBAL_ARGS.file)
    else:
        all_files = glob.glob(os.path.join(HERE, "*.json"))
        for f in all_files:
            print_debug("\n==== {}".format(f))
            main(f)

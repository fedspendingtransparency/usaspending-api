import json
import subprocess
import os

from uuid import uuid4

TEMPLATE = {
    "create_matview": "CREATE MATERIALIZED VIEW {} AS\n{} WITH {}DATA;",
    "create_table": "CREATE TABLE {} AS SELECT * from {} WITH NO DATA;",
    "drop_table": "DROP TABLE IF EXISTS {} CASCADE;",
    "drop_matview": "DROP MATERIALIZED VIEW IF EXISTS {} CASCADE;",
    "rename_matview": "ALTER MATERIALIZED VIEW {}{} RENAME TO {};",
    "rename_table": "ALTER TABLE {}{} RENAME TO {};",
    "cluster_matview": "CLUSTER VERBOSE {} USING {};",
    "refresh_matview": "REFRESH MATERIALIZED VIEW {}{} WITH DATA;",
    "empty_matview": "REFRESH MATERIALIZED VIEW {} WITH NO DATA;",
    "analyze": "ANALYZE VERBOSE {};",
    "vacuum": "VACUUM VERBOSE ANALYZE {};",
    "create_index": "CREATE {}INDEX {} ON {} USING {}({}){}{};",
    "create_stats": "CREATE STATISTICS {} ON {} FROM {};",
    "rename_index": "ALTER INDEX {}{} RENAME TO {};",
    "rename_stats": "ALTER STATISTICS {} RENAME TO {};",
    "grant_select": "GRANT SELECT ON {} TO {};",
    "sql_print_output": "DO $$ BEGIN RAISE NOTICE '{}'; END $$;",
    "read_indexes": "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = '{}' AND tablename = '{}';",
    "read_constraints": "select conname, pg_get_constraintdef(oid) from pg_constraint where contype IN ('p', 'f') and conrelid = '{}'::regclass;",
}


CLUSTERING_INDEX = None
COMPONENT_DIR = "componentized/"
HERE = os.path.abspath(os.path.dirname(__file__))
MAX_NAME_LENGTH = 45  # postgres max 63 ascii chars


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
        index_def = [col["name"]]  # Critical to have col or expression. Exception if missing
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


def make_table_drops(table_name):
    table_old_name = table_name + "_old"
    table_temp_name = table_name + "_temp"
    return [TEMPLATE["drop_table"].format(table_old_name), TEMPLATE["drop_table"].format(table_temp_name)]


def make_matview_refresh(matview_name, concurrently="CONCURRENTLY "):
    statement_list = [TEMPLATE["refresh_matview"].format(concurrently, matview_name)]
    if concurrently == "CONCURRENTLY ":
        statement_list.append(TEMPLATE["vacuum"].format(matview_name))
    return statement_list


def make_indexes_sql(sql_json, entity_name, unique_string, progress_sql, quiet):
    unique_name_list = []
    create_indexes = []
    rename_old_indexes = []
    rename_new_indexes = []
    for idx in sql_json["indexes"]:
        if len(idx["name"]) > MAX_NAME_LENGTH:
            raise Exception("Desired index name is too long. Keep under {} chars".format(MAX_NAME_LENGTH))

        final_index = "idx_" + unique_string + "_" + idx["name"]
        final_index = final_index.replace("-", "")
        unique_name_list.append(final_index)
        tmp_index = final_index + "_temp"
        old_index = final_index + "_old"

        idx_str = create_index_string(entity_name, tmp_index, idx)

        create_indexes.append(idx_str)
        rename_old_indexes.append(TEMPLATE["rename_index"].format("IF EXISTS ", final_index, old_index))
        rename_new_indexes.append(TEMPLATE["rename_index"].format("", tmp_index, final_index))

    if len(unique_name_list) != len(set(unique_name_list)):
        raise Exception("Name collision detected. Examine JSON file")
    total = len(create_indexes)

    if not quiet:
        print("There are {} index creations".format(total))

    indexes_and_msg = []
    for n, index in enumerate(create_indexes):
        if n % 10 == 0 and n > 0 and progress_sql:
            console = TEMPLATE["sql_print_output"].format("{} indexes created, {} remaining".format(n, total - n))
            indexes_and_msg.append(console)
        indexes_and_msg.append(index)

    return indexes_and_msg, rename_old_indexes, rename_new_indexes


def make_modification_sql(entity_name, quiet):
    global CLUSTERING_INDEX
    sql_strings = []
    if CLUSTERING_INDEX:
        if not quiet:
            print("*** This matview will be clustered on {} ***".format(CLUSTERING_INDEX))
        sql_strings.append(TEMPLATE["cluster_matview"].format(entity_name, CLUSTERING_INDEX))
    sql_strings.append(TEMPLATE["analyze"].format(entity_name))
    sql_strings.append(TEMPLATE["grant_select"].format(entity_name, "readonly"))
    return sql_strings


def make_stats_sql(sql_json, matview_name, unique_string):
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

        final_stat = "st_" + unique_string + "_" + stat["name"]
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

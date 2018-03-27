import argparse
import glob
import json
import os
import subprocess
from uuid import uuid4

# Usage: python matview_sql_generator.py (from usaspending_api/database_scripts/matview_generator)
#        ^--- Will clobber files in usaspending_api/database_scripts/matviews

'''
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
    "  \"transaction_normalized\".\"action_date\",",
    "  \"transaction_normalized\".\"fiscal_year\",",
    "  \"awards\".\"type\",",
    "  \"awards\".\"category\",",
    "FROM",
    "  \"awards\"",
    "LEFT OUTER JOIN",
    "  \"transaction_normalized\" ON (\"awards\".\"latest_transaction_id\" = \"transaction_normalized\".\"id\")",
    "WHERE",
    "  \"transaction_normalized\".action_date >= '2007-10-01'",
    "ORDER BY",
    "  \"action_date\" DESC"

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
'''

TEMPLATE = {
    'create_matview': 'CREATE MATERIALIZED VIEW {} AS\n{};',
    'drop_matview': 'DROP MATERIALIZED VIEW IF EXISTS {} CASCADE;',
    'rename_matview': 'ALTER MATERIALIZED VIEW {}{} RENAME TO {};',
    'cluster_matview': 'CLUSTER VERBOSE {} USING {};',
    'refresh_matview': 'REFRESH MATERIALIZED VIEW CONCURRENTLY {} WITH DATA;',
    'analyze': 'ANALYZE VERBOSE {};',
    'vacuum': 'VACUUM ANALYZE VERBOSE {};',
    'create_index': 'CREATE {}INDEX {} ON {} USING {}({}){}{};',
    'rename_index': 'ALTER INDEX {}{} RENAME TO {};',
    'grant_select': 'GRANT SELECT ON {} TO {};',
}
HEADER = [
    '--------------------------------------------------------',
    '-- Created using matview_sql_generator.py             --',
    '--    The SQL definition is stored in a json file     --',
    '--    Look in matview_generator for the code.         --',
    '--                                                    --',
    '--         !!DO NOT DIRECTLY EDIT THIS FILE!!         --',
    '--------------------------------------------------------',
]
CLUSTERING_INDEX = None
COMMIT_HASH = ''
DEST_FOLDER = '../matviews/'
COMPONENT_DIR = 'componentized/'
HERE = os.path.abspath(os.path.dirname(__file__))
MAX_NAME_LENGTH = 45  # postgres max 63 ascii chars
RANDOM_CHARS = ''


def ingest_json(path):
    with open(path) as f:
        doc = json.load(f)
    return doc


def generate_uid(characters=8, filename=None):
    git_hash = None
    try:
        git_hash = get_git_commit(characters - 1, filename)
    except Exception as e:
        print('Error: [{}]. Continuing...'.format(e))
    if git_hash is None:
        return str(uuid4())[:characters]
    else:
        return git_hash + '$'


def get_git_commit(characters=8, filename=None):
    cmd = 'git log -n 1 --pretty=format:"%H"'
    args = cmd.split(' ')
    if filename and os.path.isfile(filename):
        args.append(filename)

    shell = subprocess.run(args, stdout=subprocess.PIPE, check=True)
    if shell.stdout:
        # First character is a '#' so skip it
        return shell.stdout[1:characters + 1].decode()
    return None


def create_index_string(matview_name, index_name, idx):
    if idx.get('cluster_on_this', False):
        global CLUSTERING_INDEX
        CLUSTERING_INDEX = index_name
    idx_method = idx.get('method', 'BTREE')  # if missing, defaults to BTREE
    idx_unique = 'UNIQUE ' if idx.get('unique', False) else ''
    idx_where = ' WHERE ' + idx['where'] if idx.get('where', None) else ''
    idx_with = ''
    if idx_method.upper() == 'BTREE':
        idx_with = ' WITH (fillfactor = 97)'  # reduce btree index size by 7% from the default 10% free-space

    idx_cols = []
    for col in idx['columns']:
        index_def = [col["name"]]  # Critial to have col or expression. Exception if missing
        if col.get('order', None):  # if missing, skip and let postgres default to ASC
            index_def.append(col['order'])
        if col.get('collation', None):  # if missing, skip and let postgres use default
            index_def.append('COLLATE ' + col['collation'])
        if col.get('opclass', None):  # if missing, skip and let postgres choose
            index_def.append(col['opclass'])
        idx_cols.append(' '.join(index_def))
    idx_str = TEMPLATE['create_index'].format(
        idx_unique,
        index_name,
        matview_name,
        idx_method,
        ', '.join(idx_cols),
        idx_with,
        idx_where,
    )
    return idx_str


def make_sql_header():
    return ['\n'.join(HEADER)]


def make_matview_drops(final_matview_name):
    matview_temp_name = final_matview_name + '_temp'
    matview_archive_name = final_matview_name + '_old'

    return [
        TEMPLATE['drop_matview'].format(matview_temp_name),
        TEMPLATE['drop_matview'].format(matview_archive_name)
    ]


def make_matview_create(final_matview_name, sql):
    matview_sql = '\n'.join(sql)
    matview_temp_name = final_matview_name + '_temp'
    return [TEMPLATE['create_matview'].format(matview_temp_name, matview_sql)]


def make_matview_refresh(matview_name):
    return [
        TEMPLATE['refresh_matview'].format(matview_name),
        TEMPLATE['analyze'].format(matview_name)
    ]


def make_indexes_sql(sql_json, matview_name):
    unique_name_list = []
    create_indexes = []
    rename_old_indexes = []
    rename_new_indexes = []
    for idx in sql_json['indexes']:
        if len(idx['name']) > MAX_NAME_LENGTH:
            raise Exception('Desired index name is too long. Keep under {} chars'.format(MAX_NAME_LENGTH))

        final_index = 'idx_' + COMMIT_HASH + RANDOM_CHARS + '_' + idx['name']
        unique_name_list.append(final_index)
        tmp_index = final_index + '_temp'
        old_index = final_index + '_old'

        idx_str = create_index_string(matview_name, tmp_index, idx)

        create_indexes.append(idx_str)
        rename_old_indexes.append(TEMPLATE['rename_index'].format('IF EXISTS ', final_index, old_index))
        rename_new_indexes.append(TEMPLATE['rename_index'].format('', tmp_index, final_index))

    if len(unique_name_list) != len(set(unique_name_list)):
        raise Exception('Name collision detected. Examine JSON file')
    print('There are {} index creations'.format(len(create_indexes)))

    return create_indexes, rename_old_indexes, rename_new_indexes


def make_modification_sql(matview_name):
    global CLUSTERING_INDEX
    sql_strings = []
    if CLUSTERING_INDEX:
        print('*** This matview will be clustered on {} ***'.format(CLUSTERING_INDEX))
        sql_strings.append(TEMPLATE['cluster_matview'].format(matview_name, CLUSTERING_INDEX))
    sql_strings.append(TEMPLATE['analyze'].format(matview_name))
    sql_strings.append(TEMPLATE['grant_select'].format(matview_name, 'readonly'))
    return sql_strings


def make_rename_sql(matview_name, old_indexes, new_indexes):
    matview_temp_name = matview_name + '_temp'
    matview_archive_name = matview_name + '_old'
    sql_strings = []
    sql_strings.append(TEMPLATE['rename_matview'].format('IF EXISTS ', matview_name, matview_archive_name))
    sql_strings += old_indexes
    sql_strings.append('')
    sql_strings.append(TEMPLATE['rename_matview'].format('', matview_temp_name, matview_name))
    sql_strings += new_indexes
    return sql_strings


def create_all_sql_strings(sql_json):
    ''' Desired ordering of steps for final SQL:
        1. Drop existing "_temp" and "_old" matviews
        2. Create new matview
        3. Create indexes for new matview
        4. (optional) Cluster matview on index
        5. analyze verbose <matview>
        6. Rename existing matview, append with _old
        7. Rename all existing matview indexes to avoid name collisions
        8. Rename new matview
        9. Rename new matview indexes
    '''
    final_sql_strings = []

    matview_name = sql_json['final_name']
    matview_temp_name = matview_name + '_temp'

    create_indexes, rename_old_indexes, rename_new_indexes = make_indexes_sql(sql_json, matview_temp_name)

    final_sql_strings.extend(make_sql_header())
    final_sql_strings.extend(make_matview_drops(matview_name))
    final_sql_strings.append('')
    final_sql_strings.extend(make_matview_create(matview_name, sql_json['matview_sql']))

    final_sql_strings.append('')
    final_sql_strings += create_indexes
    final_sql_strings.append('')
    final_sql_strings.extend(make_rename_sql(matview_name, rename_old_indexes, rename_new_indexes))
    final_sql_strings.append('')
    final_sql_strings.extend(make_modification_sql(matview_name))
    return final_sql_strings


def write_sql_file(str_list, filename):
    fname = filename + '.sql'

    print('Creating file: {}'.format(fname))
    with open(fname, 'w') as f:
        fstring = '\n'.join(str_list)
        f.write(fstring)
        f.write('\n')


def create_componentized_files(sql_json):
    filename_base = os.path.join(DEST_FOLDER, COMPONENT_DIR, sql_json['final_name'])

    matview_name = sql_json['final_name']
    matview_temp_name = matview_name + '_temp'

    create_indexes, rename_old_indexes, rename_new_indexes = make_indexes_sql(sql_json, matview_temp_name)

    sql_strings = make_sql_header() + make_matview_drops(matview_name)
    write_sql_file(sql_strings, filename_base + '__drops')

    sql_strings = make_sql_header() + make_matview_create(matview_name, sql_json['matview_sql'])
    write_sql_file(sql_strings, filename_base + '__matview')

    sql_strings = make_sql_header() + create_indexes
    write_sql_file(sql_strings, filename_base + '__indexes')

    sql_strings = make_sql_header() + make_modification_sql(matview_name)
    write_sql_file(sql_strings, filename_base + '__mods')

    sql_strings = make_sql_header() + make_rename_sql(matview_name, rename_old_indexes, rename_new_indexes)
    write_sql_file(sql_strings, filename_base + '__renames')

    if 'refresh' in sql_json and sql_json['refresh'] is True:
        sql_strings = make_sql_header() + make_matview_refresh(matview_name)
        write_sql_file(sql_strings, filename_base + '__refresh')


def create_monolith_file(sql_json):
    sql_strings = create_all_sql_strings(sql_json)
    print('Preparing to store "{}" in sql file'.format(sql_json['final_name']))
    write_sql_file(sql_strings, os.path.join(DEST_FOLDER, sql_json['final_name']))


def main(source_file):
    global COMMIT_HASH
    global RANDOM_CHARS
    COMMIT_HASH = generate_uid(9, source_file)
    RANDOM_CHARS = str(uuid4())[:3]

    try:
        sql_json = ingest_json(source_file)
    except Exception as e:
        print(e)
        raise SystemExit
    create_monolith_file(sql_json)
    create_componentized_files(sql_json)
    print('Done')


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(
        prog='matview_sql_generator.py',
        description='Generates all of the necessary SQL files for jenkins scripts')
    arg_parser.add_argument(
        '--dest',
        type=str,
        default='../matviews/',
        help='Destination folder for all generated sql files')
    arg_parser.add_argument(
        '--file',
        type=str,
        default=None,
        help='filepath to the json file containing the sql description')
    args = arg_parser.parse_args()

    DEST_FOLDER = args.dest
    if not os.path.exists(os.path.join(DEST_FOLDER, COMPONENT_DIR)):
        os.makedirs(os.path.join(DEST_FOLDER, COMPONENT_DIR))

    if args.file is not None:
        if os.path.isfile(args.file):
            print('Creating matview SQL using {}'.format(args.file))
            main(args.file)
    else:
        all_files = glob.glob(os.path.join(HERE, '*.json'))
        for f in all_files:
            print('\n==== {}'.format(f))
            main(f)

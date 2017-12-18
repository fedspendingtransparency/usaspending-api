import json
import os
import sys
from uuid import uuid4

'''
POSTGRES INDEX FORMAT
    CREATE [ UNIQUE ] INDEX [ name ] ON table_name [ USING method ]
    ( { column_name | ( expression ) } [ COLLATE collation ] [ opclass ] [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [, ...] )
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
    'drop_matview': 'DROP MATERIALIZED VIEW IF EXISTS {};',
    'rename_matview': 'ALTER MATERIALIZED VIEW {}{} RENAME TO {};',
    'cluster_matview': 'CLUSTER VERBOSE {} USING {};',
    'vacuum': 'VACUUM ANALYZE VERBOSE {};',
    'create_index': 'CREATE {}INDEX {} ON {} USING {}({}){}{};',
    'rename_index': 'ALTER INDEX {}{} RENAME TO {};',
}
DEST_FOLDER = '../matviews/'
OVERWRITE_FILE = True
HEADER = [
    '--------------------------------------------------------',
    '-- Created using matview_sql_generator.py             --',
    '--    The SQL definition is stored in a json file     --',
    '--    Look in matview_generator for the code.         --',
    '--                                                    --',
    '--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --',
    '--------------------------------------------------------',
]
MAX_NAME_LENGTH = 45  # postgres max 63 ascii chars
RANDOM_CHARS = str(uuid4())[:8]
CLUSTERING_INDEX = None


def ingest_json(path):
    with open(path) as f:
        doc = json.load(f)
    return doc


def create_index_string(matview_name, index_name, idx):
    if idx.get('cluster_on_this', False):
        global CLUSTERING_INDEX
        CLUSTERING_INDEX = index_name
    idx_method = idx.get('method', 'BTREE')  # if missing, defaults to BTREE
    idx_unique = 'UNIQUE ' if idx.get('unique', False) else ''
    idx_where = ' WHERE ' + idx['where'] if idx.get('where', None) else ''
    idx_with = ''
    if idx_method.upper() == 'BTREE':
        idx_with = ' WITH (fillfactor = 100)'  # reduce btree index size by 10%

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


def create_sql_strings(sql_json):
    ''' Steps:
        1. Drop temp-named matviews
        2. Create matview
        3. Create temp indexes
        4. Rename existing matview, append _old
        5. Rename all existing matview indexes
        6. Rename new matview
        7. Rename new matview indexes
        8. Cluster matview on index
        9. vacuum analyze verbose
    '''
    final_sql_strings = []
    create_indexes = []
    rename_old_indexes = []
    rename_new_indexes = []

    matview_name = sql_json['final_name']
    matview_temp_name = matview_name + '_temp'
    matview_archive_name = matview_name + '_old'
    matview_sql = '\n'.join(sql_json['matview_sql'])

    final_sql_strings.append('\n'.join(HEADER))
    final_sql_strings.append(TEMPLATE['drop_matview'].format(matview_temp_name))
    final_sql_strings.append(TEMPLATE['drop_matview'].format(matview_archive_name))
    final_sql_strings.append('')
    final_sql_strings.append(TEMPLATE['create_matview'].format(matview_temp_name, matview_sql))

    for idx in sql_json['indexes']:
        if len(idx['name']) > MAX_NAME_LENGTH:
            raise Exception('Desired index name is too long. Keep under {} chars'.format(MAX_NAME_LENGTH))
        final_index = 'idx_' + RANDOM_CHARS + '__' + idx['name']
        tmp_index = final_index + '_temp'
        old_index = final_index + '_old'

        idx_str = create_index_string(matview_temp_name, tmp_index, idx)

        create_indexes.append(idx_str)
        rename_old_indexes.append(TEMPLATE['rename_index'].format('IF EXISTS ', final_index, old_index))
        rename_new_indexes.append(TEMPLATE['rename_index'].format('', tmp_index, final_index))

    print('There are {} index creations'.format(len(create_indexes)))

    final_sql_strings.append('')
    final_sql_strings += create_indexes
    final_sql_strings.append('')
    if CLUSTERING_INDEX:
        print('*** This matview has clustering on {} ***'.format(CLUSTERING_INDEX))
        final_sql_strings.append(TEMPLATE['cluster_matview'].format(matview_temp_name, CLUSTERING_INDEX))
        final_sql_strings.append('')
    final_sql_strings.append(TEMPLATE['vacuum'].format(matview_temp_name))
    final_sql_strings.append('')
    final_sql_strings.append(TEMPLATE['rename_matview'].format('IF EXISTS ', matview_name, matview_archive_name))
    final_sql_strings += rename_old_indexes
    final_sql_strings.append('')
    final_sql_strings.append(TEMPLATE['rename_matview'].format('', matview_temp_name, matview_name))
    final_sql_strings += rename_new_indexes
    final_sql_strings.append('')
    return final_sql_strings


def write_sql_file(str_list, filename):
    fname = filename + '.sql'
    if not OVERWRITE_FILE:
        sequence = 0
        while os.path.isfile(fname):
            sequence += 1
            fname = filename + str(sequence) + '.sql'

    print('Creating file: {}'.format(fname))
    with open(fname, 'w') as f:
        fstring = '\n'.join(str_list)
        f.write(fstring)


def main(source_file):
    try:
        sql_json = ingest_json(source_file)
    except Exception as e:
        print(e)
        raise SystemExit
    sql_strings = create_sql_strings(sql_json)
    print('Preparing to store "{}" in sql file'.format(sql_json['final_name']))
    write_sql_file(sql_strings, DEST_FOLDER + sql_json['final_name'])
    print('Done')


if __name__ == '__main__':
    if len(sys.argv) > 1:
        print('Creating matview SQL using {}'.format(sys.argv[1]))
        main(sys.argv[1])
    else:
        print('I need a json file with sql info')

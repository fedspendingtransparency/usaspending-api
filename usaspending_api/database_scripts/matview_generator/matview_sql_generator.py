import json
import os
import sys


TEMPLATE = {
    'create_matview': 'CREATE MATERIALIZED VIEW {} AS\n{};',
    'drop_matview': 'DROP MATERIALIZED VIEW IF EXISTS {};',
    'rename_matview': 'ALTER MATERIALIZED VIEW {}{} RENAME TO {};',
    'create_index': 'CREATE INDEX {} ON {} USING {}({} COLLATE "{}");',
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


def ingest_json(path):
    with open(path) as f:
        doc = json.load(f)
    return doc


def create_sql_strings(sql_json):
    ''' Steps:
        1. Drop temp-named matviews
        2. Create matview
        3. Create temp indexes
        4. Rename existing matview, append _old
        5. Rename all existing matview indexes
        6. Rename new matview
        7. Rename new matview indexes
    '''
    final_sql_strings = []
    create_indexes = []
    rename_old_indexes = []
    rename_new_indexes = []

    matview_name = sql_json['final_name']
    matview_temp_name = matview_name + '_temp'
    matview_archive_name = matview_name + '_old'

    final_sql_strings.append('\n'.join(HEADER))
    final_sql_strings.append(TEMPLATE['drop_matview'].format(matview_temp_name))
    final_sql_strings.append(TEMPLATE['drop_matview'].format(matview_archive_name))
    final_sql_strings.append('')
    final_sql_strings.append(TEMPLATE['create_matview'].format(matview_temp_name, '\n'.join(sql_json['matview_sql'])))

    for idx in sql_json['indexes']:
        final_index = matview_name + '_' + idx['name']
        tmp_index = final_index + '_temp'
        old_index = final_index + '_old'

        idx_type = 'BTREE'
        idx_collate = 'default'
        if 'collate' in idx:
            idx_collate = idx['collate']
        if 'type' in idx:
            idx_type = idx['type']
        idx_str = TEMPLATE['create_index'].format(tmp_index, matview_temp_name, idx_type, idx['columns'], idx_collate)
        if 'where' in idx:
            idx_str = idx_str[:-1] + ' WHERE {};'.format(idx['where'])
        create_indexes.append(idx_str)
        rename_old_indexes.append(TEMPLATE['rename_index'].format('IF EXISTS ', final_index, old_index))
        rename_new_indexes.append(TEMPLATE['rename_index'].format('', tmp_index, final_index))

    final_sql_strings.append('')
    final_sql_strings += create_indexes
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

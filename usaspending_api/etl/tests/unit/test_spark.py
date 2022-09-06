from usaspending_api.etl.management.commands.load_query_to_delta import TABLE_SPEC as LOAD_QUERY_TABLE_SPEC
from usaspending_api.etl.management.commands.load_table_to_delta import TABLE_SPEC as LOAD_TABLE_TABLE_SPEC


def test_table_spec_consistency():
    table_spec_config_groups = {
        "LOAD_QUERY_TABLE_SPEC": LOAD_QUERY_TABLE_SPEC,
        "LOAD_TABLE_TABLE_SPEC": LOAD_TABLE_TABLE_SPEC,
    }
    for table_spec_group_name, table_spec_config_group in table_spec_config_groups.items():
        unioned_table_spec_keys = set()
        for table_name, config in table_spec_config_group.items():
            unioned_table_spec_keys = unioned_table_spec_keys.union(set(list(config.keys())))
        for table_name, config in table_spec_config_group.items():
            diff = unioned_table_spec_keys - set(list(config.keys()))
            if diff:
                raise Exception(f"{table_name} is missing the following {table_spec_group_name} values: {diff}")

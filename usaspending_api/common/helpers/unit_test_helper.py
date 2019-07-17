from django.core.exceptions import FieldError
from usaspending_api.download.v2 import download_column_historical_lookups as lookup_mapping
from usaspending_api.download.lookups import VALUE_MAPPINGS


def add_to_mock_objects(mock_obj, mock_models_list):
    for mock_model in mock_models_list:
        mock_obj.add(mock_model)


def mappings_test(download_type, sublevel):
    download_mapping = VALUE_MAPPINGS[download_type]
    table_name = download_mapping["table_name"]
    table = download_mapping["table"]

    try:
        query_values = lookup_mapping.query_paths[table_name][sublevel].values()
        table.objects.values(*query_values)
    except FieldError:
        return False
    return True

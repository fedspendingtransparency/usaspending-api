from django.core.exceptions import FieldError
from usaspending_api.download.v2 import download_column_historical_lookups as lookup_mapping
from usaspending_api.download.lookups import VALUE_MAPPINGS


def mappings_test(download_type, sublevel):
    download_mapping = VALUE_MAPPINGS[download_type]
    table_name = download_mapping["table_name"]
    table = download_mapping["table"]
    annotations_function = download_mapping.get("annotations_function")

    try:
        if annotations_function is not None:
            query_values = [
                value for value in lookup_mapping.query_paths[table_name][sublevel].values() if value is not None
            ]
            annotations = annotations_function({})
        else:
            query_values = [value for value in lookup_mapping.query_paths[table_name][sublevel].values()]
            annotations = {}
        table.objects.values(*query_values, **annotations)
    except FieldError:
        return False
    return True

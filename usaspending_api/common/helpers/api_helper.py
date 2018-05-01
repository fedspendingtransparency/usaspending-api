def alias_response(field_to_alias_dict, results):
    results_copy = results.copy()
    for result in results_copy:
        for field in field_to_alias_dict:
            if field in result:
                value = result[field]
                del result[field]
                result[field_to_alias_dict[field]] = value

    return results_copy
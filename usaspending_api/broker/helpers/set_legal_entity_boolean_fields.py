from usaspending_api.broker.helpers.build_legal_entity_booleans_dict import build_legal_entity_booleans_dict


def set_legal_entity_boolean_fields(row):
    """ in place updates to specific fields to be mapped as booleans """
    legal_entity_bool_dict = build_legal_entity_booleans_dict(row)
    for key in legal_entity_bool_dict:
        row[key] = legal_entity_bool_dict[key]

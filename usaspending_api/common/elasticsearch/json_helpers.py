import json


def json_str_to_dict(string: str) -> dict:
    if not str:
        return {}

    try:
        return json.loads(string)
    except json.decoder.JSONDecodeError:
        pass  # Give the unicode_escape a chance to succeed

    try:
        return json.loads(string.encode("unicode_escape"))
    except json.decoder.JSONDecodeError as e:
        raise e

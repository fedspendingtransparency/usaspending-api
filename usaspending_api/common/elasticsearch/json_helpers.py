import json
import re


def json_str_to_dict(string: str) -> dict:
    if not str:
        return {}

    try:
        return json.loads(string)
    except json.decoder.JSONDecodeError:
        pass  # Give the unicode_escape a chance to succeed

    try:
        return json.loads(string.encode("unicode_escape"))
    except json.decoder.JSONDecodeError:
        pass  # One more attempt to Decode

    try:
        regex = r"\"([^\"]*)\"\s?:\s?\"([^\"]*(?:(?:\w|\s)?(?:\"|\')?(?:\w|\s)?)*[^\"]*)(?:(?:\"\,)|(?:\"\}))"
        matches = re.findall(regex, string)
        return {key: value for key, value in matches}
    except json.decoder.JSONDecodeError as e:
        raise e

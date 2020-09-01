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
    except json.decoder.JSONDecodeError as e:

        # Try to parse the string with Regex before throwing error
        key_count_regex = r"\"[^\"]*\"\s?:"
        grouping_regex = r"\"([^\"]*)\"\s?:\s?\"([^\"]*(?:(?:\w|\s)?(?:\"|\')?(?:\w|\s)?)*[^\"]*)(?:(?:\"\,)|(?:\"\}))"

        key_count_matches = re.findall(key_count_regex, string)
        grouping_matches = re.findall(grouping_regex, string)

        # Need to verify the correct number of elements in case grouping regex didn't work
        if (
            isinstance(key_count_matches, list)
            and isinstance(grouping_matches, list)
            and len(key_count_matches) == len(grouping_matches)
        ):
            return {key: value for key, value in grouping_matches}
        else:
            return e  # Return the JSONDecodeError since we couldn't salvage with regex

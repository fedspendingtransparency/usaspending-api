import json
from datetime import datetime, timezone


class OpsReporter:
    """
    Object to load useful operational metrics during routine script executions
    When instantiating a new OpsReporter class, metrics can be loaded as keyword args
    Later, additional data can be added just like adding/updating values in a dictionary.
    At the end of the script, send the JSON to the intended destination.
    """

    required_keys = ("job_name", "duration", "iso_start_datetime", "end_status")

    def __init__(self, **kwargs):
        self._internal_dict = {}
        if kwargs:
            self._internal_dict = kwargs

        if "iso_start_datetime" not in self._internal_dict:
            self._internal_dict["iso_start_datetime"] = datetime.now(timezone.utc).isoformat()

    def __getattr__(self, key):
        try:
            return self._internal_dict[key]
        except KeyError:
            raise AttributeError(key)

    def __getitem__(self, key):
        try:
            return self._internal_dict[key]
        except KeyError:
            raise AttributeError(key)

    def __setitem__(self, key, value):
        self._internal_dict[key] = value

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return "OpsReporter({})".format(", ".join(["{}:{}".format(k, v) for k, v in self._internal_dict.items()]))

    def json_dump(self):
        self._verify_required_keys()
        return json.dumps(self._internal_dict)

    def dump_to_file(self, filename):
        with open(filename, "w+") as f:
            json.dump(self._internal_dict, f)

    def _verify_required_keys(self):
        missing_required_keys = set(self.required_keys) - set(self._internal_dict.keys())
        if missing_required_keys:
            raise Exception("Missing required keys: {}".format(missing_required_keys))

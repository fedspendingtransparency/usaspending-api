import json


class OpsReporter:
    required_files = ("job_name", "duration", "iso_start_datetime", "end_status")

    def __init__(self, **kwargs):
        self.internal_dict = {}
        if kwargs:
            self.internal_dict = kwargs

    def __getattr__(self, key):
        try:
            return self.internal_dict[key]
        except KeyError:
            raise AttributeError(key)

    def __getitem__(self, key):
        try:
            return self.internal_dict[key]
        except KeyError:
            raise AttributeError(key)

    def __setitem__(self, key, value):
        self.internal_dict[key] = value

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return ",".join(["{}:{}".format(k, v) for k, v in self.internal_dict.items()])

    def json_dump(self):
        self._verify_required_fields()
        return json.dumps(self.internal_dict)

    def _verify_required_fields(self):
        for field in self.required_files:
            self.internal_dict[field]

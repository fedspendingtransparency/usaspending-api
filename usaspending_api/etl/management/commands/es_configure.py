import json
import subprocess

from django.core.management.base import BaseCommand
from time import perf_counter

from usaspending_api import settings
from usaspending_api.etl.es_etl_helpers import VIEW_COLUMNS

CURL_STATEMENT = 'curl -XPUT "{url}" -H "Content-Type: application/json" -d \'{data}\''

CURL_COMMANDS = {
    "template": "{host}/_template/{name}?pretty",
    "cluster": "{host}/_cluster/settings?pretty",
    "settings": "{host}/_settings?pretty",
}

FILES = {
    "template": settings.APP_DIR / "etl" / "es_transaction_template.json",
    "settings": settings.APP_DIR / "etl" / "es_settings.json",
}


class Command(BaseCommand):
    help = """
    This script applies configuration changes to an Elasticsearch cluster.
    Requires env var ES_HOSTNAME to be set
    """

    # used by parent class
    def handle(self, *args, **options):
        """ Script execution of custom code starts in this method"""
        start = perf_counter()
        if not settings.ES_HOSTNAME:
            print("$ES_HOSTNAME is not set! Abort Script")
            raise SystemExit

        cluster, index_settings = get_elasticsearch_settings()
        template = get_index_template()
        host = settings.ES_HOSTNAME

        run_curl_cmd(payload=cluster, url=CURL_COMMANDS["cluster"], host=host)
        run_curl_cmd(payload=index_settings, url=CURL_COMMANDS["settings"], host=host)
        run_curl_cmd(payload=template, url=CURL_COMMANDS["template"], host=host, name="transaction_template")
        print("Script completed in {} seconds".format(perf_counter() - start))


def run_curl_cmd(**kwargs):
    url = kwargs["url"].format(**kwargs)
    cmd = CURL_STATEMENT.format(url=url, data=json.dumps(kwargs["payload"]))
    print("Running: {}\n\n".format(cmd))

    subprocess.Popen(cmd, shell=True).wait()
    print("\n\n---------------------------------------------------------------")
    return


def get_elasticsearch_settings():
    es_config = return_json_from_file(FILES["settings"])
    es_config["settings"]["index.max_result_window"] = settings.ES_TRANSACTIONS_MAX_RESULT_WINDOW
    return es_config["cluster"], es_config["settings"]


def get_index_template():
    template = return_json_from_file(FILES["template"])
    template["index_patterns"] = ["*{}".format(settings.ES_TRANSACTIONS_NAME_PATTERN)]
    template["settings"]["index.max_result_window"] = settings.ES_TRANSACTIONS_MAX_RESULT_WINDOW
    validate_known_fields(template)
    return template


def return_json_from_file(path):
    """Read and parse file as JSON

    Library performs validation which is helpful before sending to ES
    """
    filepath = str(path)
    if not path.exists():
        raise SystemExit("File {} does not exist!!!!".format(filepath))

    print("Reading file: {}".format(filepath))
    with open(filepath, "r") as f:
        json_to_dict = json.load(f)

    return json_to_dict


def validate_known_fields(template):
    defined_fields = set([field for field in template["mappings"]["transaction_mapping"]["properties"]])
    load_columns = set(VIEW_COLUMNS)
    if defined_fields ^ load_columns:  # check if any fields are not in both sets
        raise RuntimeError("Mismatch between template and fields in ETL! Resolve before continuing!")


def retrieve_transaction_index_template():
    """This function is used for test configuration"""
    with open(str(FILES["template"])) as f:
        mapping_dict = json.load(f)
        template = json.dumps(mapping_dict)

    return template

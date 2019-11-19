import json
import subprocess

from django.core.management.base import BaseCommand

from usaspending_api import settings
from usaspending_api.etl.es_etl_helpers import TRANSACTION_VIEW_COLUMNS, AWARD_VIEW_COLUMNS

CURL_STATEMENT = 'curl -XPUT "{url}" -H "Content-Type: application/json" -d \'{data}\''

CURL_COMMANDS = {
    "template": "{host}/_template/{name}?pretty",
    "cluster": "{host}/_cluster/settings?pretty",
    "settings": "{host}/_settings?pretty",
}

FILES = {
    "transaction_template": settings.APP_DIR / "etl" / "es_transaction_template.json",
    "award_template": settings.APP_DIR / "etl" / "es_award_template.json",
    "settings": settings.APP_DIR / "etl" / "es_config_objects.json",
}


class Command(BaseCommand):
    help = """
    This script applies configuration changes to an Elasticsearch cluster.
    Requires env var ES_HOSTNAME to be set
    """

    def add_arguments(self, parser):
        parser.add_argument(
            "--awards", action="store_true", help="Load awards template instead of transaction template."
        )

        parser.add_argument(  # used by parent class
            "--template-only",
            action="store_true",
            help="When this flag is set, skip the cluster and index settings. Useful when creating a new index",
        )

    def handle(self, *args, **options):
        cluster, index_settings = get_elasticsearch_settings()
        awards = options["awards"]
        template = get_index_template(awards)

        if not options["template_only"]:
            run_curl_cmd(payload=cluster, url=CURL_COMMANDS["cluster"], host=settings.ES_HOSTNAME)
            run_curl_cmd(payload=index_settings, url=CURL_COMMANDS["settings"], host=settings.ES_HOSTNAME)

        if not awards:
            run_curl_cmd(
                payload=template, url=CURL_COMMANDS["template"], host=settings.ES_HOSTNAME, name="transaction_template"
            )
        else:
            run_curl_cmd(
                payload=template, url=CURL_COMMANDS["template"], host=settings.ES_HOSTNAME, name="award_template"
            )


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


def get_index_template(awards: bool):
    template = return_json_from_file(FILES["{}_template".format("award" if awards else "transaction")])
    template["index_patterns"] = [
        "*{}".format(settings.ES_AWARDS_NAME_SUFFIX if awards else settings.ES_TRANSACTIONS_NAME_SUFFIX)
    ]
    template["settings"]["index.max_result_window"] = (
        settings.ES_AWARDS_MAX_RESULT_WINDOW if awards else settings.ES_TRANSACTIONS_MAX_RESULT_WINDOW
    )
    validate_known_fields(template, awards)
    return template


def return_json_from_file(path):
    """Read and parse file as JSON

    Library performs JSON validation which is helpful before sending to ES
    """
    filepath = str(path)
    if not path.exists():
        raise SystemExit("Fatal error: file {} does not exist.".format(filepath))

    print("Reading file: {}".format(filepath))
    with open(filepath, "r") as f:
        json_to_dict = json.load(f)

    return json_to_dict


def validate_known_fields(template, awards):
    defined_fields = set(
        [field for field in template["mappings"]["award_mapping" if awards else "transaction_mapping"]["properties"]]
    )
    load_columns = set(AWARD_VIEW_COLUMNS) if awards else set(TRANSACTION_VIEW_COLUMNS)
    if defined_fields ^ load_columns:  # check if any fields are not in both sets
        raise RuntimeError(
            "Mismatch between template and fields in ETL! Resolve before continuing!\n {}".format(
                defined_fields.difference(load_columns) or load_columns.difference(defined_fields)
            )
        )


def retrieve_transaction_index_template(awards=False):
    """This function is used for test configuration"""
    with open(str(FILES["{}_template".format("award" if awards else "transaction")])) as f:
        mapping_dict = json.load(f)
        template = json.dumps(mapping_dict)

    return template

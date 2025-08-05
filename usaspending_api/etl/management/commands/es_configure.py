import json
import logging
import subprocess
from time import perf_counter

from django.conf import settings
from django.core.management.base import BaseCommand

logger = logging.getLogger("script")

CURL_STATEMENT = 'curl -XPUT "{url}" -H "Content-Type: application/json" -d \'{data}\''

CURL_COMMANDS = {
    "template": "{host}/_template/{name}?pretty",
    "cluster": "{host}/_cluster/settings?pretty",
}

FILES = {
    "award_template": settings.APP_DIR / "etl" / "es_award_template.json",
    "settings": settings.APP_DIR / "etl" / "es_config_objects.json",
    "transaction_template": settings.APP_DIR / "etl" / "es_transaction_template.json",
    "subaward_template": settings.APP_DIR / "etl" / "es_subaward_template.json",
    "recipient_profile_template": settings.APP_DIR / "etl" / "es_recipient_profile_template.json",
    "location_template": settings.APP_DIR / "etl" / "es_location_template.json",
}


class Command(BaseCommand):
    help = """
    This script applies configuration changes to an Elasticsearch cluster.
    Requires env var ES_HOSTNAME to be set
    """

    def add_arguments(self, parser):
        parser.add_argument(
            "--load-type",
            type=str,
            help="Select which type of index to configure, current options are awards or transactions",
            choices=[
                "transaction",
                "award",
                "covid19-faba",
                "transactions",
                "awards",
                "recipient",
                "location",
                "subaward",
            ],
            default="transaction",
        )
        parser.add_argument(
            "--template-only",
            action="store_true",
            help="When this flag is set, skip the cluster and index settings. Useful when creating a new index",
        )

    def handle(self, *args, **options):
        logger.info("Starting ES Configure")
        start = perf_counter()
        if not settings.ES_HOSTNAME:
            raise SystemExit("Fatal error: $ES_HOSTNAME is not set.")
        self.load_type = options["load_type"]
        if options["load_type"] in ("award", "awards"):
            self.index_pattern = f"*{settings.ES_AWARDS_NAME_SUFFIX}"
            self.max_result_window = settings.ES_AWARDS_MAX_RESULT_WINDOW
            self.template_name = "award_template"
        elif options["load_type"] in ("transaction", "transactions"):
            self.index_pattern = f"*{settings.ES_TRANSACTIONS_NAME_SUFFIX}"
            self.max_result_window = settings.ES_TRANSACTIONS_MAX_RESULT_WINDOW
            self.template_name = "transaction_template"
        elif options["load_type"] == "subaward":
            self.index_pattern = f"*{settings.ES_SUBAWARD_NAME_SUFFIX}"
            self.max_result_window = settings.ES_SUBAWARD_MAX_RESULT_WINDOW
            self.template_name = "subaward_template"
        elif options["load_type"] == "recipient":
            self.index_pattern = f"*{settings.ES_RECIPIENTS_NAME_SUFFIX}"
            self.max_result_window = settings.ES_RECIPIENTS_MAX_RESULT_WINDOW
            self.template_name = "recipient_profile_template"
        elif options["load_type"] == "location":
            self.index_pattern = f"*{settings.ES_LOCATIONS_NAME_SUFFIX}"
            self.max_result_window = settings.ES_LOCATIONS_MAX_RESULT_WINDOW
            self.template_name = "location_template"
        else:
            raise RuntimeError(f"No config for {options['load_type']}")

        cluster = self.get_elasticsearch_settings()
        template = self.get_index_template()

        if not options["template_only"] and cluster:
            self.run_curl_cmd(payload=cluster, url=CURL_COMMANDS["cluster"], host=settings.ES_HOSTNAME)

        self.run_curl_cmd(
            payload=template, url=CURL_COMMANDS["template"], host=settings.ES_HOSTNAME, name=self.template_name
        )

        logger.info(f"ES Configure took {perf_counter() - start:.2f}s")

    def run_curl_cmd(self, **kwargs) -> None:
        url = kwargs["url"].format(**kwargs)
        cmd = CURL_STATEMENT.format(url=url, data=json.dumps(kwargs["payload"]))

        try:
            subprocess.Popen(cmd, shell=True).wait()
        except Exception as e:
            logger.exception(f"Failed on command: {cmd}")
            raise e

    def get_elasticsearch_settings(self):
        es_config = self.return_json_from_file(FILES["settings"])
        return es_config["cluster"]

    def get_index_template(self):
        template = self.return_json_from_file(FILES[self.template_name])
        template["index_patterns"] = [self.index_pattern]
        template["settings"]["index.max_result_window"] = self.max_result_window
        return template

    def return_json_from_file(self, path):
        """Read and parse file as JSON

        Library performs JSON validation which is helpful before sending to ES
        """
        filepath = str(path)
        if not path.exists():
            raise SystemExit(f"Fatal error: file {filepath} does not exist.")

        logger.debug(f"Reading file: {filepath}")
        with open(filepath, "r") as f:
            json_to_dict = json.load(f)

        return json_to_dict

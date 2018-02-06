import os
import json
import subprocess

from django.core.management.base import BaseCommand
from time import perf_counter
from usaspending_api import settings


CURL_COMMANDS = {
    'template': '{es_host}/_template/{name}?pretty',
    'cluster': '{es_host}/_cluster/settings?pretty',
    'settings': '{es_host}/_settings?pretty',
}

FILES = {
    'template': '/usaspending_api/etl/es_transaction_template.json',
    'settings': '/usaspending_api/etl/es_settings.json',
}


class Command(BaseCommand):
    help = '''
    This script applies configuration changes to an Elasticsearch cluster.
    Requires env var ES_HOSTNAME to be set
    '''

    # used by parent class
    def handle(self, *args, **options):
        ''' Script execution of custom code starts in this method'''
        start = perf_counter()
        if not settings.ES_HOSTNAME:
            print('$ES_HOSTNAME is not set! Abort Script')
            raise SystemExit

        cluster, index_settings = get_elasticsearch_settings()
        template = create_template()
        host = settings.ES_HOSTNAME

        run_curl_cmd(payload=cluster, host=host, url=CURL_COMMANDS['cluster'])
        run_curl_cmd(payload=index_settings, host=host, url=CURL_COMMANDS['settings'])
        run_curl_cmd(payload=template, host=host, url=CURL_COMMANDS['template'], name="transaction_template")
        print("Script completed in {} seconds".format(perf_counter() - start))


def run_curl_cmd(payload, **kwargs):
    template = 'curl -XPUT "{host}{url}" -H "Content-Type: application/json" -d \'{data}\''
    cmd = template.format(**kwargs, data=json.dumps(payload))
    print('Running: {}\n\n'.format(cmd))

    subprocess.Popen(cmd, shell=True).wait()
    print('\n\n---------------------------------------------------------------')
    return


def get_elasticsearch_settings():
    filename = os.path.curdir + FILES['settings']
    if not os.path.isfile(filename):
        print('File {} does not exist!!!!'.format(filename))
        raise SystemExit

    print('Attemping to use {}'.format(filename))
    # Read and parse file as JSON validation before sending it to ES
    with open(filename, 'r') as f:
        es_config = json.load(f)
    return es_config['cluster'], es_config['settings']


def create_template():
    template_file = os.path.curdir + FILES['template']
    # Read and parse file as JSON validation before sending it to ES
    with open(template_file, 'r') as f:
        template = json.load(f)
    template['index_patterns'] = [settings.TRANSACTIONS_INDEX_ROOT + '*']
    return template

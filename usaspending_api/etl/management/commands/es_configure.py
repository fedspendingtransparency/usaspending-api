import os
import json
import subprocess

from django.core.management.base import BaseCommand
from time import perf_counter
from usaspending_api import settings


class Command(BaseCommand):
    help = '''
    This script applies configuration changes to an ElasticSearch cluster.
    Default mapper file location is in `usaspending_api/etl/es_settings.json`
    Requires env var ES_HOSTNAME to be set
    '''

    # used by parent class
    def add_arguments(self, parser):
        parser.add_argument(
            '--path',
            '-p',
            default=None,
            type=str,
            help='Point to custom location of elasticsearch json configuration file')

    # used by parent class
    def handle(self, *args, **options):
        ''' Script execution of custom code starts in this method'''
        start = perf_counter()
        if not settings.ES_HOSTNAME:
            print('$ES_HOSTNAME is not set! Abort Script')
            raise SystemExit

        # assumes script is run from repo project root
        settings_file = os.path.curdir + '/usaspending_api/etl/es_settings.json'

        if options['path']:
            settings_file = options['path']

        if not os.path.isfile(settings_file):
            print('File {} does not exist!!!!'.format(settings_file))
            raise SystemExit

        print('Attemping to use {}'.format(settings_file))

        with open(settings_file, 'r') as f:
            # Read and parse file as lite JSON validation before sending it to ES
            es_config = json.load(f)

        cmd = 'curl -XPUT "{es_host}/_settings" -H "Content-Type: application/json" -d \'{data}\''

        subprocess.Popen(cmd.format(es_host=settings.ES_HOSTNAME, data=json.dumps(es_config)), shell=True).wait()

        print('---------------------------------------------------------------')
        print("Script completed in {} seconds".format(perf_counter() - start))

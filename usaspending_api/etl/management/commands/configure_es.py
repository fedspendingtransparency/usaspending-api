import os
import json
import subprocess

from django.core.management.base import BaseCommand
from time import perf_counter
from usaspending_api import settings


class Command(BaseCommand):
    # used by parent class
    def add_arguments(self, parser):
        parser.add_argument(
            '--path',
            '-p',
            default=None,
            type=str,
            help='Point to custom location of elasticsearch configuration file')

    # used by parent class
    def handle(self, *args, **options):
        ''' Script execution of custom code starts in this method'''
        start = perf_counter()
        self.deleted_ids = {}

        # assumes script is run from repo project root
        settings_file = os.path.curdir + '/usaspending_api/etl/es_settings.json'

        if options['path']:
            settings_file = options['path']

        if not os.path.isfile(settings_file):
            print('File {} does not exist!!!!'.format(settings_file))
            raise SystemExit
        print('using {}'.format(settings_file))
        with open(settings_file, 'r') as f:
            es_config = json.load(f)

        cmd = 'curl -XPUT "{es_host}/_settings" -H "Content-Type: application/json" -d \'{data}\''

        subprocess.Popen(cmd.format(es_host=settings.ES_HOSTNAME, data=json.dumps(es_config)), shell=True).wait()

        print('---------------------------------------------------------------')
        print("Script completed in {} seconds".format(perf_counter() - start))

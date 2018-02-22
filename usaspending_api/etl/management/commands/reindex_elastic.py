import logging
from elasticsearch import Elasticsearch
import time
from usaspending_api import settings
from usaspending_api.awards.v2.lookups.elasticsearch_lookups import indices_to_award_types
from django.core.management.base import BaseCommand
import os
import json

logging.basicConfig(format='%(asctime)s |  %(message)s', level=logging.WARNING)
RETRIES = 3
INGEST_RATE = 296666/25.

ES_CLIENT = Elasticsearch(settings.ES_HOSTNAME, timeout=300)
INDICES_SUB_NAMES = indices_to_award_types.keys()


class Command(BaseCommand):
    # used by parent class
    def add_arguments(self, parser):
        parser.add_argument(
            'dest_prefix',
            type=str,
            help='Prefix of output index')
        parser.add_argument(
            'source_prefix',
            default=settings.TRANSACTIONS_INDEX_ROOT,
            type=str,
            help='Prefix of index')
        parser.add_argument(
            'mapping_file',
            default='../es_transaction_template.json',
            type=str,
            help='Mapping file')
        parser.add_argument(
            'alias_prefix',
            default='trans',
            type=str,
            help='Mapping file')

    def handle(self, *args, **options):
        ''' Script execution of custom code starts in this method'''
        set_config()
        self.source_prefix = options['source_prefix']
        self.dest_prefix = options['dest_prefix']
        self.alias_prefix = options['alias_prefix']
        #TODO: 
        self.wipe = False
        directory = os.path.dirname(os.path.abspath(__file__))
        mappingfile = os.path.join(directory, options['mapping_file'])
        with open(mappingfile) as f:
            data = json.load(f)
            self.mapping_file = json.dumps(data)

        self.indicies_dict()
        self.controller()

    def single_reindex(self, source, dest):
        logging.warn('calling create')
        ES_CLIENT.indices.create(dest, body=self.mapping_file)
        body_ = {
                "source": {
                        "index": source
                        },
                "dest": {
                        "index": dest
                        }
                }
        logging.warn('CREATING INDEX {}'.format(dest))
        ES_CLIENT.reindex(body=body_)

    def indicies_dict(self):
        index_name = self.source_prefix + '*'
        source_indices = ES_CLIENT.indices.get(index_name).keys()
        source_indices = sorted(source_indices, key=lambda x: x.split('-')[-1])

        dest_indices = [i.replace(self.source_prefix, self.dest_prefix) for i in source_indices]
        self.reindex_dict = dict(zip(source_indices, dest_indices))
        count = self.get_total_count(index_name)
        logging.warn('Reindexing a total of {} documents'.format(count))

    def get_total_count(self, index_name):
        try:
            response = ES_CLIENT.search(index=index_name)
            return response["hits"]["total"]
        except:
            return -1

    def verify(self, source, target):
        difference = self.get_total_count(source) - self.get_total_count(target)
        if difference != 0:
            logging.warn('Still have {} documents to reindex'.format(difference))
        return difference, difference/INGEST_RATE

    def controller(self):
        for source, dest in self.reindex_dict.items():
            if ES_CLIENT.indices.exists(dest):
                if not self.wipe:
                    logging.warn('{} exists... passing ... '.format(dest))
                else:
                    logging.warn('{} exists... deleting... {}'.format(dest))
                    # ES_CLIENT.indices.delete(dest)
            if not ES_CLIENT.indices.exists(dest):
                self.size, self.sleep_time = self.verify(source, dest)
                logging.warn('Reindexing {} -- {} documents'.format(source, self.size))
                try:
                    self.single_reindex(source, dest)
                    logging.info(source)
                except Exception as e:
                    logging.info('got exception')
                    for attempt in range(RETRIES):
                        self.size, self.sleep_time = self.verify(source, dest)
                        if self.sleep_time > 0:
                            logging.warn('---------- Sleep_time of index >>> {} ------------------'
                                         .format(str(self.sleep_time)))
                            time.sleep(self.sleep_time)
        print('\n\n')
        index_name = self.dest_prefix + '*'
        time.sleep(5)
        count = self.get_total_count(index_name)
        logging.warn('Reindexed a total of {} documents'.format(count))
        logging.warn('****   FINISHED REINDEXING  ****')

        for source, dest in self.reindex_dict.items():
            transaction_category = next((s for s in INDICES_SUB_NAMES if dest.find(s) != -1), None)
            alias_name = '{}-{}'.format(self.source_prefix, transaction_category)
            logging.warn('ALIAS NAME ----     {}'.format(alias_name))
            logging.warn('CLOSING -------     {}'.format(source))
            logging.warn('NEW INDEX  ----     {}'.format(dest))
            ES_CLIENT.indices.close(source)
            ES_CLIENT.indices.put_alias(dest, alias_name)
            print('\n')
        logging.warn('****   COMPLETE   ****')


def set_config():
    if not os.environ.get('ES_HOSTNAME'):
        print('Missing environment variable `ES_HOSTNAME`')
        raise SystemExit

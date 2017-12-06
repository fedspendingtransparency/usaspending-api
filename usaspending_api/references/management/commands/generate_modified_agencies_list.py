import pandas as pd
import logging
import os

import django
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = 'Takes in the authoritative_agencies_list.csv and the broker_agency_list.csv to make' \
           'a modified_authoritative_agencies_list.csv'

    logger = logging.getLogger('console')

    def add_arguments(self, parser):
        parser.add_argument(
            '--authoritative_agencies_list',
            dest='agencies_list',
            type=str,
            help='The original authoritative agencies list',
            default=os.path.join(django.conf.settings.BASE_DIR,
                                 'usaspending_api', 'data', 'authoritative_agency_list.csv')
        )
        parser.add_argument(
            '--broker_agency_list',
            dest='broker_agency_list',
            type=str,
            help='List that displays which entries from the agencies list are used in the broker',
            default=os.path.join(django.conf.settings.BASE_DIR,
                                 'usaspending_api', 'data', 'broker_agency_list.csv')
        )

    def csv_to_df(self, csv_path, skiprows=0):
        csv_df = None
        try:
            with open(csv_path, encoding='Latin-1') as csv_file:
                csv_df = pd.read_csv(csv_file, dtype=str, skiprows=skiprows)
        except IOError:
            self.logger.log('Could not open file: {}'.format(csv_path))
        return csv_df

    def handle(self, *args, **options):
        agencies_list_path = options.get('agencies_list')
        agencies_dir = os.path.dirname(agencies_list_path)
        broker_agency_list_path = options.get('broker_agency_list')

        # Get the authoritative agencies list
        agencies_list_df = self.csv_to_df(agencies_list_path)
        # Get the broker agencies list
        broker_agency_list_df = self.csv_to_df(broker_agency_list_path, skiprows=1)
        # Padding cgac to 3 chars for matching later on
        broker_agency_list_df['CGAC.AGENCY.CODE'] = broker_agency_list_df['CGAC.AGENCY.CODE']\
            .apply(lambda x: x.zfill(3))

        # Remove all the rows where awarding_agency_name is NA, awarding_sub_tier_agency_n is also NA
        broker_agency_list_df = broker_agency_list_df[pd.notnull(
            broker_agency_list_df['awarding_agency_name'])]
        # Remove other columns and duplicates
        broker_agency_list_df = broker_agency_list_df[['CGAC.AGENCY.CODE', 'SUBTIER.CODE']]\
            .drop_duplicates()
        # keep only the rows from agency list that have a cgac and subtier combo from broker_agency_list
        modified_agency_list = pd.merge(agencies_list_df, broker_agency_list_df,
                                        left_on=['CGAC AGENCY CODE', 'SUBTIER CODE'],
                                        right_on=['CGAC.AGENCY.CODE', 'SUBTIER.CODE'],
                                        how='inner', suffixes=('', '_y'))
        # Keep only the columns from the agencies list
        modified_agency_list = modified_agency_list[list(agencies_list_df.columns)]
        # Export to csv in the same directory as the authoritative_agencies_list.csv
        modified_agency_list.to_csv(os.path.join(agencies_dir,
                                                 'modified_authoritative_agency_list.csv'),
                                    mode='w', index=False)
        self.logger.info('Complete')

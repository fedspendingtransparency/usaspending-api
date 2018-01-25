from django.db import connection, transaction
from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned
from django.apps import apps
from usaspending_api.references.models import ToptierAgency, SubtierAgency, OfficeAgency, Agency
from usaspending_api.etl.helpers import pad_function
import os
import logging
import pandas as pd
import numpy as np


# TODO: Is this still needed?
MATVIEW_SQL = """
    DROP MATERIALIZED VIEW IF EXISTS agency_by_subtier_and_optionally_toptier;
    CREATE MATERIALIZED VIEW agency_by_subtier_and_optionally_toptier AS
    WITH subq AS (
        SELECT a.id,
               s.subtier_code,
               ''::TEXT AS cgac_code,
               rank() OVER (PARTITION BY s.subtier_agency_id ORDER BY s.update_date DESC) AS update_rank
        FROM   agency a
        JOIN   subtier_agency s ON (s.subtier_agency_id = a.subtier_agency_id) )
    SELECT subtier_code, cgac_code, id
    FROM   subq
    WHERE  update_rank = 1   -- forces only one row per subtier code
    UNION ALL
    SELECT
           s.subtier_code,
           t.cgac_code,
           a.id
    FROM   agency a
    JOIN   subtier_agency s ON (s.subtier_agency_id = a.subtier_agency_id)
    JOIN   toptier_agency t ON (t.toptier_agency_id = a.toptier_agency_id);
    CREATE INDEX ON agency_by_subtier_and_optionally_toptier (subtier_code, cgac_code);
    """


class Command(BaseCommand):
    help = "Loads agencies and sub-tier agencies from authoritative OMB list in \
            the folder of this management command."

    logger = logging.getLogger('console')

    # Holds subtiers with missing toptier links that we were unable to save
    unsaved_subtiers = {}

    @transaction.atomic
    def handle(self, *args, **options):

        """
            The toptier table is expected to contain unique CGACs or FRECs, based on the
            name specified in the CSV. The names must match for this to consider it a toptier agency.

            Every normalized "Agency" entry will point to a "ToptierAgency (CGAC/FREC) and a SubtierAgency (FPDS, or
            name only) entry.

            Toptier normalized "Agency" entries also have their FPDS and other info in a SubtierAgency entry.

            NB:
                Air Force / Army / Navy are toptiers, not subtiers of DoD, in this implementation of the agency list.

                Any agencies with unknown subtiers are ignored.

                "U.S. Congress" (000) and "Other (Listed Under Department of State)" (067) are also skipped.

        """
        self.logger.info('Beginning to load agencies')
        with open(os.path.join(settings.BASE_DIR,
                  'usaspending_api', 'data', 'agency_codes_list.csv'), encoding="Latin-1") \
                as agency_list:

            broker_agency_df = pd.read_csv(agency_list, dtype=str)

        # Cleaning CSV data
        # Triming Data
        broker_agency_df = broker_agency_df.applymap(lambda x: self.trim_item(x) if len(str(x).strip()) else None)
        # Arguments for pad_function are (padding length, keep null)
        broker_agency_df['CGAC AGENCY CODE'] = broker_agency_df['CGAC AGENCY CODE'].apply(pad_function, args=(3, False))
        broker_agency_df['SUBTIER CODE'] = broker_agency_df['SUBTIER CODE'].apply(pad_function, args=(4, False))
        broker_agency_df['FREC'] = broker_agency_df['FREC'].apply(pad_function, args=(4, False))
        broker_agency_df = broker_agency_df.fillna('')

        for _, row in broker_agency_df.iterrows():
            fpds_code = row.get('FPDS DEPARTMENT ID', '')
            cgac_code = row.get('CGAC AGENCY CODE', '')
            frec_code = row.get('FREC', '')
            department_name = row.get('AGENCY NAME', '')
            department_abbr = row.get('AGENCY ABBREVIATION', '')
            subtier_name = row.get('SUBTIER NAME', '')
            subtier_code = row.get('SUBTIER CODE', '')
            subtier_abbr = row.get('SUBTIER ABBREVIATION', '')
            frec_entity_description = row.get('FREC Entity Description', '')
            mission = row.get('MISSION', '')
            website = row.get('WEBSITE', '')
            icon_filename = row.get('ICON FILENAME', '')
            is_frec = row.get('IS FREC', 'FALSE')

            # Skip these agencies altogether
            if 'unknown' in subtier_code.lower() or cgac_code in ['000', '067']:
                continue

            # This toptier_flag comparison determines what we consider a toptier
            # toptier_code is used to create subtier agency and logging purposes
            if is_frec.upper() == 'TRUE':
                toptier_flag = (subtier_name.upper() == frec_entity_description.upper())
                toptier_code = frec_code
            else:
                toptier_flag = (subtier_name.upper() == department_name.upper())
                toptier_code = cgac_code

            if toptier_flag:  # create or update the toptier agency

                toptier_agency, created = ToptierAgency.objects.get_or_create(cgac_code=toptier_code)
                toptier_agency.name = subtier_name  # based on matching above
                toptier_agency.abbreviation = subtier_abbr if is_frec == 'True' else department_abbr
                toptier_agency.fpds_code = fpds_code
                toptier_agency.mission = mission
                toptier_agency.website = website
                toptier_agency.icon_filename = icon_filename

                toptier_agency.save()

                self.check_unsaved_subtiers(toptier_code)

            # Navy / Army / Air Force will just be a toptier, skip subtier
            if 'subsumed under dod' in subtier_code.lower():
                toptier_agency, created = ToptierAgency.objects.get_or_create(name=department_name)
                toptier_agency.cgac_code = cgac_code
                toptier_agency.abbreviation = department_abbr
                toptier_agency.save()

                agency, created = Agency.objects.get_or_create(toptier_agency=toptier_agency,
                                                               subtier_agency=None,
                                                               toptier_flag=True)
                agency.save()

                continue

            toptier_agency, subtier_agency = self.create_subtier(subtier_name, subtier_code, subtier_abbr, toptier_code)

            if toptier_agency and subtier_agency:
                agency, created = Agency.objects.get_or_create(toptier_agency=toptier_agency,
                                                               subtier_agency=subtier_agency,
                                                               toptier_flag=toptier_flag)
                agency.save()

        with connection.cursor() as cursor:
            cursor.execute(MATVIEW_SQL)

        self.logger.info('Loading Agencies Completed')

    def create_subtier(self, subtier_name, subtier_code, subtier_abbr, toptier_code):
        """
        Creates a subtier entry that is linked to a toptier. If unable to find a toptier, the function will return none
        """
        # Still need to grab the toptier for mapping
        try:
            toptier_agency = ToptierAgency.objects.get(cgac_code=toptier_code)
        except ObjectDoesNotExist:
            # Cannot get Toptier agency because it does not exist
            self.logger.warning('Could not find toptier agency for {} TOPTIER code, {} SUBTIER {}'.format(
                toptier_code,
                subtier_code,
                subtier_name
            ))

            subtier_dict = {'subtier_name': subtier_name,
                            'subtier_code': subtier_code,
                            'subtier_abbr': subtier_abbr}

            try:
                self.unsaved_subtiers[toptier_code].append(subtier_dict)

            except KeyError:
                self.unsaved_subtiers[toptier_code] = [subtier_dict]

            return None, None

        subtier_agency, created = SubtierAgency.objects.get_or_create(subtier_code=subtier_code)
        subtier_agency.name = subtier_name
        subtier_agency.abbreviation = subtier_abbr
        subtier_agency.save()

        return toptier_agency, subtier_agency

    def check_unsaved_subtiers(self, toptier_code):
        # Check that toptier is not missing a subtier
        if self.unsaved_subtiers.get(toptier_code):

            for subtier in self.unsaved_subtiers[toptier_code]:
                # Create subtier and save
                self.create_subtier(subtier['subtier_name'], subtier['subtier_code'],
                                    subtier['subtier_abbr'], toptier_code)

                self.logger.info('Saving unmatched subtier code {} toptier code {}'.format(
                    subtier['subtier_code'], toptier_code)
                )

                del self.unsaved_subtiers[toptier_code]

    def trim_item(self,item):
        if type(item) == np.str:
            return item.strip()
        return item

from django.db import connection
from django.core.management.base import BaseCommand
from usaspending_api.references.models import ToptierAgency, SubtierAgency, Agency
import os
import csv
import logging
import django

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

    def handle(self, *args, **options):

        '''
            The toptier table is expected to contain unique CGACs or FRECs, based on the
            name specified in the CSV. The names must match for this to consider it a toptier agency.

            The ordering of the CSV important because the toptier must exist before any
            other subtiers can be inserted.

            Every normalized "Agency" entry will point to a "ToptierAgency (CGAC/FREC) and a SubtierAgency (FPDS, or
            name only) entry.

            Toptier normalized "Agency" entries also have their FPDS and other info in a SubtierAgency entry.

            NB:
                Air Force / Army / Navy are toptiers, not subtiers of DoD, in this implementation of the agency list.

                Any agencies with unknown subtiers are ignored.

                "U.S. Congress" (000) and "Other (Listed Under Department of State)" (000) are also skipped.

        '''

        try:
            with open(os.path.join(django.conf.settings.BASE_DIR,
                      'usaspending_api', 'data', 'authoritative_agency_list.csv'), encoding="Latin-1") \
                    as csvfile:

                reader = csv.DictReader(csvfile)

                for row in reader:
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
                    is_frec = row.get('IS_FREC', 'FALSE')

                    # Skip these agencies altogether
                    if 'unknown' in subtier_code.lower() or cgac_code in ['000', '067']:
                        continue

                    toptier_agency = None
                    subtier_agency = None
                    toptier_flag = False

                    # This comparison determines what we consider a toptier
                    if is_frec == 'TRUE':
                        toptier_flag = (subtier_name == frec_entity_description)
                    else:
                        toptier_flag = (subtier_name == department_name)

                    if toptier_flag:  # create or update the toptier agency
                        toptier_name = subtier_name  # based on matching above
                        toptier_agency, created = ToptierAgency.objects.get_or_create(name=toptier_name)

                        if is_frec == 'TRUE':
                            toptier_agency.cgac_code = frec_code
                            toptier_agency.abbreviation = subtier_abbr
                        else:
                            toptier_agency.cgac_code = cgac_code
                            toptier_agency.abbreviation = department_abbr

                        toptier_agency.fpds_code = fpds_code
                        toptier_agency.mission = mission
                        toptier_agency.website = website
                        toptier_agency.icon_filename = icon_filename

                        if is_frec == 'TRUE':
                            toptier_agency.cgac_code = frec_code
                            toptier_agency.abbreviation = subtier_abbr
                        else:
                            toptier_agency.cgac_code = cgac_code
                            toptier_agency.abbreviation = department_abbr

                        toptier_agency.save()

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

                    # Still need to grab the toptier for mapping
                    if is_frec == 'TRUE':
                        toptier_agency = ToptierAgency.objects.get(cgac_code=frec_code)
                    else:
                        toptier_agency = ToptierAgency.objects.get(cgac_code=cgac_code)

                    # Sanity check
                    assert(subtier_code.isalnum() and len(subtier_code) == 4)

                    subtier_agency, created = SubtierAgency.objects.get_or_create(subtier_code=subtier_code)
                    subtier_agency.name = subtier_name
                    subtier_agency.abbreviation = subtier_abbr
                    subtier_agency.save()

                    agency, created = Agency.objects.get_or_create(toptier_agency=toptier_agency,
                                                                   subtier_agency=subtier_agency,
                                                                   toptier_flag=toptier_flag)
                    agency.save()

                with connection.cursor() as cursor:
                    cursor.execute(MATVIEW_SQL)

        except IOError:
            self.logger.log("Could not open file to load from")

from django.db import connection
from django.core.management.base import BaseCommand, CommandError
from usaspending_api.references.models import ToptierAgency, SubtierAgency, OfficeAgency, Agency
import os
import csv
import logging
import django

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

        try:
            with open(os.path.join(django.conf.settings.BASE_DIR,
                      'usaspending_api', 'data', 'authoritative_agency_list.csv'), encoding="Latin-1") \
                    as csvfile:

                reader = csv.DictReader(csvfile)
                counter = 0
                for row in reader:
                    fpds_code = row.get('FPDS DEPARTMENT ID', '')
                    cgac_code = row.get('CGAC AGENCY CODE', '')
                    frec_code = row.get('FREC', '')
                    department_name = row.get('AGENCY NAME', '')
                    department_abbr = row.get('AGENCY ABBREVIATION', '')
                    subtier_name = row.get('SUBTIER NAME', '')
                    subtier_code = row.get('SUBTIER CODE', '')
                    subtier_abbr = row.get('SUBTIER ABBREVIATION', '')
                    mission = row.get('MISSION', '')
                    website = row.get('WEBSITE', '')
                    icon_filename = row.get('ICON FILENAME', '')

                    toptier_agency = None
                    subtier_agency = None
                    toptier_flag = False

                    # Toptier agency
                    # First, see if we have a toptier agency that matches our fpds and cgac codes
                    # We use only these codes here to make sure we are idempotent with previous
                    # versions of this agency loader
                    if frec_code:
                        toptier_flag = True
                        toptier_agency, created = ToptierAgency.objects.get_or_create(cgac_code=frec_code, fpds_code=fpds_code)
                        toptier_agency.name = subtier_name
                        toptier_agency.abbreviation = subtier_abbr
                    else:
                        toptier_flag = (subtier_name == department_name)
                        toptier_agency, created = ToptierAgency.objects.get_or_create(cgac_code=cgac_code, fpds_code=fpds_code)
                        # Set or update the department name and abbreviation
                        toptier_agency.name = department_name
                        toptier_agency.abbreviation = department_abbr
                    if toptier_flag:
                        toptier_agency.mission = mission
                        toptier_agency.website = website
                        toptier_agency.icon_filename = icon_filename

                    toptier_agency.save()

                    # Do the same with the subtier that exists, but only on subtier code
                    # Only do this if subtier_code is not "Unknown"
                    if "unknown" not in subtier_code.lower():
                        subtier_agency, created = SubtierAgency.objects.get_or_create(subtier_code=subtier_code)
                        subtier_agency.name = subtier_name
                        subtier_agency.abbreviation = subtier_abbr
                        subtier_agency.save()

                    # Create new summary agency object
                    # Top tier flag is set after to insure variable is idempotent.
                    agency, created = Agency.objects.get_or_create(toptier_agency=toptier_agency,
                                                                   subtier_agency=subtier_agency)
                    agency.toptier_flag = toptier_flag
                    agency.save()

                with connection.cursor() as cursor:
                    cursor.execute(MATVIEW_SQL)

        except IOError:
            self.logger.log("Could not open file to load from")

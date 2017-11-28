from django.core.management.base import BaseCommand, CommandError
from usaspending_api.references.models import Cfda
from datetime import datetime
import os
import csv
import logging
import django
import ftplib

logger = logging.getLogger('console')
cfda_relative_path = '/usaspending_api/references/management/commands/programs-full-usaspending.csv'
cfda_abs_path = os.path.join(django.conf.settings.BASE_DIR + cfda_relative_path)


class Command(BaseCommand):

    help = "Loads program information obtained from csv file on ftp.cfda.gov"

    def add_arguments(self, parser):
        parser.add_argument(
            '--update-file',
            action='store_true',
            dest='update-file',
            default=False,
            help='Pull the updated CSV from ftp.cfda.gov before running.'
        )

    def handle(self, *args, **options):

        if options['update-file']:

            ftp_conn = ftplib.FTP()
            ftp_conn.connect("ftp.cfda.gov")
            ftp_conn.login()
            ftp_conn.cwd('usaspending')
            most_recent = sorted(ftp_conn.nlst(), reverse=True)[0]

            logger.info('Updating {} with new file {}...'.format(cfda_abs_path, most_recent))
            ftp_conn.retrbinary("RETR " + most_recent, open(cfda_abs_path, 'wb').write)

        load_cfda()


def load_cfda():
    """
    Update from ftp.cfda.gov and then create/update references_cfda table
    """
    try:
        print(cfda_abs_path)
        with open(cfda_abs_path, errors='backslashreplace') as csvfile:

            reader = csv.DictReader(csvfile, delimiter=',', quotechar='"', skipinitialspace='true')

            rows = list(reader)
            total_rows = len(rows)

            for index, row in enumerate(rows, 1):

                if not (index % 100):
                    logger.info('CFDA loading/updating row {} of {}'.format(index, total_rows))

                cfda_program, created = Cfda.objects.get_or_create(
                                program_number=row['Program Number'])

                cfda_program.data_source = "USA"
                cfda_program.program_title = row['Program Title']
                cfda_program.popular_name = row['Popular Name (020)']
                cfda_program.federal_agency = row['Federal Agency (030)']
                cfda_program.authorization = row['Authorization (040)']
                cfda_program.objectives = row['Objectives (050)']
                cfda_program.types_of_assistance = row['Types of Assistance (060)']
                cfda_program.uses_and_use_restrictions = row['Uses and Use Restrictions (070)']
                cfda_program.applicant_eligibility = row['Applicant Eligibility (081)']
                cfda_program.beneficiary_eligibility = row['Beneficiary Eligibility (082)']
                cfda_program.credentials_documentation = row['Credentials/Documentation (083)']
                cfda_program.pre_application_coordination = row['Preapplication Coordination (091)']
                cfda_program.application_procedures = row['Application Procedures (092)']
                cfda_program.award_procedure = row['Award Procedure (093)']
                cfda_program.deadlines = row['Deadlines (094)']
                cfda_program.range_of_approval_disapproval_time = row['Range of Approval/Disapproval Time (095)']
                cfda_program.appeals = row['Appeals (096)']
                cfda_program.renewals = row['Renewals (097)']
                cfda_program.formula_and_matching_requirements = row['Formula and Matching Requirements (101)']
                cfda_program.length_and_time_phasing_of_assistance = row['Length and Time Phasing of Assistance (102)']
                cfda_program.reports = row['Reports (111)']
                cfda_program.audits = row['Audits (112)']
                cfda_program.records = row['Records (113)']
                cfda_program.account_identification = row['Account Identification (121)']
                cfda_program.obligations = row['Obligations (122)']
                cfda_program.range_and_average_of_financial_assistance = row['Range and Average of Financial Assistance (123)']
                cfda_program.program_accomplishments = row['Program Accomplishments (130)']
                cfda_program.regulations_guidelines_and_literature = row['Regulations, Guidelines, and Literature (140)']
                cfda_program.regional_or_local_office = row['Regional or Local Office (151) ']
                cfda_program.headquarters_office = row['Headquarters Office (152)']
                cfda_program.website_address = row['Website Address (153)']
                cfda_program.related_programs = row['Related Programs (160)']
                cfda_program.examples_of_funded_projects = row['Examples of Funded Projects (170)']
                cfda_program.criteria_for_selecting_proposals = row['Criteria for Selecting Proposals (180)']
                cfda_program.url = row['URL']
                cfda_program.recovery = row['Recovery']
                cfda_program.omb_agency_code = row['OMB Agency Code']
                cfda_program.omb_bureau_code = row['OMB Bureau Code']
                if row['Published Date']:
                    cfda_program.published_date = datetime.strptime(row['Published Date'], '%b, %d %Y')
                if row['Archived Date']:
                    cfda_program.archived_date = datetime.strptime(row['Archived Date'], '%b, %d %Y')

                # TODO: add way to check/print out any cfda codes that got updated (not just created)
                if created:
                    logger.info('Created a new cfda code for {} ({})'.format(row['Program Number'],
                                                                             row['Program Title']))

                cfda_program.save()

    except IOError:
        logger.info("Could not open file to load from")

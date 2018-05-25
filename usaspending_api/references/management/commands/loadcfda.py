import os
import csv
import logging
from datetime import datetime, timedelta

import boto3
from botocore.handlers import disable_signing
from botocore.exceptions import ClientError

from django.conf import settings
from django.core.management.base import BaseCommand
from usaspending_api.references.models import Cfda

logger = logging.getLogger('console')
cfda_relative_path = '/usaspending_api/references/management/commands/programs-full-usaspending.csv'
cfda_abs_path = os.path.join(settings.BASE_DIR + cfda_relative_path)

CFDA_FILE_FORMAT = settings.CFDA_FILE_PATH
WEEKDAY_UPLOADED = 5  # datetime.weekday()'s integer representing the day it's usually uploaded (Saturday)
DAYS_TO_SEARCH = 4 * 7  # 4 weeks


class Command(BaseCommand):

    help = 'Loads program information obtained from csv file on ftp.cfda.gov'

    def find_latest_file(self, bucket, days_to_search=DAYS_TO_SEARCH):
        # TODO: If the bucket is public, simply use the folder structure to find the latest file instead of guessing
        # Check for the latest Saturday upload, otherwise manually look it up
        today = datetime.today()
        if today.weekday() == WEEKDAY_UPLOADED:
            logger.info('Checking today\'s entry')
            latest_file = today.strftime(CFDA_FILE_FORMAT)
            if self.file_exists(bucket, latest_file):
                return latest_file

        logger.info('Checking last week\'s entry')
        last_week = today - timedelta(7 - abs(today.weekday() - WEEKDAY_UPLOADED))
        latest_file = last_week.strftime(CFDA_FILE_FORMAT)
        if self.file_exists(bucket, latest_file):
            return latest_file
        else:
            logger.info('Looking within the past {} days'.format(days_to_search))
            try_date = today
            while days_to_search > 0:
                latest_file = try_date.strftime(CFDA_FILE_FORMAT)
                if not self.file_exists(bucket, latest_file):
                    try_date = try_date - timedelta(1)
                    days_to_search -= 1
                else:
                    break
            if days_to_search == 0:
                logger.error('Could not find cfda file within the past {} days.'.format(days_to_search))
                return None
            return latest_file

    def file_exists(self, bucket, src):
        try:
            bucket.Object(src).load()
            return True
        except ClientError:
            return False

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
            gsa_connection = boto3.resource('s3', region_name=settings.CFDA_REGION)
            # disregard aws credentials for public file
            gsa_connection.meta.client.meta.events.register('choose-signer.s3.*', disable_signing)
            gsa_bucket = gsa_connection.Bucket(settings.CFDA_BUCKET_NAME)

            latest_file = self.find_latest_file(gsa_bucket)
            if not latest_file:
                logger.error('Could not find cfda file. Local not updated.')
            else:
                logger.info('Updating {} with new file {}...'.format(cfda_abs_path, latest_file))
                gsa_bucket.download_file(latest_file, cfda_abs_path)

        load_cfda(cfda_abs_path)


def load_cfda(abs_path):
    """
    Using file at abs_path, update/create CFDA objects.
    """
    try:

        with open(abs_path, errors='backslashreplace') as csvfile:

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
                cfda_program.range_and_average_of_financial_assistance = row['Range and Average of '
                                                                             'Financial Assistance (123)']
                cfda_program.program_accomplishments = row['Program Accomplishments (130)']
                cfda_program.regulations_guidelines_and_literature = row['Regulations, Guidelines, '
                                                                         'and Literature (140)']
                cfda_program.regional_or_local_office = row['Regional or, Local Office (151)']
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
                    cfda_program.published_date = datetime.strptime(row['Published Date'], '%b %d,%Y')
                if row['Archived Date']:
                    cfda_program.archived_date = datetime.strptime(row['Archived Date'], '%b %d,%Y')

                # TODO: add way to check/print out any cfda codes that got updated (not just created)
                if created:
                    logger.info('Created a new cfda code for {} ({})'.format(row['Program Number'],
                                                                             row['Program Title']))

                cfda_program.save()

    except IOError:
        logger.info('Could not open file to load: {}'.format(abs_path))

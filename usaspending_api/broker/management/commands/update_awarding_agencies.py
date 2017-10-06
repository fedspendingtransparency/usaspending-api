import logging
import timeit

from django.core.management.base import BaseCommand
from datetime import datetime
from usaspending_api.awards.models import TransactionNormalized, TransactionFABS, TransactionFPDS
from usaspending_api.awards.models import Award
from usaspending_api.references.models import Agency, ToptierAgency, SubtierAgency


logger = logging.getLogger('console')


subtier_agency_map = {subtier_agency['subtier_code']: subtier_agency['subtier_agency_id'] for subtier_agency in SubtierAgency.objects.values('subtier_code', 'subtier_agency_id')}
subtier_to_agency_map = {agency['subtier_agency_id']: {'agency_id': agency['id'], 'toptier_agency_id': agency['toptier_agency_id']} for agency in Agency.objects.values('id', 'toptier_agency_id', 'subtier_agency_id')}
toptier_agency_map = {toptier_agency['toptier_agency_id']: toptier_agency['cgac_code'] for toptier_agency in ToptierAgency.objects.values('toptier_agency_id', 'cgac_code')}
agency_no_sub_map = {(agency.toptier_agency.cgac_code, agency.subtier_agency.subtier_code): agency for agency in Agency.objects.all()}
agency_cgac_only_map = {agency.toptier_agency.cgac_code: agency for agency in Agency.objects.filter(subtier_agency__isnull=True)}
agency_toptier_map = {agency.toptier_agency.cgac_code: agency for agency in Agency.objects.filter(toptier_flag=True)}

class Command(BaseCommand):

    help = "Updates empty awarding and funding agency fields on transactions and awards due to subtier/toptier mapping"

    @staticmethod
    def update_awarding_funding_agency(fiscal_year=None, file_type=None, page=1, limit=500000):

        """
        Uses the TransactionFPDS or TransactionFABS is present to update missing awarding and funding agency
        in TransactionNormalized and Awards
        """

        offset = (page - 1) * limit

        range_low = offset
        range_high = offset + limit

        if file_type == 'D1':
            # List of Transaction FPDS mapping transaction ids, cgac code, and subtier code
            # Filters out FPDS transactions where the transaction is equal to the fiscal year
            transaction_cgac_subtier_map = [
                                               {
                                                'transaction_id': transaction_FPDS['transaction_id'],
                                                'awarding_cgac_code': transaction_FPDS['awarding_agency_code'],
                                                'funding_cgac_code': transaction_FPDS['funding_agency_code'],
                                                'awarding_subtier_code': transaction_FPDS['awarding_sub_tier_agency_c'],
                                                'funding_subtier_code': transaction_FPDS['funding_sub_tier_agency_co']
                                               }
                                               for transaction_FPDS in TransactionFPDS.objects
                                               .filter(transaction__fiscal_year=fiscal_year)
                                               .values('transaction_id',
                                                       'awarding_agency_code',
                                                       'funding_agency_code',
                                                       'awarding_sub_tier_agency_c',
                                                       'funding_sub_tier_agency_co'
                                                       )[range_low:range_high]
                                            ]
        elif file_type == 'D2':
            # List of Transaction FABS mapping transaction ids, cgac code, and subtier code
            # Filters out FABS transactions where the where the transaction is equal to the fiscal year
            transaction_cgac_subtier_map = [
                                                {
                                                 'transaction_id': transaction_FABS['transaction_id'],
                                                 'awarding_cgac_code': transaction_FABS['awarding_agency_code'],
                                                 'funding_cgac_code': transaction_FABS['funding_agency_code'],
                                                 'awarding_subtier_code': transaction_FABS['awarding_sub_tier_agency_c'],
                                                 'funding_subtier_code': transaction_FABS['funding_sub_tier_agency_co']
                                                }
                                                for transaction_FABS in TransactionFABS.objects
                                                .filter(transaction__fiscal_year=fiscal_year)
                                                .values('transaction_id',
                                                        'awarding_agency_code',
                                                        'funding_agency_code',
                                                        'awarding_sub_tier_agency_c',
                                                        'funding_sub_tier_agency_co'
                                                        )[range_low:range_high]
                                            ]

        total_rows = len(transaction_cgac_subtier_map)

        logger.info("Processing " + str(total_rows) + " rows of transaction data")
        logger.info("Rows range from {} to {}".format(range_low, range_high))

        # Go through each D1 or D2 transaction to update awarding/funding agency if missing

        index = 1

        start_time = datetime.now()
        for row in transaction_cgac_subtier_map:

            if not (index % 100):
                logger.info('Updating agencies: Loading row {} of {} ({})'.format(str(index),
                                                                             str(total_rows),
                                                                             datetime.now() - start_time))

            index += 1

            # Find corresponding transaction
            transaction = TransactionNormalized.objects.filter(id=row['transaction_id']).first()

            # Skips transaction if unable to find it in Transaction Normalized
            if transaction is None:
                logger.error('Unable to find Transaction {}'.format(str(row['transaction_id'])))
                continue

            # Update awarding and funding agency if awarding of funding agency is empty
            awarding_agency = Agency.get_by_toptier_subtier(row['awarding_cgac_code'], row['awarding_subtier_code'])
            funding_agency = Agency.get_by_toptier_subtier(row['funding_cgac_code'], row['funding_subtier_code'])

            # Find the agency that this award transaction belongs to. If it doesn't exist, create it.
            awarding_agency = agency_no_sub_map.get((
                row['awarding_cgac_code'],
                row["awarding_subtier_code"]
            ))

            if awarding_agency is None:
                awarding_agency = agency_cgac_only_map.get(row['awarding_cgac_code'])

            funding_agency = agency_no_sub_map.get((
                row['funding_cgac_code'],
                row["funding_subtier_code"]
            ))

            if funding_agency is None:
                funding_agency = agency_cgac_only_map.get(row['funding_cgac_code'])

            # If unable to get agency moves on to the next transaction
            if awarding_agency is None and funding_agency is None:
                logger.error('Unable to find awarding agency CGAC {} Subtier {} and funding agency CGAC {} Subtier {}'
                             .format(
                                row['awarding_cgac_code'],
                                row['awarding_subtier_code'],
                                row['funding_cgac_code'],
                                row['awarding_subtier_code'])
                             )
                continue

            if awarding_agency is None:
                logger.error('Unable to find awarding agency for CGAC {} Subtier {}'.format(
                                                                                            row['awarding_cgac_code'],
                                                                                            row['awarding_subtier_code']
                                                                                            ))

            elif funding_agency is None:
                pass
                #logger.error('Unable to find funding agency for CGAC {} Subtier {}'.format(
                   # row['funding_cgac_code'],
                   # row['funding_subtier_code']
                #))

            # Update awarding/funding agency connected to transaction
           # if transaction.awarding_agency is None and awarding_agency is not None:
            transaction.awarding_agency = awarding_agency

           # if transaction.funding_agency is None and funding_agency is not None:
            transaction.funding_agency = funding_agency

            # Update awarding/funding agency connected to transaction's award
            award = Award.objects.filter(id=transaction.award.id).first()

            if award is None:
                logger.error('Unable to find Award {}'.format(str(transaction.award.id)))
                continue

            #if award.awarding_agency is None and awarding_agency is not None:
            award.awarding_agency = awarding_agency

            #if award.funding_agency is None and funding_agency is not None:
            award.funding_agency = funding_agency

            try:
                # Save updates to Database
                transaction.save()
                award.save()

            except Exception as e:
                logger.error('Unable to save Transaction {} and Award {}:{}'.format(str(transaction.id),
                                                                                    str(award.id),
                                                                                    str(e)))

    def add_arguments(self, parser):

        parser.add_argument(
            '--fiscal_year',
            dest='fiscal_year',
            nargs='+',
            type=int,
            help="Year for which to run awarding agency clean up on"
        )

        parser.add_argument(
            '--assistance',
            action='store_true',
            dest='assistance',
            default=False,
            help='Runs the award only for Award Financial Assistance (Assistance) data'
        )

        parser.add_argument(
            '--contracts',
            action='store_true',
            dest='contracts',
            default=False,
            help='Runs the historical loader only for Award Procurement (Contract) data'
        )

        parser.add_argument(
            '--page',
            dest="page",
            nargs='+',
            type=int,
            help="Page for batching and parallelization"
        )

        parser.add_argument(
            '--limit',
            dest="limit",
            nargs='+',
            type=int,
            help="Limit for batching and parallelization"
        )

    def handle(self, *args, **options):
        logger.info('Starting updating awarding agencies...')

        fiscal_year = options.get('fiscal_year')[0]

        page = options.get('page')
        limit = options.get('limit')

        page = page[0] if page else 1
        limit = limit[0] if limit else 500000

        if options.get('contracts', None):
            logger.info('Starting D1 awarding/funding agencies updates')
            start = timeit.default_timer()
            self.update_awarding_funding_agency(fiscal_year, 'D1', page=page, limit=limit)
            end = timeit.default_timer()
            logger.info('Finished D1 awarding agencies updates in ' + str(end - start) + ' seconds')

        elif options.get('assistance', None):
            logger.info('Starting D2 awarding/funding agencies updates')
            start = timeit.default_timer()
            self.update_awarding_funding_agency(fiscal_year, 'D2', page=page, limit=limit)
            end = timeit.default_timer()
            logger.info('Finished D2 awarding/funding agencies updates in ' + str(end - start) + ' seconds')

        else:
            logger.error('Not a valid data type: --assistance,--contracts')

        logger.info('Finished')
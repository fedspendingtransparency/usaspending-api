import logging
import timeit

from django.core.management.base import BaseCommand
from django.db.models import Q

from usaspending_api.awards.models import TransactionNormalized, TransactionFABS, TransactionFPDS
from usaspending_api.awards.models import Award
from usaspending_api.references.models import Agency


logger = logging.getLogger('console')


class Command(BaseCommand):

    help = "Updates empty awarding and funding agency fields on transactions and awards due to subtier/toptier mapping"

    @staticmethod
    def update_awarding_funding_agency(fiscal_year=None, file_type=None):

        """
        Uses the TransactionFPDS or TransactionFABS is present to update missing awarding and funding agency
        in TransactionNormalized and Awards
        """

        if file_type == 'D1':
            # List of Transaction FPDS mapping transaction ids, cgac code, and subtier code
            # Filters out FPDS transactions where the transaction's awarding or funding agency is null and by fiscal year
            transaction_cgac_subtier_map = [
                                               {
                                                'transaction_id': transaction['transaction_id'],
                                                'awarding_cgac_code': transaction['awarding_agency_code'],
                                                'funding_cgac_code': transaction['funding_agency_code'],
                                                'awarding_subtier_code': transaction['awarding_sub_tier_agency_c'],
                                                'funding_subtier_code': transaction['funding_sub_tier_agency_co']
                                               }
                                               for transaction in TransactionFPDS.objects
                                               .filter(Q(transaction__fiscal_year=fiscal_year) &
                                                       (Q(transaction__awarding_agency__isnull=True) |
                                                        Q(transaction__funding_agency__isnull=True))
                                                       )
                                               .values('transaction_id',
                                                       'awarding_agency_code',
                                                       'funding_agency_code',
                                                       'awarding_sub_tier_agency_c',
                                                       'funding_sub_tier_agency_co'
                                                       )
                                            ]
        elif file_type == 'D2':
            # List of Transaction FABS mapping transaction ids, cgac code, and subtier code
            # Filters out FABS transactions where the transaction's awarding or funding agency is null and by fiscal year
            transaction_cgac_subtier_map = [
                                                {
                                                 'transaction_id': transaction['transaction_id'],
                                                 'awarding_cgac_code': transaction['awarding_agency_code'],
                                                 'funding_cgac_code': transaction['funding_agency_code'],
                                                 'awarding_subtier_code': transaction['awarding_sub_tier_agency_c'],
                                                 'funding_subtier_code': transaction['funding_sub_tier_agency_co']
                                                }
                                                for transaction in TransactionFABS.objects
                                                .filter(Q(transaction__fiscal_year=fiscal_year) &
                                                        (Q(transaction__awarding_agency__isnull=True) |
                                                         Q(transaction__funding_agency__isnull=True))
                                                        )
                                                .values('transaction_id',
                                                        'awarding_agency_code',
                                                        'funding_agency_code',
                                                        'awarding_sub_tier_agency_c',
                                                        'funding_sub_tier_agency_co'
                                                        )
                                            ]

        total_rows = len(transaction_cgac_subtier_map)

        logger.info("Processing " + str(total_rows) + " rows of transaction data")

        # Go through each D1 or D2 transaction to update awarding/funding agency if missing
        for row in transaction_cgac_subtier_map:
            # Find corresponding transaction
            transaction = TransactionNormalized.objects.filter(id=row['transaction_id']).first()

            # Skips transaction if unable to find it in Transaction Normalized
            if transaction is None:
                logger.error('Unable to find Transaction {}'.format(str(row['transaction_id'])))
                continue

            # Update awarding and funding agency if awarding of funding agency is empty
            awarding_agency = Agency.get_by_toptier_subtier(row['awarding_cgac_code'], row['awarding_subtier_code'])
            funding_agency = Agency.get_by_toptier_subtier(row['funding_cgac_code'], row['funding_subtier_code'])

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

            if funding_agency is None and row['funding_cgac_code'] is None and row['funding_subtier_code'] is None:
                logger.info('No funding agency for transaction'.format(str(row['transaction_id'])))

            elif funding_agency is None:
                logger.error('Unable to find awarding agency for CGAC {} Subtier {}'.format(
                    row['funding_cgac_code'],
                    row['funding_subtier_code']
                ))

            # Update awarding/funding agency connected to transaction
            if transaction.awarding_agency is None and awarding_agency is not None:
                transaction.awarding_agency = awarding_agency

            if transaction.funding_agency is None and funding_agency is not None:
                transaction.funding_agency = funding_agency

            # Update awarding/funding agency connected to transaction's award
            award = Award.objects.filter(id=transaction.award.id).first()

            if award is None:
                logger.error('Unable to find Award {}'.format(str(transaction.award.id)))
                continue

            if award.awarding_agency is None and awarding_agency is not None:
                award.awarding_agency = awarding_agency

            if award.funding_agency is None and funding_agency is not None:
                award.funding_agency = funding_agency

            try:
                # Save updates to Database
                transaction.save()
                award.save()

                logger.info('Transaction {}: Awarding and funding agency fields updated'.format(str(transaction.id)))
                logger.info('Award {}: Awarding and funding agency fields updated'.format(str(award.id)))

            except:
                logger.error('Unable to save Transaction {} and Award {}'.format(str(transaction.id),
                                                                                 str(award.id)))

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

    def handle(self, *args, **options):
        logger.info('Starting updating awarding agencies...')

        fiscal_year = options.get('fiscal_year')[0]

        if options.get('contracts', None):
            logger.info('Starting D1 awarding agencies updates')
            start = timeit.default_timer()
            self.update_awarding_funding_agency(fiscal_year, 'D1')
            end = timeit.default_timer()
            logger.info('Finished D1 awarding agencies updates in ' + str(end - start) + ' seconds')

        elif options.get('assistance', None):
            logger.info('Starting D2 awarding agencies updates')
            start = timeit.default_timer()
            self.update_awarding_funding_agency(fiscal_year, 'D2')
            end = timeit.default_timer()
            logger.info('Finished D2 awarding agencies updates in ' + str(end - start) + ' seconds')

        else:
            logger.error('Not a valid data type: --assistance,--contracts')

        logger.info('Finished')

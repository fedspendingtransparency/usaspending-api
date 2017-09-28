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
                                                'cgac_code': transaction['awarding_agency_code'],
                                                'subtier_code': transaction['awarding_sub_tier_agency_c'],
                                               }
                                               for transaction in TransactionFPDS.objects
                                               .filter(Q(transaction__fiscal_year=fiscal_year) &
                                                       (Q(transaction__awarding_agency__isnull=True) |
                                                        Q(transaction__funding_agency__isnull=True))
                                                       )
                                               .values('transaction_id',
                                                       'awarding_agency_code',
                                                       'awarding_sub_tier_agency_c'
                                                       )
                                            ]
        elif file_type == 'D2':
            # List of Transaction FABS mapping transaction ids, cgac code, and subtier code
            # Filters out FABS transactions where the transaction's awarding or funding agency is null and by fiscal year
            transaction_cgac_subtier_map = [
                                                {
                                                 'transaction_id': transaction['transaction_id'],
                                                 'cgac_code': transaction['awarding_agency_code'],
                                                 'subtier_code': transaction['awarding_sub_tier_agency_c']
                                                }
                                                for transaction in TransactionFABS.objects
                                                .filter(Q(transaction__fiscal_year=fiscal_year) &
                                                        (Q(transaction__awarding_agency__isnull=True) |
                                                         Q(transaction__funding_agency__isnull=True))
                                                        )
                                                .values('transaction_id',
                                                        'awarding_agency_code',
                                                        'awarding_sub_tier_agency_c'
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
            agency = Agency.get_by_toptier_subtier(row['cgac_code'], row['subtier_code'])

            # If unable to get agency moves on to the next transaction
            if agency is None:
                logger.error('Unable to find Agency for CGAC {}, Subtier {}'.format(
                                                                                    row['cgac_code'],
                                                                                    row['subtier_code']))
                continue

            # Update awarding/funding agency connected to transaction
            if transaction.awarding_agency is None:
                transaction.awarding_agency = agency

            if transaction.funding_agency is None:
                transaction.funding_agency = agency

            # Update awarding/funding agency connected to transaction's award
            award = Award.objects.filter(id=transaction.award.id).first()

            if award is None:
                logger.error('Unable to find Award {}'.format(str(transaction.award.id)))
                continue

            if award.awarding_agency is None:
                award.awarding_agency = agency

            if award.funding_agency is None:
                award.funding_agency = agency

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

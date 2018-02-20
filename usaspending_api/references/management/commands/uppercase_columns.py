import logging

from django.core.management.base import BaseCommand
from django.db import connection

logger = logging.getLogger('console')


class Command(BaseCommand):
    def handle(self, *args, **options):
        with connection.cursor() as db_cursor:

            logger.info("Starting uppercase fixes")

            for table, sql_string in UPPERCASE_UPDATES.items():
                logger.info("Updating {} table columns".format(table))
                db_cursor.execute(sql_string)

            logger.info("Completed uppercase fixes")


UPPERCASE_UPDATES = {
    'transaction_fabs': """
            UPDATE transaction_fabs
            SET award_description = UPPER(award_description),
                awardee_or_recipient_legal = UPPER(awardee_or_recipient_legal),
                business_types = UPPER(business_types),
                fain = UPPER(fain),
                legal_entity_foreign_city = UPPER(legal_entity_foreign_city),
                legal_entity_foreign_posta = UPPER(legal_entity_foreign_posta),
                legal_entity_foreign_provi = UPPER(legal_entity_foreign_provi),
                place_of_performance_forei = UPPER(place_of_performance_forei),
                place_of_performance_zip4a = UPPER(place_of_performance_zip4a),
                sai_number = UPPER(sai_number),
                cfda_title = UPPER(cfda_title),
                awarding_agency_name = UPPER(awarding_agency_name),
                awarding_sub_tier_agency_n = UPPER(awarding_sub_tier_agency_n),
                funding_agency_name = UPPER(funding_agency_name),
                funding_sub_tier_agency_na = UPPER(funding_sub_tier_agency_na),
                afa_generated_unique = UPPER(afa_generated_unique),
                place_of_performance_zip5 = UPPER(place_of_performance_zip5),
                place_of_perform_zip_last4 = UPPER(place_of_perform_zip_last4)
            WHERE UPPER(award_description) != award_description
                OR UPPER(awardee_or_recipient_legal) != awardee_or_recipient_legal
                OR UPPER(business_types) != business_types
                OR UPPER(fain) != fain
                OR UPPER(legal_entity_foreign_city) != legal_entity_foreign_city
                OR UPPER(legal_entity_foreign_posta) != legal_entity_foreign_posta
                OR UPPER(legal_entity_foreign_provi) != legal_entity_foreign_provi
                OR UPPER(place_of_performance_forei) != place_of_performance_forei
                OR UPPER(place_of_performance_zip4a) != place_of_performance_zip4a
                OR UPPER(sai_number) != sai_number
                OR UPPER(cfda_title) != cfda_title
                OR UPPER(awarding_agency_name) != awarding_agency_name
                OR UPPER(awarding_sub_tier_agency_n) != awarding_sub_tier_agency_n
                OR UPPER(funding_agency_name) != funding_agency_name
                OR UPPER(funding_sub_tier_agency_na) != funding_sub_tier_agency_na
                OR UPPER(afa_generated_unique) != afa_generated_unique
                OR UPPER(place_of_performance_zip5) != place_of_performance_zip5
                OR UPPER(place_of_perform_zip_last4) != place_of_perform_zip_last4;
        """}

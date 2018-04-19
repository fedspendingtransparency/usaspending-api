import logging

from django.db.models import F

from usaspending_api.awards.v2.filters.filter_helpers import sum_transaction_amount

# from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.awards.v2.filters.view_selector import spending_by_category as sbc_view_queryset
from usaspending_api.awards.v2.lookups.lookups import award_type_mapping

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers import get_simple_pagination_metadata
from usaspending_api.core.validator.award_filter import AWARD_FILTER
from usaspending_api.references.models import Cfda

logger = logging.getLogger(__name__)


class business_logic:
    def __new__(cls, payload):
        cls.category = payload['category']
        cls.scope = payload['scope']
        cls.page = payload['page']
        cls.limit = payload['limit']

        cls.lower_limit = (cls.page - 1) * cls.limit
        cls.upper_limit = cls.page * cls.limit

        cls.filters = {
            item['name']: payload[item['name']] for item in AWARD_FILTER if item['name'] in payload}

        if (cls.scope is None) and (cls.category != "cfda_programs"):
            raise InvalidParameterException("Missing one or more required request parameters: scope")

        if 'award_type_codes' in cls.filters:
            cls.filter_types = cls.filters['award_type_codes']
        else:
            cls.filter_types = award_type_mapping
        cls.queryset, cls.model = sbc_view_queryset('{}-{}'.format(cls.category, cls.scope or ''), cls.filters)

        return cls.logic(cls)

    def logic(self):
        # filter the transactions by category
        if self.category == "awarding_agency":
            results = self.awarding_agency(self)
        elif self.category == "funding_agency":
            results = self.funding_agency(self)
        elif self.category == 'recipient':
            results = self.recipient(self)
        elif self.category == 'cfda_programs':
            results = self.cfda_programs(self)
        elif self.category == 'industry_codes':
            results = self.industry_codes(self)
        else:
            raise InvalidParameterException("Category \"{}\" is not yet implemented".format(self.category))

        page_metadata = get_simple_pagination_metadata(len(results), self.limit, self.page)

        response = {
            "category": self.category,
            "limit": self.limit,
            "page_metadata": page_metadata,
            "results": results[:self.limit],
            "scope": self.scope,
        }
        return response

    def awarding_agency(self):
        # filter self.queryset
        if self.scope == "agency":
            self.queryset = self.queryset \
                .filter(awarding_toptier_agency_name__isnull=False) \
                .values(
                    agency_name=F('awarding_toptier_agency_name'),
                    agency_abbreviation=F('awarding_toptier_agency_abbreviation'))

        elif self.scope == "subagency":
            self.queryset = self.queryset \
                .filter(
                    awarding_subtier_agency_name__isnull=False) \
                .values(
                    agency_name=F('awarding_subtier_agency_name'),
                    agency_abbreviation=F('awarding_subtier_agency_abbreviation'))

        else:
            raise InvalidParameterException("Scope \"{}\" is not implemented".format(self.scope))

        self.queryset = sum_transaction_amount(self.queryset, 'aggregated_amount', filter_types=self.filter_types)\
            .order_by('-aggregated_amount')
        return list(self.queryset[self.lower_limit:self.upper_limit + 1])

    def funding_agency(self):
        if self.scope == "agency":
            self.queryset = self.queryset \
                .filter(funding_toptier_agency_name__isnull=False) \
                .values(
                    agency_name=F('funding_toptier_agency_name'),
                    agency_abbreviation=F('funding_toptier_agency_abbreviation'))

        elif self.scope == "subagency":
            self.queryset = self.queryset \
                .filter(
                    funding_subtier_agency_name__isnull=False) \
                .values(
                    agency_name=F('funding_subtier_agency_name'),
                    agency_abbreviation=F('funding_subtier_agency_abbreviation'))

        else:
            raise InvalidParameterException("Scope \"{}\" is not implemented".format(self.scope))

        self.queryset = sum_transaction_amount(self.queryset, 'aggregated_amount', filter_types=self.filter_types) \
            .order_by('-aggregated_amount')

        return list(self.queryset[self.lower_limit:self.upper_limit + 1])

    def recipient(self):
        ###################################################################
        # POOR PERFORMANCE. Needs `recipient_id`/`parent_recipient_unique_id` in additional matviews
        ###################################################################
        if self.scope == "duns":
            self.queryset = self.queryset \
                .values(legal_entity_id=F("recipient_id"))
            self.queryset = sum_transaction_amount(self.queryset, 'aggregated_amount', filter_types=self.filter_types) \
                .order_by('-aggregated_amount') \
                .values("aggregated_amount", "legal_entity_id", "recipient_name") \
                .order_by("-aggregated_amount")

        elif self.scope == "parent_duns":
            self.queryset = self.queryset \
                .filter(parent_recipient_unique_id__isnull=False)
            self.queryset = sum_transaction_amount(
                self.queryset,
                'aggregated_amount',
                filter_types=self.filter_types,
                calculate_totals=False) \
                .values(
                    'aggregated_amount',
                    'recipient_name',
                    'parent_recipient_unique_id') \
                .order_by('-aggregated_amount')

        else:
            raise InvalidParameterException("Scope \"{}\" is not implemented".format(self.scope))

        return list(self.queryset[self.lower_limit:self.upper_limit + 1])

    def cfda_programs(self):
        if self.model == 'SummaryCfdaNumbersView':
            self.queryset = self.queryset \
                .filter(
                    federal_action_obligation__isnull=False,
                    cfda_number__isnull=False) \
                .values(cfda_program_number=F("cfda_number"))
            self.queryset = sum_transaction_amount(self.queryset, 'aggregated_amount', filter_types=self.filter_types) \
                .values(
                    "aggregated_amount",
                    "cfda_program_number",
                    program_title=F("cfda_title")) \
                .order_by('-aggregated_amount')

            # Begin DB hits here
            results = list(self.queryset[self.lower_limit:self.upper_limit + 1])

            for trans in results:
                trans['popular_name'] = None
                # small DB hit every loop here
                cfda = Cfda.objects \
                    .filter(
                        program_title=trans['program_title'],
                        program_number=trans['cfda_program_number']) \
                    .values('popular_name').first()

                if cfda:
                    trans['popular_name'] = cfda['popular_name']

        else:
            self.queryset = self.queryset \
                .filter(
                    cfda_number__isnull=False) \
                .values(cfda_program_number=F("cfda_number"))
            self.queryset = sum_transaction_amount(self.queryset, 'aggregated_amount', filter_types=self.filter_types) \
                .values(
                    "aggregated_amount",
                    "cfda_program_number",
                    popular_name=F("cfda_popular_name"),
                    program_title=F("cfda_title")) \
                .order_by('-aggregated_amount')

            # Begin DB hits here
            results = list(self.queryset[self.lower_limit:self.upper_limit + 1])

        return results

    def industry_codes(self):
        if self.scope == "psc":
            self.queryset = self.queryset \
                .filter(product_or_service_code__isnull=False) \
                .values(psc_code=F("product_or_service_code"))

            self.queryset = sum_transaction_amount(self.queryset, 'aggregated_amount', filter_types=self.filter_types) \
                .order_by('-aggregated_amount')

        elif self.scope == "naics":
            self.queryset = self.queryset \
                .filter(naics_code__isnull=False) \
                .values('naics_code')
            self.queryset = sum_transaction_amount(self.queryset, 'aggregated_amount', filter_types=self.filter_types) \
                .order_by('-aggregated_amount') \
                .values(
                    'naics_code',
                    'aggregated_amount',
                    'naics_description')
        else:
            raise InvalidParameterException("Scope \"{}\" is not implemented".format(self.scope))
        return list(self.queryset[self.lower_limit:self.upper_limit + 1])

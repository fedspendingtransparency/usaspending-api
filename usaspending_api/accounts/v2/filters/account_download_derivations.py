import logging

from django.db.models import Case, CharField, OuterRef, Subquery, When, Value
from django.db.models.functions import Concat

from usaspending_api.references.models import ToptierAgency

logger = logging.getLogger(__name__)


def base_treasury_account_derivations(tas_id):
    # Derive treasury_account_symbol, allocation_transfer_agency_name, agency_name, and federal_account_symbol
    ata_subquery = ToptierAgency.objects.filter(cgac_code=OuterRef('{}__allocation_transfer_agency_id'.format(tas_id)))
    agency_name_subquery = ToptierAgency.objects.filter(cgac_code=OuterRef('{}__agency_id'.format(tas_id)))

    return {
        'treasury_account_symbol': Concat(
            '{}__agency_id'.format(tas_id),
            Value('-'),
            Case(When(**{'{}__availability_type_code'.format(tas_id): 'X', 'then': Value('X')}),
                 default=Concat('{}__beginning_period_of_availability'.format(tas_id), Value('/'),
                                '{}__ending_period_of_availability'.format(tas_id)),
                 output_field=CharField()),
            Value('-'),
            '{}__main_account_code'.format(tas_id),
            Value('-'),
            '{}__sub_account_code'.format(tas_id),
            output_field=CharField()),
        'allocation_transfer_agency_name': Subquery(ata_subquery.values('name')[:1]),
        'agency_name': Subquery(agency_name_subquery.values('name')[:1]),
        'federal_account_symbol': Concat('{}__federal_account__agency_identifier'.format(tas_id), Value('-'),
                                         '{}__federal_account__main_account_code'.format(tas_id))
    }

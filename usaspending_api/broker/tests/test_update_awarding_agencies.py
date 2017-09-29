from model_mommy import mommy
import pytest

from django.core.management import call_command

from usaspending_api.awards.models import TransactionNormalized, TransactionFABS, TransactionFPDS
from usaspending_api.awards.models import Award
from usaspending_api.references.models import Agency, SubtierAgency, ToptierAgency

@pytest.fixture()
def transaction_data():
    """Mock data for various transactions"""

    # Contract Transaction - Fiscal Year 2017
    award_fpds_2017 = mommy.make(Award, id=10, awarding_agency=None, funding_agency=None)

    transaction_normalized_fpds_2017 = mommy.make(TransactionNormalized, id=1234, awarding_agency=None,
                                                  funding_agency=None,
                                                  fiscal_year=2017, award=award_fpds_2017)

    transaction_fpds_2017 = mommy.make(TransactionFPDS, transaction=transaction_normalized_fpds_2017,
                                       awarding_agency_code='001', awarding_sub_tier_agency_c='011',
                                       funding_agency_code='002', funding_sub_tier_agency_co='022'
                                       )

    # Assistance Transaction - Fiscal Year 20176
    award_fabs_2017 = mommy.make(Award, id=40, awarding_agency=None, funding_agency=None)
    transaction_normalized_fabs_2017 = mommy.make(TransactionNormalized, id=13141516, awarding_agency=None,
                                                  funding_agency=None,
                                                  fiscal_year=2017, award=award_fabs_2017)

    transaction_fabs_2017 = mommy.make(TransactionFABS, transaction=transaction_normalized_fabs_2017,
                                       awarding_agency_code='001', awarding_sub_tier_agency_c='011',
                                       funding_agency_code=None, funding_sub_tier_agency_co=None
                                       )

    # Awarding Agency lookup
    subtier_awarding_agency = mommy.make(SubtierAgency, subtier_agency_id=456, subtier_code='011')

    toptier_awarding_agency = mommy.make(ToptierAgency, toptier_agency_id=123, cgac_code='001')
    awarding_agency = mommy.make(Agency, id=123456, toptier_agency=toptier_awarding_agency,
                        subtier_agency=subtier_awarding_agency)

    # Funding Agency lookup
    subtier_funding_agency = mommy.make(SubtierAgency, subtier_agency_id=578, subtier_code='022')
    toptier_funding_agency = mommy.make(ToptierAgency, toptier_agency_id=910, cgac_code='002')
    funding_agency = mommy.make(Agency, id=45678, toptier_agency=toptier_funding_agency,
                                subtier_agency=subtier_funding_agency)




@pytest.mark.django_db
def test_contracts_command(transaction_data):
    """
    :param transaction_data: takes transaction data from above
    :return: tests FPDS data FY 2017 to make sure it correct funding and awarding agency data is added
    """
    call_command('update_awarding_agencies', '--fiscal_year', 2017, '--contracts')

    fpds_transaction_2017 = TransactionNormalized.objects.filter(id=1234).first()
    fpds_award_2017 = Award.objects.filter(id=10).first()
    fabs_transaction_2017 = TransactionNormalized.objects.filter(id=13141516).first()
    fabs_award_2017 = Award.objects.filter(id=40).first()

    awarding_agency = Agency.objects.filter(id=123456).first()
    funding_agency = Agency.objects.filter(id=45678).first()

    assert fpds_transaction_2017.awarding_agency == awarding_agency
    assert fpds_transaction_2017.funding_agency == funding_agency
    assert fpds_award_2017.awarding_agency == awarding_agency
    assert fpds_award_2017.funding_agency == funding_agency

    assert fabs_transaction_2017.awarding_agency is None
    assert fabs_transaction_2017.funding_agency is None
    assert fabs_award_2017.awarding_agency is None
    assert fabs_award_2017.funding_agency is None

@pytest.mark.django_db
def test_assistance_command(transaction_data):
    """
    :param transaction_data: takes transaction data from above
    :return: tests FABS data FY 2017 to make sure it correct funding and awarding agency data is added
    takes into account when funding agency code is missing from transaction data
    """
    call_command('update_awarding_agencies', '--fiscal_year', 2017, '--assistance')

    fpds_transaction_2017 = TransactionNormalized.objects.filter(id=1234).first()
    fpds_award_2017 = Award.objects.filter(id=10).first()
    fabs_transaction_2017 = TransactionNormalized.objects.filter(id=13141516).first()
    fabs_award_2017 = Award.objects.filter(id=40).first()

    awarding_agency = Agency.objects.filter(id=123456).first()

    assert fpds_transaction_2017.awarding_agency is None
    assert fpds_transaction_2017.funding_agency is None
    assert fpds_award_2017.awarding_agency is None
    assert fpds_award_2017.funding_agency is None

    assert fabs_transaction_2017.awarding_agency == awarding_agency
    assert fabs_transaction_2017.funding_agency == awarding_agency
    assert fabs_award_2017.awarding_agency == awarding_agency
    assert fabs_award_2017.funding_agency == awarding_agency





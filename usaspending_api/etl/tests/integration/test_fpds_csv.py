import os

from model_bakery import baker
import pytest

from django.core.management import call_command

from usaspending_api.awards.models import TransactionFPDS
from usaspending_api.search.models import TransactionSearch


@pytest.fixture()
def agency_data(db):
    call_command("flush", "--noinput")
    global fpds_transaction1, fpds_transaction2, fpds_transaction3, fpds_transaction4, fpds_transaction5

    fpds_transaction1 = baker.make(
        TransactionSearch, is_fpds=True, detached_award_proc_unique="4732_4730_GSMPNB710761_0_GS06F0007J_0"
    )
    fpds_transaction2 = baker.make(
        TransactionSearch, is_fpds=True, detached_award_proc_unique="4740_4740_GS02B2365011120_0_GS02B23650_0"
    )
    fpds_transaction3 = baker.make(
        TransactionSearch, is_fpds=True, detached_award_proc_unique="9700_9700_0003_24_W52P1J10D0022_1"
    )
    fpds_transaction4 = baker.make(
        TransactionSearch, is_fpds=True, detached_award_proc_unique="9700_-none-_FA520908P0219_P00004_-none-_0"
    )
    fpds_transaction5 = baker.make(
        TransactionSearch, is_fpds=True, detached_award_proc_unique="9700_9700_N407_1_N0017804D4113_14"
    )


@pytest.mark.skip(reason="subaward loading is on hold until broker resolves data anomalies")
@pytest.mark.django_db
def test_fpds_csv(agency_data):
    global fpds_transaction1, fpds_transaction2, fpds_transaction3, fpds_transaction4, fpds_transaction5

    call_command("load_fpds_csv", os.path.join(os.path.dirname(__file__), "../../../data/testing_data/fpds_small.csv"))

    fpds_transaction1 = TransactionFPDS.objects.filter(
        detached_award_proc_unique="4732_4730_GSMPNB710761_0_GS06F0007J_0"
    ).first()
    fpds_transaction2 = TransactionFPDS.objects.filter(
        detached_award_proc_unique="4740_4740_GS02B2365011120_0_GS02B23650_0"
    ).first()
    fpds_transaction3 = TransactionFPDS.objects.filter(
        detached_award_proc_unique="9700_9700_0003_24_W52P1J10D0022_1"
    ).first()
    fpds_transaction4 = TransactionFPDS.objects.filter(
        detached_award_proc_unique="9700_-none-_FA520908P0219_P00004_-none-_0"
    ).first()
    fpds_transaction5 = TransactionFPDS.objects.filter(
        detached_award_proc_unique="9700_9700_N407_1_N0017804D4113_14"
    ).first()

    assert fpds_transaction1.base_exercised_options_val == str(64.32)
    assert fpds_transaction1.base_and_all_options_value == str(64.32)

    assert fpds_transaction2.base_exercised_options_val == str(10719.76)
    assert fpds_transaction2.base_and_all_options_value == str(246554.48)

    assert fpds_transaction3.base_exercised_options_val == str(-22545.78)
    assert fpds_transaction3.base_and_all_options_value == str(-22545.78)

    assert fpds_transaction4.base_exercised_options_val == str(57497.82)
    assert fpds_transaction4.base_and_all_options_value == str(6425.35)

    assert fpds_transaction5.base_exercised_options_val == str(115810.00)
    assert fpds_transaction5.base_and_all_options_value == str(0.00)

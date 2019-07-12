import pytest

from itertools import cycle

from usaspending_api.awards.models import TransactionNormalized, TransactionFPDS, TransactionFABS
from usaspending_api.etl.helpers import update_model_description_fields
from usaspending_api.data.daims_maps import daims_maps
from model_mommy.recipe import Recipe

"""
This tests a handful of description fields to ensure the command works, but is
not exhaustive over _all_ description fields.
"""


@pytest.fixture()
def description_updatable_models():
    cost_or_pricing_data = daims_maps["cost_or_pricing_data_map"].keys()
    multiple_or_single_award_i = daims_maps["multiple_or_single_award_i_map"].keys()
    cost_accounting_standards = daims_maps["cost_accounting_standards_map"].keys()
    fed_biz_opps = daims_maps["fed_biz_opps_map"].keys()
    action_type = ["A", "B", "C", "D"]

    transaction_contract_recipe = Recipe(
        TransactionFPDS,
        cost_or_pricing_data=cycle(cost_or_pricing_data),
        multiple_or_single_award_i=cycle(multiple_or_single_award_i),
        cost_accounting_standards=cycle(cost_accounting_standards),
        fed_biz_opps=cycle(fed_biz_opps),
        transaction__action_type=cycle(action_type),
    )

    transaction_assistance_recipe = Recipe(TransactionFABS, transaction__action_type=cycle(action_type))

    transaction_contract_recipe.make(_quantity=10)
    transaction_assistance_recipe.make(_quantity=10)


@pytest.mark.skip
@pytest.mark.django_db
def test_description_fields(description_updatable_models):
    update_model_description_fields()

    for item in TransactionFPDS.objects.all():
        assert item.cost_or_pricing_data_desc == daims_maps["cost_or_pricing_data_map"][item.cost_or_pricing_data]
        assert (
            item.multiple_or_single_aw_desc
            == daims_maps["multiple_or_single_award_i_map"][item.multiple_or_single_award_i]
        )
        assert (
            item.cost_accounting_stand_desc
            == daims_maps["cost_accounting_standards_map"][item.cost_accounting_standards]
        )
        assert item.fed_biz_opps_description == daims_maps["fed_biz_opps_map"][item.fed_biz_opps]

    contract_transactions = TransactionNormalized.objects.filter(
        assistance_data__isnull=False, contract_data__isnull=True
    )
    assistance_transactions = TransactionNormalized.objects.filter(
        assistance_data__isnull=True, contract_data__isnull=False
    )

    assert (
        assistance_transactions.filter(action_type="A").first().action_type_description
        == daims_maps["TransactionNormalized.action_type_map"]["case_2_map"]["A"]
    )
    assert (
        assistance_transactions.filter(action_type="B").first().action_type_description
        == daims_maps["TransactionNormalized.action_type_map"]["case_2_map"]["B"]
    )
    assert (
        assistance_transactions.filter(action_type="C").first().action_type_description
        == daims_maps["TransactionNormalized.action_type_map"]["case_2_map"]["C"]
    )
    assert (
        assistance_transactions.filter(action_type="D").first().action_type_description
        == daims_maps["TransactionNormalized.action_type_map"]["case_2_map"]["D"]
    )

    assert (
        contract_transactions.filter(action_type="A").first().action_type_description
        == daims_maps["TransactionNormalized.action_type_map"]["case_1_map"]["A"]
    )
    assert (
        contract_transactions.filter(action_type="B").first().action_type_description
        == daims_maps["TransactionNormalized.action_type_map"]["case_1_map"]["B"]
    )
    assert (
        contract_transactions.filter(action_type="C").first().action_type_description
        == daims_maps["TransactionNormalized.action_type_map"]["case_1_map"]["C"]
    )
    assert (
        contract_transactions.filter(action_type="D").first().action_type_description
        == daims_maps["TransactionNormalized.action_type_map"]["case_1_map"]["D"]
    )

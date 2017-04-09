import pytest

from model_mommy import mommy
from itertools import cycle

from usaspending_api.awards.models import TransactionContract
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
    multiple_or_single_award_idv = daims_maps["multiple_or_single_award_idv_map"].keys()
    cost_accounting_standards = daims_maps["cost_accounting_standards_map"].keys()
    fed_biz_opps = daims_maps["fed_biz_opps_map"].keys()

    transaction_contract_recipe = Recipe(
        TransactionContract,
        cost_or_pricing_data=cycle(cost_or_pricing_data),
        multiple_or_single_award_idv=cycle(multiple_or_single_award_idv),
        cost_accounting_standards=cycle(cost_accounting_standards),
        fed_biz_opps=cycle(fed_biz_opps),
    )

    transaction_contract_recipe.make(_quantity=10)


@pytest.mark.django_db
def test_description_fields(description_updatable_models):
    update_model_description_fields()

    for item in TransactionContract.objects.all():
        assert item.cost_or_pricing_data_description == daims_maps["cost_or_pricing_data_map"][item.cost_or_pricing_data]
        assert item.multiple_or_single_award_idv_description == daims_maps["multiple_or_single_award_idv_map"][item.multiple_or_single_award_idv]
        assert item.cost_accounting_standards_description == daims_maps["cost_accounting_standards_map"][item.cost_accounting_standards]
        assert item.fed_biz_opps_description == daims_maps["fed_biz_opps_map"][item.fed_biz_opps]

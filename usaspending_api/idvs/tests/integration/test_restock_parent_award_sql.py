import pytest

from django.core.management import call_command
from model_bakery import baker

from usaspending_api.idvs.tests.data.idv_data import create_tree
from usaspending_api.awards.models import ParentAward

c1 = {
    "generated_unique_award_id": "c1",
    "piid": "5678",
    "parent_award_piid": "ABCD_PARENT_1",
    "fpds_parent_agency_id": "1234",
    "total_obligation": 1000,
    "base_and_all_options_value": 2000,
    "base_exercised_options_val": 3000,
    "type": "A",
}
c2 = {
    "generated_unique_award_id": "c2",
    "piid": "7890",
    "parent_award_piid": "ABCD_PARENT_1",
    "fpds_parent_agency_id": "1234",
    "total_obligation": 1000,
    "base_and_all_options_value": 2000,
    "base_exercised_options_val": 3000,
    "type": "A",
}
c3 = {
    "generated_unique_award_id": "c3",
    "piid": "ABCD",
    "parent_award_piid": "ABCD_PARENT_1",
    "fpds_parent_agency_id": "1234",
    "total_obligation": 1000,
    "base_and_all_options_value": 2000,
    "base_exercised_options_val": 3000,
    "type": "A",
}
p1 = {
    "generated_unique_award_id": "p1",
    "piid": "ABCD_PARENT_1",
    "fpds_agency_id": "1234",
    "parent_award_piid": "ABCD_TOP_PARENT_1",
    "fpds_parent_agency_id": "3333",
    "total_obligation": 0,
    "base_and_all_options_value": 0,
    "base_exercised_options_val": 0,
    "type": "IDV_A",
}

c4 = {
    "generated_unique_award_id": "c4",
    "piid": "GAHDS",
    "parent_award_piid": "ABCD_PARENT_2",
    "fpds_parent_agency_id": "7890",
    "total_obligation": 1000,
    "base_and_all_options_value": 1000,
    "base_exercised_options_val": 1000,
    "type": "A",
}
p2 = {
    "generated_unique_award_id": "p2",
    "piid": "ABCD_PARENT_2",
    "fpds_agency_id": "7890",
    "parent_award_piid": "ABCD_TOP_PARENT_1",
    "fpds_parent_agency_id": "3333",
    "total_obligation": 0,
    "base_and_all_options_value": 0,
    "base_exercised_options_val": 0,
    "type": "IDV_A",
}

p3 = {
    "generated_unique_award_id": "p2",
    "piid": "ABCD_PARENT_3",
    "fpds_agency_id": "7890",
    "parent_award_piid": "ABCD_TOP_PARENT_2",
    "fpds_parent_agency_id": "3333",
    "total_obligation": 0,
    "base_and_all_options_value": 0,
    "base_exercised_options_val": 0,
    "type": "IDV_A",
}

tp1 = {
    "generated_unique_award_id": "tp1",
    "piid": "ABCD_TOP_PARENT_1",
    "fpds_agency_id": "3333",
    "parent_award_piid": "",
    "fpds_parent_agency_id": "",
    "total_obligation": 0,
    "base_and_all_options_value": 0,
    "base_exercised_options_val": 0,
    "type": "IDV_A",
}

tp2 = {
    "generated_unique_award_id": "tp2",
    "piid": "ABCD_TOP_PARENT_2",
    "fpds_agency_id": "3333",
    "parent_award_piid": "",
    "fpds_parent_agency_id": "",
    "total_obligation": 0,
    "base_and_all_options_value": 0,
    "base_exercised_options_val": 0,
    "type": "IDV_A",
}


def _set_up_db(*awards):
    award_dict = create_tree(awards)
    i = 1
    for award in award_dict:
        baker.make("search.AwardSearch", award_id=i, **award)
        i = i + 1
    call_command("restock_parent_award")


def modify_award_dict(award, updated_param_dict):
    from copy import deepcopy

    local_award = deepcopy(award)
    for key, value in updated_param_dict.items():
        local_award[key] = value
    return local_award


@pytest.mark.django_db(transaction=True)  # Must use tx, since restock_parent_award commits in SQL
def test_basic_idv_aggregation_2_level(set_up_related_award_objects):
    _set_up_db(c1, c2, c3, p1)
    parent_1 = ParentAward.objects.get(generated_unique_award_id="p1")
    assert parent_1.direct_base_exercised_options_val == 9000
    assert parent_1.direct_base_and_all_options_value == 6000
    assert parent_1.direct_total_obligation == 3000
    assert parent_1.direct_idv_count == 0
    assert parent_1.direct_contract_count == 3
    assert parent_1.rollup_base_exercised_options_val == 9000
    assert parent_1.rollup_base_and_all_options_value == 6000
    assert parent_1.rollup_total_obligation == 3000
    assert parent_1.rollup_idv_count == 0
    assert parent_1.rollup_contract_count == 3


@pytest.mark.django_db(transaction=True)  # Must use tx, since restock_parent_award commits in SQL
def test_basic_idv_aggregation_3_level(set_up_related_award_objects):
    _set_up_db(c1, c2, c3, p1, c4, p2, tp1)
    parent_1 = ParentAward.objects.get(generated_unique_award_id="p1")
    assert parent_1.direct_base_exercised_options_val == 9000
    assert parent_1.direct_base_and_all_options_value == 6000
    assert parent_1.direct_total_obligation == 3000
    assert parent_1.direct_idv_count == 0
    assert parent_1.direct_contract_count == 3
    assert parent_1.rollup_base_exercised_options_val == 9000
    assert parent_1.rollup_base_and_all_options_value == 6000
    assert parent_1.rollup_total_obligation == 3000
    assert parent_1.rollup_idv_count == 0
    assert parent_1.rollup_contract_count == 3
    top_parent_1 = ParentAward.objects.get(generated_unique_award_id="tp1")
    assert top_parent_1.rollup_base_exercised_options_val == 10000
    assert top_parent_1.rollup_base_and_all_options_value == 7000
    assert top_parent_1.rollup_total_obligation == 4000
    assert top_parent_1.rollup_idv_count == 2
    assert top_parent_1.rollup_contract_count == 4


@pytest.mark.django_db(transaction=True)  # Must use tx, since restock_parent_award commits in SQL
def test_idv_aggregation_3_level_ignore_idv_internal_values(set_up_related_award_objects):
    p1_with_internal_value = modify_award_dict(
        p1, {"total_obligation": 1000, "base_and_all_options_value": 1000, "base_exercised_options_val": 1000}
    )
    _set_up_db(c1, c2, c3, p1_with_internal_value, c4, p2, tp1)
    parent_1 = ParentAward.objects.get(generated_unique_award_id="p1")
    assert parent_1.direct_base_exercised_options_val == 9000
    assert parent_1.direct_base_and_all_options_value == 6000
    assert parent_1.direct_total_obligation == 3000
    assert parent_1.direct_idv_count == 0
    assert parent_1.direct_contract_count == 3
    assert parent_1.rollup_base_exercised_options_val == 9000
    assert parent_1.rollup_base_and_all_options_value == 6000
    assert parent_1.rollup_total_obligation == 3000
    assert parent_1.rollup_idv_count == 0
    assert parent_1.rollup_contract_count == 3
    top_parent_1 = ParentAward.objects.get(generated_unique_award_id="tp1")
    assert top_parent_1.rollup_base_exercised_options_val == 10000
    assert top_parent_1.rollup_base_and_all_options_value == 7000
    assert top_parent_1.rollup_total_obligation == 4000
    assert top_parent_1.rollup_idv_count == 2
    assert top_parent_1.rollup_contract_count == 4


@pytest.mark.django_db(transaction=True)  # Must use tx, since restock_parent_award commits in SQL
def test_idv_aggregation_3_level_self_parented_award(set_up_related_award_objects):
    p1_self_parented = modify_award_dict(p1, {"piid": "ABCD_PARENT_1"})
    _set_up_db(c1, c2, c3, p1_self_parented)
    parent_1 = ParentAward.objects.get(generated_unique_award_id="p1")
    assert parent_1.direct_base_exercised_options_val == 9000
    assert parent_1.direct_base_and_all_options_value == 6000
    assert parent_1.direct_total_obligation == 3000
    assert parent_1.direct_idv_count == 0
    assert parent_1.direct_contract_count == 3
    assert parent_1.rollup_base_exercised_options_val == 9000
    assert parent_1.rollup_base_and_all_options_value == 6000
    assert parent_1.rollup_total_obligation == 3000
    assert parent_1.rollup_idv_count == 0
    assert parent_1.rollup_contract_count == 3


@pytest.mark.django_db(transaction=True)  # Must use tx, since restock_parent_award commits in SQL
def test_idv_aggregation_3_level_circular_referencing_idvs(set_up_related_award_objects):
    child_referencing_parent = modify_award_dict(
        tp1, {"parent_award_piid": p1["piid"], "fpds_parent_agency_id": p1["fpds_agency_id"]}
    )
    _set_up_db(p1, child_referencing_parent)
    parent_1 = ParentAward.objects.get(generated_unique_award_id="tp1")
    assert parent_1.direct_base_exercised_options_val == 0
    assert parent_1.direct_base_and_all_options_value == 0
    assert parent_1.direct_total_obligation == 0
    assert parent_1.direct_idv_count == 1
    assert parent_1.direct_contract_count == 0
    assert parent_1.rollup_base_exercised_options_val == 0
    assert parent_1.rollup_base_and_all_options_value == 0
    assert parent_1.rollup_total_obligation == 0
    # This IDV has 1 "child" and 1 "grandchild": Its child, and its child's
    # child, which is itself.
    # @TODO: Maybe fix this? Edge case for sure
    assert parent_1.rollup_idv_count == 2
    assert parent_1.rollup_contract_count == 0
    parent_2 = ParentAward.objects.get(generated_unique_award_id="p1")
    assert parent_2.direct_base_exercised_options_val == 0
    assert parent_2.direct_base_and_all_options_value == 0
    assert parent_2.direct_total_obligation == 0
    assert parent_2.direct_idv_count == 1
    assert parent_2.direct_contract_count == 0
    assert parent_2.rollup_base_exercised_options_val == 0
    assert parent_2.rollup_base_and_all_options_value == 0
    assert parent_2.rollup_total_obligation == 0
    # Same as above
    assert parent_2.rollup_idv_count == 2
    assert parent_2.rollup_contract_count == 0


@pytest.mark.django_db(transaction=True)  # Must use tx, since restock_parent_award commits in SQL
def test_idv_aggregation_3_level_circular_referencing_idv_and_child(set_up_related_award_objects):
    child_referencing_parent = modify_award_dict(
        tp2, {"parent_award_piid": p3["piid"], "fpds_parent_agency_id": p3["fpds_agency_id"]}
    )
    true_child = modify_award_dict(
        p3,
        {"type": "A", "total_obligation": 1000, "base_and_all_options_value": 2000, "base_exercised_options_val": 3000},
    )
    _set_up_db(true_child, child_referencing_parent)
    parent_1 = ParentAward.objects.get(generated_unique_award_id="tp2")
    assert parent_1.direct_base_exercised_options_val == 3000
    assert parent_1.direct_base_and_all_options_value == 2000
    assert parent_1.direct_total_obligation == 1000
    assert parent_1.direct_idv_count == 0
    assert parent_1.direct_contract_count == 1
    assert parent_1.rollup_base_exercised_options_val == 3000
    assert parent_1.rollup_base_and_all_options_value == 2000
    assert parent_1.rollup_total_obligation == 1000
    assert parent_1.rollup_idv_count == 0
    assert parent_1.rollup_contract_count == 1

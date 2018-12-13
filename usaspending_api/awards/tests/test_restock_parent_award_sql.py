import pytest
from django.core.management import call_command
from model_mommy import mommy

from usaspending_api.awards.tests.data.idv_data import set_up_related_award_objects, create_tree
from usaspending_api.awards.models import ParentAward

c1 = {
    "generated_unique_award_id":"c1",
    "piid": "5678",
    "parent_award_piid": "ABCD_PARENT_1",
    "fpds_parent_agency_id":"1234",
    "total_obligation": 1000,
    "base_and_all_options_value": 2000,
    "base_exercised_options_val":3000,
    "type": "A",
    }
c2 = {
    "generated_unique_award_id":"c2",
    "piid": "7890",
    "parent_award_piid": "ABCD_PARENT_1",
    "fpds_parent_agency_id":"1234",
    "total_obligation": 1000,
    "base_and_all_options_value": 2000,
    "base_exercised_options_val":3000,
    "type": "A",
    }
c3 = {
    "generated_unique_award_id":"c3",
    "piid": "ABCD",
    "parent_award_piid": "ABCD_PARENT_1",
    "fpds_parent_agency_id":"1234",
    "total_obligation": 1000,
    "base_and_all_options_value": 2000,
    "base_exercised_options_val":3000,
    "type": "A"
    }
p1 = {
    "generated_unique_award_id":"p1",
    "piid": "ABCD_PARENT_1",
    "fpds_agency_id":"1234",
    "parent_award_piid": "",
    "fpds_parent_agency_id":"",
    "total_obligation": 0,
    "base_and_all_options_value":0,
    "base_exercised_options_val":0,
    "type": "IDV_A",
    }

c4 = {
    "generated_unique_award_id":"c4",
    "piid": "GAHDS",
    "parent_award_piid": "ABCD_PARENT_2",
    "fpds_parent_agency_id":"7890",
    "total_obligation": 1000,
    "base_and_all_options_value": 1000,
    "base_exercised_options_val":1000,
    "type": "A"
    }
p2 = {
    "generated_unique_award_id":"p2",
    "piid": "ABCD_PARENT_2",
    "fpds_agency_id":"7890",
    "parent_award_piid": "",
    "fpds_parent_agency_id":"",
    "total_obligation": 0,
    "base_and_all_options_value":0,
    "base_exercised_options_val":0,
    "type": "IDV_A",
    }

tp1 = {
    "generated_unique_award_id":"tp1",
    "piid": "ABCD_PARENT_3",
    "fpds_agency_id":"3333",
    "parent_award_piid": "",
    "fpds_parent_agency_id":"",
    "total_obligation": 0,
    "base_and_all_options_value":0,
    "base_exercised_options_val":0,
    "type": "IDV_A",
    }

generated_pk = 1

@pytest.mark.django_db()
def test_basic_idv_aggregation_2_level(client):
    set_up_related_award_objects()
    award_dict = create_tree(c1, c2, c3, p1)
    for award in award_dict:
         mommy.make('awards.Award', **award)
    call_command('restock_parent_award')
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



@pytest.mark.django_db()
def test_basic_idv_aggregation_3_level(client):
    p1['fpds_parent_agency_id'] = "3333"
    p1['parent_award_piid'] = "ABCD_PARENT_3"
    p2['fpds_parent_agency_id'] = "3333"
    p2['parent_award_piid'] = "ABCD_PARENT_3"
    set_up_related_award_objects()
    award_dict = create_tree(c1, c2, c3, p1, c4, p2, tp1)
    for award in award_dict:
         mommy.make('awards.Award', **award)
    call_command('restock_parent_award')
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




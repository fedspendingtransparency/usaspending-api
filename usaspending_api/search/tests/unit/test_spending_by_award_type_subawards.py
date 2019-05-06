import json
import pytest

from rest_framework import status
from usaspending_api.common.helpers.unit_test_helper import add_to_mock_objects
from django_mock_queries.query import MockModel


@pytest.mark.django_db
def test_spending_by_award_subawards_success(client, refresh_matviews):
    # test idv subawards search
    resp = client.post(
        '/api/v2/search/spending_by_award',
        content_type='application/json',
        data=json.dumps({
            "fields": ["Sub-Award ID"],
            "filters": {
                "award_type_codes": ["IDV_A", "IDV_B", "IDV_B_A", "IDV_B_B", "IDV_B_C", "IDV_C", "IDV_D", "IDV_E"]
            },
            "subawards": True
        }))
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_spending_by_award_subawards_fail(client, refresh_matviews):
    # test idv subawards error message
    resp = client.post(
        '/api/v2/search/spending_by_award',
        content_type='application/json',
        data=json.dumps({
            "fields": ["Sub-Award ID"],
            "filters": {
                "award_type_codes": ["06"]
            },
            "subawards": True
        }))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_spending_by_award_subawards(client, mock_matviews_qs):

    mock_model_0 = MockModel(fain="", prime_award_type="IDV_A", award_ts_vector="",
                             subaward_number="EP-W-13-028-0", award_type="procurement",
                             recipient_name="Frodo Baggins", action_date="2013-10-01",
                             amount=125000, awarding_toptier_agency_name="Environmental Protection Agency",
                             awarding_subtier_agency_name="Environmental Protection Agency", piid="EPW13028",
                             prime_recipient_name="Frodo Baggins")

    mock_model_1 = MockModel(fain="", prime_award_type="IDV_B", award_ts_vector="",
                             subaward_number="EP-W-13-028-1", award_type="procurement",
                             recipient_name="Samwise Gamgee", action_date="2013-09-01",
                             amount=102432, awarding_toptier_agency_name="Environmental Protection Agency",
                             awarding_subtier_agency_name="Environmental Protection Agency", piid="EPW13028",
                             prime_recipient_name="Samwise Gamgee")

    mock_model_2 = MockModel(fain="", prime_award_type="IDV_C", award_ts_vector="",
                             subaward_number="EP-W-13-028-2", award_type="procurement",
                             recipient_name="Legolas Greenleaf", action_date="2013-09-01",
                             amount=10, awarding_toptier_agency_name="Environmental Protection Agency",
                             awarding_subtier_agency_name="Environmental Protection Agency", piid="EPW13028",
                             prime_recipient_name="Legolas Greenleaf")

    mock_model_3 = MockModel(fain="", prime_award_type="IDV_D", award_ts_vector="",
                             subaward_number="EP-W-13-028-3", award_type="procurement",
                             recipient_name="Gandalf", action_date="2013-10-01",
                             amount=125000, awarding_toptier_agency_name="Environmental Protection Agency",
                             awarding_subtier_agency_name="Environmental Protection Agency", piid="EPW13028",
                             prime_recipient_name="Gandalf")

    mock_model_4 = MockModel(fain="", prime_award_type="IDV_E", award_ts_vector="",
                             subaward_number="EP-W-13-028-4", award_type="procurement",
                             recipient_name="Radagast", action_date="2013-10-01",
                             amount=125000, awarding_toptier_agency_name="Environmental Protection Agency",
                             awarding_subtier_agency_name="Environmental Protection Agency", piid="EPW13028",
                             prime_recipient_name="Radagast")

    mock_model_5 = MockModel(fain="", prime_award_type="IDV_B_A", award_ts_vector="",
                             subaward_number="EP-W-13-028-5", award_type="procurement",
                             recipient_name="Tom Bombadil", action_date="2013-10-01",
                             amount=125000, awarding_toptier_agency_name="Environmental Protection Agency",
                             awarding_subtier_agency_name="Environmental Protection Agency", piid="EPW13028",
                             prime_recipient_name="Tom Bombadil")

    mock_model_6 = MockModel(fain="", prime_award_type="IDV_B_B", award_ts_vector="",
                             subaward_number="EP-W-13-028-6", award_type="procurement",
                             recipient_name="Tom Bombadil", action_date="2013-10-01",
                             amount=125000, awarding_toptier_agency_name="Environmental Protection Agency",
                             awarding_subtier_agency_name="Environmental Protection Agency", piid="EPW13028",
                             prime_recipient_name="Tom Bombadil")

    mock_model_7 = MockModel(fain="", prime_award_type="IDV_B_C", award_ts_vector="",
                             subaward_number="EP-W-13-028-7", award_type="procurement",
                             recipient_name="Sauron", action_date="2013-10-01",
                             amount=125000, awarding_toptier_agency_name="Environmental Protection Agency",
                             awarding_subtier_agency_name="Environmental Protection Agency", piid="EPW13028",
                             prime_recipient_name="Sauron")

    add_to_mock_objects(mock_matviews_qs, [mock_model_0, mock_model_1, mock_model_2, mock_model_3, mock_model_4,
                        mock_model_5, mock_model_6, mock_model_7])
    resp = client.post(
        '/api/v2/search/spending_by_award',
        content_type='application/json',
        data=json.dumps({
            "filters": {
                "award_type_codes": ["IDV_A", "IDV_B", "IDV_B_A", "IDV_B_B", "IDV_B_C", "IDV_C", "IDV_D", "IDV_E"]},
            "fields": ["Sub-Award ID", "Sub-Awardee Name", "Sub-Award Date", "Sub-Award Amount", "Awarding Agency",
                       "Awarding Sub Agency", "Prime Award ID", "Prime Recipient Name"],
            "subawards": True
        }))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) == 8

    resp = client.post(
        '/api/v2/search/spending_by_award',
        content_type='application/json',
        data=json.dumps({
            "filters": {
                "award_type_codes": ["IDV_A"]},
            "fields": ["Sub-Award ID"],
            "subawards": True
        }))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) == 1

    resp = client.post(
        '/api/v2/search/spending_by_award',
        content_type='application/json',
        data=json.dumps({
            "filters": {
                "award_type_codes": ["IDV_B"]},
            "fields": ["Sub-Award ID"],
            "subawards": True
        }))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) == 1

    resp = client.post(
        '/api/v2/search/spending_by_award',
        content_type='application/json',
        data=json.dumps({
            "filters": {
                "award_type_codes": ["IDV_C"]},
            "fields": ["Sub-Award ID"],
            "subawards": True
        }))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) == 1

    resp = client.post(
        '/api/v2/search/spending_by_award',
        content_type='application/json',
        data=json.dumps({
            "filters": {
                "award_type_codes": ["IDV_D"]},
            "fields": ["Sub-Award ID"],
            "subawards": True
        }))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) == 1

    resp = client.post(
        '/api/v2/search/spending_by_award',
        content_type='application/json',
        data=json.dumps({
            "filters": {
                "award_type_codes": ["IDV_E"]},
            "fields": ["Sub-Award ID"],
            "subawards": True
        }))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) == 1

    resp = client.post(
        '/api/v2/search/spending_by_award',
        content_type='application/json',
        data=json.dumps({
            "filters": {
                "award_type_codes": ["IDV_B_A"]},
            "fields": ["Sub-Award ID"],
            "subawards": True
        }))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) == 1

    resp = client.post(
        '/api/v2/search/spending_by_award',
        content_type='application/json',
        data=json.dumps({
            "filters": {
                "award_type_codes": ["IDV_B_B"]},
            "fields": ["Sub-Award ID"],
            "subawards": True
        }))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) == 1

    resp = client.post(
        '/api/v2/search/spending_by_award',
        content_type='application/json',
        data=json.dumps({
            "filters": {
                "award_type_codes": ["IDV_B_C"]},
            "fields": ["Sub-Award ID"],
            "subawards": True
        }))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) == 1

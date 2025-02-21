import json
import pytest

from model_bakery import baker
from rest_framework import status

from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


@pytest.mark.django_db
def test_spending_by_award_subawards_success(client):
    # test idv subawards search
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "fields": ["Sub-Award ID"],
                "filters": {
                    "award_type_codes": ["IDV_A", "IDV_B", "IDV_B_A", "IDV_B_B", "IDV_B_C", "IDV_C", "IDV_D", "IDV_E"]
                },
                "subawards": True,
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_spending_by_award_subawards_fail(client):
    # test idv subawards error message
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps({"fields": ["Sub-Award ID"], "filters": {"award_type_codes": ["X"]}, "subawards": True}),
    )
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_spending_by_award_subawards(client, monkeypatch, elasticsearch_subaward_index):
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=1,
        sub_awardee_or_recipient_uniqu="DUNS A",
        prime_award_type="IDV_A",
        prime_award_group="procurement",
        action_date="2020-07-03",
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=2,
        sub_awardee_or_recipient_uniqu="DUNS B",
        prime_award_type="IDV_B",
        prime_award_group="procurement",
        action_date="2020-07-03",
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=3,
        sub_awardee_or_recipient_uniqu="DUNS C",
        prime_award_type="IDV_C",
        prime_award_group="procurement",
        action_date="2020-07-03",
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=4,
        sub_awardee_or_recipient_uniqu="DUNS D",
        prime_award_type="IDV_D",
        prime_award_group="procurement",
        action_date="2020-07-03",
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=5,
        sub_awardee_or_recipient_uniqu="DUNS E",
        prime_award_type="IDV_E",
        prime_award_group="procurement",
        action_date="2020-07-03",
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=6,
        sub_awardee_or_recipient_uniqu="DUNS B_A",
        prime_award_type="IDV_B_A",
        prime_award_group="procurement",
        action_date="2020-07-03",
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=7,
        sub_awardee_or_recipient_uniqu="DUNS B_B",
        prime_award_type="IDV_B_B",
        prime_award_group="procurement",
        action_date="2020-07-03",
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=8,
        sub_awardee_or_recipient_uniqu="DUNS B_C",
        prime_award_type="IDV_B_C",
        prime_award_group="procurement",
        action_date="2020-07-03",
    )

    baker.make("recipient.RecipientLookup", duns="DUNS A", recipient_hash="f9006d7e-fa6c-fa1c-6bc5-964fe524a941")
    baker.make("recipient.RecipientLookup", duns="DUNS B", recipient_hash="f9006d7e-fa6c-fa1c-6bc5-964fe524a942")
    baker.make("recipient.RecipientLookup", duns="DUNS C", recipient_hash="f9006d7e-fa6c-fa1c-6bc5-964fe524a943")
    baker.make("recipient.RecipientLookup", duns="DUNS D", recipient_hash="f9006d7e-fa6c-fa1c-6bc5-964fe524a944")
    baker.make("recipient.RecipientLookup", duns="DUNS E", recipient_hash="f9006d7e-fa6c-fa1c-6bc5-964fe524a945")
    baker.make("recipient.RecipientLookup", duns="DUNS B_A", recipient_hash="f9006d7e-fa6c-fa1c-6bc5-964fe524a946")
    baker.make("recipient.RecipientLookup", duns="DUNS B_B", recipient_hash="f9006d7e-fa6c-fa1c-6bc5-964fe524a947")
    baker.make("recipient.RecipientLookup", duns="DUNS B_C", recipient_hash="f9006d7e-fa6c-fa1c-6bc5-964fe524a948")

    baker.make(
        "recipient.RecipientProfile",
        recipient_unique_id="DUNS A",
        recipient_level="P",
        recipient_hash="f9006d7e-fa6c-fa1c-6bc5-964fe524a941",
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_unique_id="DUNS B",
        recipient_level="C",
        recipient_hash="f9006d7e-fa6c-fa1c-6bc5-964fe524a942",
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_unique_id="DUNS C",
        recipient_level="R",
        recipient_hash="f9006d7e-fa6c-fa1c-6bc5-964fe524a943",
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_unique_id="DUNS D",
        recipient_level="P",
        recipient_hash="f9006d7e-fa6c-fa1c-6bc5-964fe524a944",
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_unique_id="DUNS E",
        recipient_level="C",
        recipient_hash="f9006d7e-fa6c-fa1c-6bc5-964fe524a945",
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_unique_id="DUNS B_A",
        recipient_level="R",
        recipient_hash="f9006d7e-fa6c-fa1c-6bc5-964fe524a946",
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_unique_id="DUNS B_B",
        recipient_level="P",
        recipient_hash="f9006d7e-fa6c-fa1c-6bc5-964fe524a947",
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_unique_id="DUNS B_C",
        recipient_level="C",
        recipient_hash="f9006d7e-fa6c-fa1c-6bc5-964fe524a948",
    )

    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["IDV_A", "IDV_B", "IDV_B_A", "IDV_B_B", "IDV_B_C", "IDV_C", "IDV_D", "IDV_E"]
                },
                "fields": [
                    "Sub-Award ID",
                    "Sub-Awardee Name",
                    "Sub-Award Date",
                    "Sub-Award Amount",
                    "Awarding Agency",
                    "Awarding Sub Agency",
                    "Prime Award ID",
                    "Prime Recipient Name",
                ],
                "subawards": True,
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 8

    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps({"filters": {"award_type_codes": ["IDV_A"]}, "fields": ["Sub-Award ID"], "subawards": True}),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1

    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps({"filters": {"award_type_codes": ["IDV_B"]}, "fields": ["Sub-Award ID"], "subawards": True}),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1

    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps({"filters": {"award_type_codes": ["IDV_C"]}, "fields": ["Sub-Award ID"], "subawards": True}),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1

    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps({"filters": {"award_type_codes": ["IDV_D"]}, "fields": ["Sub-Award ID"], "subawards": True}),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1

    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps({"filters": {"award_type_codes": ["IDV_E"]}, "fields": ["Sub-Award ID"], "subawards": True}),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1

    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps({"filters": {"award_type_codes": ["IDV_B_A"]}, "fields": ["Sub-Award ID"], "subawards": True}),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1

    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps({"filters": {"award_type_codes": ["IDV_B_B"]}, "fields": ["Sub-Award ID"], "subawards": True}),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1

    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps({"filters": {"award_type_codes": ["IDV_B_C"]}, "fields": ["Sub-Award ID"], "subawards": True}),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1

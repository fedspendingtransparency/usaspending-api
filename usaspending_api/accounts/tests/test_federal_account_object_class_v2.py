import pytest

from model_bakery import baker
from rest_framework import status


@pytest.fixture
def financial_spending_data(db):
    # federal Account
    federal_account_1 = baker.make("accounts.FederalAccount", id=1)

    # create Object classes
    object_class_1 = baker.make(
        "references.ObjectClass",
        major_object_class="10",
        major_object_class_name="mocName1",
        object_class="111",
        object_class_name="ocName1",
    )
    object_class_2 = baker.make(
        "references.ObjectClass",
        major_object_class="20",
        major_object_class_name="mocName2",
        object_class="222",
        object_class_name="ocName2",
    )
    object_class_4 = baker.make(
        "references.ObjectClass",
        major_object_class="20",
        major_object_class_name="mocName2",
        object_class="444",
        object_class_name="ocName4",
    )
    baker.make(
        "references.ObjectClass",
        major_object_class="30",
        major_object_class_name="mocName3",
        object_class="333",
        object_class_name="ocName3",
    )

    # create TAS
    tas = baker.make("accounts.TreasuryAppropriationAccount", federal_account=federal_account_1)
    baker.make("accounts.TreasuryAppropriationAccount", federal_account=federal_account_1)

    # CREATE Financial account by program activity object class
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account=tas,
        object_class=object_class_1,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account=tas,
        object_class=object_class_2,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account=tas,
        object_class=object_class_4,
    )


@pytest.mark.django_db
def test_federal_account_object_class_endpoint(client, financial_spending_data):
    """Test the award_type endpoint."""

    resp = client.get("/api/v2/federal_accounts/1/available_object_classes")
    assert resp.status_code == status.HTTP_200_OK

    # test allows for arrays to be ordered in any way
    assert resp.data["results"][0] in [
        {"id": "10", "name": "mocName1", "minor_object_class": [{"id": "111", "name": "ocName1"}]},
        {
            "id": "20",
            "name": "mocName2",
            "minor_object_class": [{"id": "222", "name": "ocName2"}, {"id": "444", "name": "ocName4"}],
        },
    ] or resp.data["results"][0] in [
        {"id": "10", "name": "mocName1", "minor_object_class": [{"id": "111", "name": "ocName1"}]},
        {
            "id": "20",
            "name": "mocName2",
            "minor_object_class": [{"id": "444", "name": "ocName4"}, {"id": "222", "name": "ocName2"}],
        },
    ]

    # check for bad request due to missing params
    resp = client.get("/api/v2/federal_accounts/2/available_object_classes")
    assert resp.data == {"results": {}}

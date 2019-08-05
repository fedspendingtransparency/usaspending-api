from datetime import date

import pytest

from model_mommy import mommy
import json

from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass


@pytest.fixture
def account_models():
    # Add submission data
    subm_2015_1 = mommy.make("submissions.SubmissionAttributes", reporting_period_start=date(2014, 10, 1))
    subm_2015_2 = mommy.make("submissions.SubmissionAttributes", reporting_period_start=date(2015, 8, 1))
    subm_2016_1 = mommy.make("submissions.SubmissionAttributes", reporting_period_start=date(2016, 1, 1))
    subm_2016_2 = mommy.make("submissions.SubmissionAttributes", reporting_period_start=date(2016, 6, 1))

    # add object classes
    obj_clas_1 = mommy.make("references.ObjectClass", object_class=1)
    obj_clas_2 = mommy.make("references.ObjectClass", object_class=2)

    # add program activity
    prg_atvy_1 = mommy.make("references.RefProgramActivity", id=1)
    prg_atvy_2 = mommy.make("references.RefProgramActivity", id=2)

    # add tas data
    tas_1 = mommy.make("accounts.TreasuryAppropriationAccount", tas_rendering_label="ABC", _fill_optional=True)
    tas_2 = mommy.make("accounts.TreasuryAppropriationAccount", tas_rendering_label="XYZ", _fill_optional=True)

    # add file A data
    mommy.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=tas_1,
        budget_authority_unobligated_balance_brought_forward_fyb=10,
        _fill_optional=True,
        submission=subm_2015_1,
    )
    mommy.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=tas_2,
        budget_authority_unobligated_balance_brought_forward_fyb=10,
        _fill_optional=True,
        submission=subm_2015_2,
    )
    mommy.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=tas_1,
        budget_authority_unobligated_balance_brought_forward_fyb=10,
        _fill_optional=True,
        submission=subm_2016_1,
    )
    mommy.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=tas_2,
        budget_authority_unobligated_balance_brought_forward_fyb=10,
        _fill_optional=True,
        submission=subm_2016_2,
    )
    AppropriationAccountBalances.populate_final_of_fy()

    # add file B data
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        object_class=obj_clas_1,
        program_activity=prg_atvy_1,
        treasury_account=tas_1,
        obligations_undelivered_orders_unpaid_total_cpe=8000,
        _fill_optional=True,
        submission=subm_2015_1,
    )  # ignored, superseded by the next submission in the FY
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        object_class=obj_clas_2,
        program_activity=prg_atvy_2,
        treasury_account=tas_2,
        obligations_undelivered_orders_unpaid_total_cpe=1000,
        _fill_optional=True,
        submission=subm_2015_2,
    )
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        object_class=obj_clas_1,
        program_activity=prg_atvy_1,
        treasury_account=tas_1,
        obligations_undelivered_orders_unpaid_total_cpe=9000,
        _fill_optional=True,
        submission=subm_2016_1,
    )  # ignored, superseded by the next submission in the FY
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        object_class=obj_clas_2,
        program_activity=prg_atvy_2,
        treasury_account=tas_2,
        obligations_undelivered_orders_unpaid_total_cpe=2000,
        _fill_optional=True,
        submission=subm_2016_2,
    )
    FinancialAccountsByProgramActivityObjectClass.populate_final_of_fy()


@pytest.mark.django_db
def test_tas_balances_list(account_models, client):
    """
    Ensure the accounts endpoint lists the right number of entities
    """
    resp = client.get("/api/v1/tas/balances/")
    assert resp.status_code == 200
    assert len(resp.data["results"]) == 4


@pytest.mark.django_db
def test_tas_balances_total(account_models, client):
    """
    Ensure the categories aggregation counts properly
    """

    response_tas_sums = {"ABC": "20.00", "XYZ": "20.00"}

    resp = client.post(
        "/api/v1/tas/balances/total/",
        content_type="application/json",
        data=json.dumps(
            {
                "field": "budget_authority_unobligated_balance_brought_forward_fyb",
                "group": "treasury_account_identifier__tas_rendering_label",
            }
        ),
    )

    assert resp.status_code == 200
    for result in resp.data["results"]:
        assert response_tas_sums[result["item"]] == result["aggregate"]


@pytest.mark.django_db
def test_tas_categories_list(account_models, client):
    """
    Ensure the categories endpoint lists the right number of entities
    """
    resp = client.get("/api/v1/tas/categories/")
    assert resp.status_code == 200
    assert len(resp.data["results"]) == 4


@pytest.mark.django_db
def test_tas_categories_total(account_models, client):
    """
    Ensure the categories aggregation counts properly
    """

    response_prg_sums = {"1": "17000.00", "2": "3000.00"}
    response_obj_sums = {"1": "17000.00", "2": "3000.00"}
    response_tas_1_obj_sums = {"1": "17000.00", "2": "3000.00"}

    resp = client.post(
        "/api/v1/tas/categories/total/",
        content_type="application/json",
        data=json.dumps({"field": "obligations_undelivered_orders_unpaid_total_cpe", "group": "program_activity"}),
    )

    assert resp.status_code == 200
    for result in resp.data["results"]:
        assert response_prg_sums[result["item"]] == result["aggregate"]

    resp = client.post(
        "/api/v1/tas/categories/total/",
        content_type="application/json",
        data=json.dumps(
            {"field": "obligations_undelivered_orders_unpaid_total_cpe", "group": "object_class__object_class"}
        ),
    )

    assert resp.status_code == 200
    for result in resp.data["results"]:
        assert response_obj_sums[result["item"]] == result["aggregate"]

    resp = client.post(
        "/api/v1/tas/categories/total/",
        content_type="application/json",
        data=json.dumps(
            {
                "field": "obligations_undelivered_orders_unpaid_total_cpe",
                "group": "object_class__object_class",
                "filters": [{"field": "treasury_account__tas_rendering_label", "operation": "equals", "value": "ABC"}],
            }
        ),
    )

    assert resp.status_code == 200
    for result in resp.data["results"]:
        assert response_tas_1_obj_sums[result["item"]] == result["aggregate"]


@pytest.mark.django_db
def test_tas_categories_quarters_total(account_models, client):
    """
    Ensure the categories quarters aggregation counts properly
    """

    response_prg_sums = {"1": "17000.00", "2": "3000.00"}
    response_obj_sums = {"1": "17000.00", "2": "3000.00"}
    response_tas_1_obj_sums = {"1": "17000.00", "2": "3000.00"}

    resp = client.post(
        "/api/v1/tas/categories/quarters/total/",
        content_type="application/json",
        data=json.dumps({"field": "obligations_undelivered_orders_unpaid_total_cpe", "group": "program_activity"}),
    )

    assert resp.status_code == 200
    for result in resp.data["results"]:
        assert response_prg_sums[result["item"]] == result["aggregate"]

    resp = client.post(
        "/api/v1/tas/categories/quarters/total/",
        content_type="application/json",
        data=json.dumps(
            {"field": "obligations_undelivered_orders_unpaid_total_cpe", "group": "object_class__object_class"}
        ),
    )

    assert resp.status_code == 200
    for result in resp.data["results"]:
        assert response_obj_sums[result["item"]] == result["aggregate"]

    resp = client.post(
        "/api/v1/tas/categories/quarters/total/",
        content_type="application/json",
        data=json.dumps(
            {
                "field": "obligations_undelivered_orders_unpaid_total_cpe",
                "group": "object_class__object_class",
                "filters": [{"field": "treasury_account__tas_rendering_label", "operation": "equals", "value": "ABC"}],
            }
        ),
    )

    assert resp.status_code == 200
    for result in resp.data["results"]:
        assert response_tas_1_obj_sums[result["item"]] == result["aggregate"]


@pytest.mark.django_db
def test_tas_list(account_models, client):
    """
    Ensure the accounts endpoint lists the right number of entities
    """
    resp = client.get("/api/v1/tas/")
    assert resp.status_code == 200
    assert len(resp.data["results"]) == 2
    assert len(resp.data["results"]) == 2


@pytest.mark.django_db
def test_tas_categories_quarters_list(account_models, client):
    """
    Ensure the tas categories quarters endpoint is functioning
    """
    resp = client.get("/api/v1/tas/categories/quarters/")
    assert resp.status_code == 200
    assert len(resp.data["results"]) == 4


@pytest.mark.django_db
def test_tas_balances_quarters_list(account_models, client):
    """
    Ensure the tas balances quarters endpoint is functioning
    """
    resp = client.get("/api/v1/tas/balances/quarters/")
    assert resp.status_code == 200
    assert len(resp.data["results"]) == 4


@pytest.mark.django_db
def test_tas_balances_quarter_total(account_models, client):
    """
    Ensure the categories aggregation counts properly
    """

    response_tas_sums = {"ABC": "20.00", "XYZ": "20.00"}

    resp = client.post(
        "/api/v1/tas/balances/total/",
        content_type="application/json",
        data=json.dumps(
            {
                "field": "budget_authority_unobligated_balance_brought_forward_fyb",
                "group": "treasury_account_identifier__tas_rendering_label",
            }
        ),
    )

    assert resp.status_code == 200
    for result in resp.data["results"]:
        print(response_tas_sums[result["item"]] + " " + result["aggregate"])
        assert response_tas_sums[result["item"]] == result["aggregate"]

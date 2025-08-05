from datetime import date

import pytest

from model_bakery import baker
import json

from usaspending_api.accounts.models import AppropriationAccountBalances


@pytest.fixture
def account_models():
    # Add submission data
    subm_2015_1 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_period_start=date(2014, 10, 1),
        reporting_fiscal_year=2015,
        reporting_fiscal_period=10,
        reporting_fiscal_quarter=4,
        toptier_code="a",
        is_final_balances_for_fy=False,
    )
    subm_2015_2 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_period_start=date(2015, 8, 1),
        reporting_fiscal_year=2015,
        reporting_fiscal_period=8,
        reporting_fiscal_quarter=3,
        toptier_code="a",
        is_final_balances_for_fy=True,
    )
    subm_2016_1 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_period_start=date(2016, 1, 1),
        reporting_fiscal_year=2016,
        reporting_fiscal_period=1,
        reporting_fiscal_quarter=1,
        toptier_code="a",
        is_final_balances_for_fy=False,
    )
    subm_2016_2 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_period_start=date(2016, 4, 1),
        reporting_fiscal_year=2016,
        reporting_fiscal_period=4,
        reporting_fiscal_quarter=2,
        toptier_code="a",
        is_final_balances_for_fy=False,
    )
    subm_2016_3 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_period_start=date(2016, 6, 1),
        reporting_fiscal_year=2016,
        reporting_fiscal_period=6,
        reporting_fiscal_quarter=2,
        toptier_code="a",
        is_final_balances_for_fy=True,
    )

    # add object classes
    obj_clas_1 = baker.make("references.ObjectClass", object_class=1)
    obj_clas_2 = baker.make("references.ObjectClass", object_class=2)

    # add program activity
    prg_atvy_1 = baker.make("references.RefProgramActivity", id=1)
    prg_atvy_2 = baker.make("references.RefProgramActivity", id=2)

    # add tas data
    tas_1 = baker.make("accounts.TreasuryAppropriationAccount", tas_rendering_label="ABC", _fill_optional=True)
    tas_2 = baker.make("accounts.TreasuryAppropriationAccount", tas_rendering_label="XYZ", _fill_optional=True)
    tas_3 = baker.make("accounts.TreasuryAppropriationAccount", tas_rendering_label="ZZZ", _fill_optional=True)

    # add file A data
    baker.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=tas_1,
        budget_authority_unobligated_balance_brought_forward_fyb=10,
        _fill_optional=True,
        submission=subm_2015_1,
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=tas_2,
        budget_authority_unobligated_balance_brought_forward_fyb=10,
        _fill_optional=True,
        submission=subm_2015_2,
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=tas_1,
        budget_authority_unobligated_balance_brought_forward_fyb=10,
        _fill_optional=True,
        submission=subm_2016_1,
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=tas_2,
        budget_authority_unobligated_balance_brought_forward_fyb=10,
        _fill_optional=True,
        submission=subm_2016_2,
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=tas_3,
        budget_authority_unobligated_balance_brought_forward_fyb=5,
        _fill_optional=True,
        submission=subm_2016_3,
    )
    AppropriationAccountBalances.populate_final_of_fy()

    # add file B data
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        object_class=obj_clas_1,
        program_activity=prg_atvy_1,
        treasury_account=tas_1,
        obligations_undelivered_orders_unpaid_total_cpe=8000,
        _fill_optional=True,
        submission=subm_2015_1,
    )  # ignored, superseded by the next submission in the FY
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        object_class=obj_clas_2,
        program_activity=prg_atvy_2,
        treasury_account=tas_2,
        obligations_undelivered_orders_unpaid_total_cpe=1000,
        _fill_optional=True,
        submission=subm_2015_2,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        object_class=obj_clas_1,
        program_activity=prg_atvy_1,
        treasury_account=tas_1,
        obligations_undelivered_orders_unpaid_total_cpe=9000,
        _fill_optional=True,
        submission=subm_2016_1,
    )  # ignored, superseded by the next submission in the FY
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        object_class=obj_clas_2,
        program_activity=prg_atvy_2,
        treasury_account=tas_2,
        obligations_undelivered_orders_unpaid_total_cpe=2000,
        _fill_optional=True,
        submission=subm_2016_2,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        object_class=obj_clas_2,
        program_activity=prg_atvy_2,
        treasury_account=tas_3,
        obligations_undelivered_orders_unpaid_total_cpe=100,
        _fill_optional=True,
        submission=subm_2016_3,
    )


@pytest.mark.django_db
def test_tas_balances_total(account_models, client):
    """
    Ensure the categories aggregation counts properly
    """

    response_tas_sums = {"XYZ": "10.00", "ZZZ": "5.00"}

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
    assert len(resp.data["results"]) == 2
    for result in resp.data["results"]:
        assert response_tas_sums[result["item"]] == result["aggregate"]


@pytest.mark.django_db
def test_tas_categories_total(account_models, client):
    """
    Ensure the categories aggregation counts properly
    """

    response_prg_sums = {"2": "1100.00"}
    response_obj_sums = {"2": "1100.00"}
    response_tas_1_obj_sums = {"2": "100.00"}

    resp = client.post(
        "/api/v1/tas/categories/total/",
        content_type="application/json",
        data=json.dumps({"field": "obligations_undelivered_orders_unpaid_total_cpe", "group": "program_activity"}),
    )

    assert resp.status_code == 200
    assert len(resp.data["results"]) == 1
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
    assert len(resp.data["results"]) == 1
    for result in resp.data["results"]:
        assert response_obj_sums[result["item"]] == result["aggregate"]

    resp = client.post(
        "/api/v1/tas/categories/total/",
        content_type="application/json",
        data=json.dumps(
            {
                "field": "obligations_undelivered_orders_unpaid_total_cpe",
                "group": "object_class__object_class",
                "filters": [{"field": "treasury_account__tas_rendering_label", "operation": "equals", "value": "ZZZ"}],
            }
        ),
    )

    assert resp.status_code == 200
    assert len(resp.data["results"]) == 1
    for result in resp.data["results"]:
        assert response_tas_1_obj_sums[result["item"]] == result["aggregate"]


@pytest.mark.django_db
def test_tas_categories_quarters_total(account_models, client):
    """
    Ensure the categories quarters aggregation counts properly
    """

    response_prg_sums = {"1": "17000.00", "2": "1100.00"}
    response_obj_sums = {"1": "17000.00", "2": "1100.00"}
    response_tas_1_obj_sums = {"2": "100.00"}

    resp = client.post(
        "/api/v1/tas/categories/quarters/total/",
        content_type="application/json",
        data=json.dumps({"field": "obligations_undelivered_orders_unpaid_total_cpe", "group": "program_activity"}),
    )

    assert resp.status_code == 200
    assert len(resp.data["results"]) == 2
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
    assert len(resp.data["results"]) == 2
    for result in resp.data["results"]:
        assert response_obj_sums[result["item"]] == result["aggregate"]

    resp = client.post(
        "/api/v1/tas/categories/quarters/total/",
        content_type="application/json",
        data=json.dumps(
            {
                "field": "obligations_undelivered_orders_unpaid_total_cpe",
                "group": "object_class__object_class",
                "filters": [{"field": "treasury_account__tas_rendering_label", "operation": "equals", "value": "ZZZ"}],
            }
        ),
    )

    assert resp.status_code == 200
    assert len(resp.data["results"]) == 1
    for result in resp.data["results"]:
        assert response_tas_1_obj_sums[result["item"]] == result["aggregate"]


@pytest.mark.django_db
def test_tas_balances_quarter_total(account_models, client):
    """
    Ensure the categories aggregation counts properly
    """

    response_tas_sums = {"ABC": "20.00", "XYZ": "10.00", "ZZZ": "5.00"}

    resp = client.post(
        "/api/v1/tas/balances/quarters/total/",
        content_type="application/json",
        data=json.dumps(
            {
                "field": "budget_authority_unobligated_balance_brought_forward_fyb",
                "group": "treasury_account_identifier__tas_rendering_label",
            }
        ),
    )

    assert resp.status_code == 200
    assert len(resp.data["results"]) == 3
    for result in resp.data["results"]:
        print(response_tas_sums[result["item"]] + " " + result["aggregate"])
        assert response_tas_sums[result["item"]] == result["aggregate"]

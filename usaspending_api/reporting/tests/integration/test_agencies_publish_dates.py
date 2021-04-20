import pytest
from model_mommy import mommy
from rest_framework import status


url = "/api/v2/reporting/agencies/publish_dates/"


@pytest.fixture
def publish_dates_data(db):
    dabs1 = mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_reveal_date="2020-01-30 00:00:00.000000+00",
        submission_fiscal_year=2020,
        submission_fiscal_month=6,
        submission_fiscal_quarter=2,
        is_quarter=True,
    )
    dabs2 = mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_reveal_date="2020-05-02 00:00:00.000000+00",
        submission_fiscal_year=2020,
        submission_fiscal_month=7,
        submission_fiscal_quarter=3,
        is_quarter=False,
    )
    dabs3 = mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_reveal_date="2019-10-01 00:00:00.000000+00",
        submission_fiscal_year=2019,
        submission_fiscal_month=12,
        submission_fiscal_quarter=4,
        is_quarter=True,
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_reveal_date="2020-09-02 00:00:00.000000+00",
        submission_fiscal_year=2020,
        submission_fiscal_month=11,
        submission_fiscal_quarter=1,
        is_quarter=False,
    )
    tas1 = mommy.make("accounts.TreasuryAppropriationAccount")
    tas2 = mommy.make("accounts.TreasuryAppropriationAccount")
    sub1 = mommy.make(
        "submissions.SubmissionAttributes",
        toptier_code="001",
        reporting_fiscal_year=2020,
        reporting_fiscal_period=6,
        reporting_fiscal_quarter=2,
        quarter_format_flag=True,
        published_date="2020-04-30 07:46:21.419796+00",
        certified_date="2020-04-30 07:46:21.419796+00",
        submission_window=dabs1,
    )
    sub2 = mommy.make(
        "submissions.SubmissionAttributes",
        toptier_code="001",
        reporting_fiscal_year=2020,
        reporting_fiscal_period=7,
        reporting_fiscal_quarter=3,
        quarter_format_flag=False,
        published_date="2020-05-02 07:46:21.419796+00",
        certified_date="2020-05-02 07:46:21.419796+00",
        submission_window=dabs2,
    )

    mommy.make(
        "submissions.SubmissionAttributes",
        toptier_code="001",
        reporting_fiscal_year=2019,
        reporting_fiscal_period=12,
        reporting_fiscal_quarter=4,
        quarter_format_flag=True,
        published_date="2020-10-02 07:46:21.419796+00",
        certified_date="2020-10-02 07:46:21.419796+00",
        submission_window=dabs3,
    )
    mommy.make(
        "submissions.SubmissionAttributes",
        toptier_code="002",
        reporting_fiscal_year=2019,
        reporting_fiscal_period=12,
        reporting_fiscal_quarter=4,
        quarter_format_flag=True,
        published_date="2020-10-02 07:46:21.419796+00",
        certified_date="2020-10-02 07:46:21.419796+00",
        submission_window=dabs3,
    )
    mommy.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas1, submission=sub1)
    mommy.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas2, submission=sub2)
    mommy.make(
        "reporting.ReportingAgencyOverview",
        toptier_code="001",
        fiscal_year=2020,
        fiscal_period=3,
        total_budgetary_resources=100.00,
    )
    mommy.make(
        "reporting.ReportingAgencyOverview",
        toptier_code="001",
        fiscal_year=2020,
        fiscal_period=7,
        total_budgetary_resources=50.00,
    )
    mommy.make(
        "reporting.ReportingAgencyOverview",
        toptier_code="001",
        fiscal_year=2019,
        fiscal_period=12,
        total_budgetary_resources=200.00,
    )
    mommy.make(
        "reporting.ReportingAgencyOverview",
        toptier_code="002",
        fiscal_year=2019,
        fiscal_period=12,
        total_budgetary_resources=300.00,
    )

    ta1 = mommy.make("references.ToptierAgency", toptier_code="001", name="Test Agency", abbreviation="TA")
    ta2 = mommy.make("references.ToptierAgency", toptier_code="002", name="Test Agency 2", abbreviation="TA2")

    mommy.make("references.Agency", id=1, toptier_agency_id=ta1.toptier_agency_id, toptier_flag=True)
    mommy.make("references.Agency", id=2, toptier_agency_id=ta2.toptier_agency_id, toptier_flag=True)


def test_basic_success(client, publish_dates_data):
    resp = client.get(url + "?fiscal_year=2020")
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 2
    expected_results = [
        {
            "agency_name": "Test Agency",
            "abbreviation": "TA",
            "toptier_code": "001",
            "current_total_budget_authority_amount": 50.00,
            "periods": [
                {
                    "period": 2,
                    "quarter": 1,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 3,
                    "quarter": 1,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 4,
                    "quarter": 2,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 5,
                    "quarter": 2,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 6,
                    "quarter": 2,
                    "quarterly": True,
                    "submission_dates": {
                        "certification_date": "2020-04-30 " "07:46:21.419796+00",
                        "publication_date": "2020-04-30 " "07:46:21.419796+00",
                    },
                },
                {
                    "period": 7,
                    "quarter": 3,
                    "submission_dates": {
                        "publication_date": "2020-05-02 07:46:21.419796+00",
                        "certification_date": "2020-05-02 07:46:21.419796+00",
                    },
                    "quarterly": False,
                },
                {
                    "period": 8,
                    "quarter": 3,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 9,
                    "quarter": 3,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 10,
                    "quarter": 4,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 11,
                    "quarter": 4,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 12,
                    "quarter": 4,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
            ],
        },
        {
            "agency_name": "Test Agency 2",
            "abbreviation": "TA2",
            "toptier_code": "002",
            "current_total_budget_authority_amount": 0.00,
            "periods": [
                {
                    "period": 2,
                    "quarter": 1,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 3,
                    "quarter": 1,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 4,
                    "quarter": 2,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 5,
                    "quarter": 2,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 6,
                    "quarter": 2,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 7,
                    "quarter": 3,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 8,
                    "quarter": 3,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 9,
                    "quarter": 3,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 10,
                    "quarter": 4,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 11,
                    "quarter": 4,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 12,
                    "quarter": 4,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
            ],
        },
    ]
    assert response["results"] == expected_results


def test_different_agencies(client, publish_dates_data):
    resp = client.get(url + "?fiscal_year=2019")
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 2
    expected_results = [
        {
            "agency_name": "Test Agency 2",
            "abbreviation": "TA2",
            "toptier_code": "002",
            "current_total_budget_authority_amount": 300.00,
            "periods": [
                {
                    "period": 2,
                    "quarter": 1,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 3,
                    "quarter": 1,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 4,
                    "quarter": 2,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 5,
                    "quarter": 2,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 6,
                    "quarter": 2,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 7,
                    "quarter": 3,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 8,
                    "quarter": 3,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 9,
                    "quarter": 3,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 10,
                    "quarter": 4,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 11,
                    "quarter": 4,
                    "submission_dates": {"certification_date": "", "publication_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 12,
                    "quarter": 4,
                    "submission_dates": {
                        "certification_date": "2020-10-02 " "07:46:21.419796+00",
                        "publication_date": "2020-10-02 " "07:46:21.419796+00",
                    },
                    "quarterly": True,
                },
            ],
        },
        {
            "agency_name": "Test Agency",
            "abbreviation": "TA",
            "toptier_code": "001",
            "current_total_budget_authority_amount": 200.00,
            "periods": [
                {
                    "period": 2,
                    "quarter": 1,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 3,
                    "quarter": 1,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 4,
                    "quarter": 2,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 5,
                    "quarter": 2,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 6,
                    "quarter": 2,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 7,
                    "quarter": 3,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 8,
                    "quarter": 3,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 9,
                    "quarter": 3,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 10,
                    "quarter": 4,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 11,
                    "quarter": 4,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 12,
                    "quarter": 4,
                    "submission_dates": {
                        "publication_date": "2020-10-02 07:46:21.419796+00",
                        "certification_date": "2020-10-02 07:46:21.419796+00",
                    },
                    "quarterly": True,
                },
            ],
        },
    ]
    assert response["results"] == expected_results


def test_filter(client, publish_dates_data):
    expected_results = [
        {
            "agency_name": "Test Agency 2",
            "abbreviation": "TA2",
            "toptier_code": "002",
            "current_total_budget_authority_amount": 300.00,
            "periods": [
                {
                    "period": 2,
                    "quarter": 1,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 3,
                    "quarter": 1,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 4,
                    "quarter": 2,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 5,
                    "quarter": 2,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 6,
                    "quarter": 2,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 7,
                    "quarter": 3,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 8,
                    "quarter": 3,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 9,
                    "quarter": 3,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 10,
                    "quarter": 4,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 11,
                    "quarter": 4,
                    "submission_dates": {"certification_date": "", "publication_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 12,
                    "quarter": 4,
                    "submission_dates": {
                        "certification_date": "2020-10-02 " "07:46:21.419796+00",
                        "publication_date": "2020-10-02 " "07:46:21.419796+00",
                    },
                    "quarterly": True,
                },
            ],
        }
    ]
    resp = client.get(url + "?fiscal_year=2019&filter=Agency 2")
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 1
    assert response["results"] == expected_results

    resp = client.get(url + "?fiscal_year=2019&filter=a2")
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 1
    assert response["results"] == expected_results


def test_fiscal_year_required(client, publish_dates_data):
    resp = client.get(url)
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    response = resp.json()
    assert response == {"detail": "Missing value: 'fiscal_year' is a required field"}


def test_publication_date_sort(client, publish_dates_data):
    resp = client.get(url + "?fiscal_year=2019&sort=publication_date")
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    response = resp.json()
    assert response == {
        "detail": "publication_date sort param must be in the format 'publication_date,<fiscal_period>' where <fiscal_period> is in the range 2-12"
    }
    dabs5 = mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        pk=5,
        submission_reveal_date="2020-01-05 00:00:00.000000+00",
        submission_fiscal_year=2020,
        submission_fiscal_quarter=1,
        submission_fiscal_month=3,
        is_quarter=True,
    )
    dabs6 = mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        pk=6,
        submission_reveal_date="2020-01-06 00:00:00.000000+00",
        submission_fiscal_year=2020,
        submission_fiscal_quarter=1,
        submission_fiscal_month=3,
        is_quarter=False,
    )
    mommy.make(
        "submissions.SubmissionAttributes",
        submission_id=5,
        toptier_code="001",
        reporting_fiscal_year=2019,
        reporting_fiscal_period=3,
        reporting_fiscal_quarter=1,
        quarter_format_flag=True,
        published_date="2020-01-28 07:46:21.419796+00",
        certified_date="2020-01-02 07:46:21.419796+00",
        submission_window=dabs5,
    )
    mommy.make(
        "submissions.SubmissionAttributes",
        submission_id=6,
        toptier_code="002",
        reporting_fiscal_year=2019,
        reporting_fiscal_period=3,
        reporting_fiscal_quarter=1,
        quarter_format_flag=False,
        published_date="2020-01-01 07:46:21.419796+00",
        certified_date="2020-01-28 07:46:21.419796+00",
        submission_window=dabs6,
    )
    mommy.make(
        "reporting.ReportingAgencyOverview",
        toptier_code="001",
        fiscal_year=2019,
        fiscal_period=3,
        total_budgetary_resources=10.00,
    )
    mommy.make(
        "reporting.ReportingAgencyOverview",
        toptier_code="002",
        fiscal_year=2019,
        fiscal_period=3,
        total_budgetary_resources=10.00,
    )

    resp = client.get(url + "?fiscal_year=2019&sort=publication_date,3&order=desc")
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 2
    expected_results = [
        {
            "agency_name": "Test Agency",
            "abbreviation": "TA",
            "toptier_code": "001",
            "current_total_budget_authority_amount": 200.00,
            "periods": [
                {
                    "period": 2,
                    "quarter": 1,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 3,
                    "quarter": 1,
                    "submission_dates": {
                        "publication_date": "2020-01-28 07:46:21.419796+00",
                        "certification_date": "2020-01-02 07:46:21.419796+00",
                    },
                    "quarterly": True,
                },
                {
                    "period": 4,
                    "quarter": 2,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 5,
                    "quarter": 2,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 6,
                    "quarter": 2,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 7,
                    "quarter": 3,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 8,
                    "quarter": 3,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 9,
                    "quarter": 3,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 10,
                    "quarter": 4,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 11,
                    "quarter": 4,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 12,
                    "quarter": 4,
                    "submission_dates": {
                        "publication_date": "2020-10-02 07:46:21.419796+00",
                        "certification_date": "2020-10-02 07:46:21.419796+00",
                    },
                    "quarterly": True,
                },
            ],
        },
        {
            "agency_name": "Test Agency 2",
            "abbreviation": "TA2",
            "toptier_code": "002",
            "current_total_budget_authority_amount": 300.00,
            "periods": [
                {
                    "period": 2,
                    "quarter": 1,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 3,
                    "quarter": 1,
                    "submission_dates": {
                        "publication_date": "2020-01-01 07:46:21.419796+00",
                        "certification_date": "2020-01-28 07:46:21.419796+00",
                    },
                    "quarterly": False,
                },
                {
                    "period": 4,
                    "quarter": 2,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 5,
                    "quarter": 2,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 6,
                    "quarter": 2,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 7,
                    "quarter": 3,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 8,
                    "quarter": 3,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 9,
                    "quarter": 3,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 10,
                    "quarter": 4,
                    "submission_dates": {"publication_date": "", "certification_date": ""},
                    "quarterly": False,
                },
                {
                    "period": 11,
                    "quarter": 4,
                    "quarterly": False,
                    "submission_dates": {"certification_date": "", "publication_date": ""},
                },
                {
                    "period": 12,
                    "quarter": 4,
                    "submission_dates": {
                        "certification_date": "2020-10-02 " "07:46:21.419796+00",
                        "publication_date": "2020-10-02 " "07:46:21.419796+00",
                    },
                    "quarterly": True,
                },
            ],
        },
    ]
    assert response["results"] == expected_results

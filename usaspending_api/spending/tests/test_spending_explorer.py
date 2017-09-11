import json

import pytest
from model_mommy import mommy
from rest_framework import status

from usaspending_api.accounts.models import TreasuryAppropriationAccount, FederalAccount
from usaspending_api.awards.models import Award, FinancialAccountsByAwards
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models import RefProgramActivity, \
    ObjectClass, LegalEntity, ToptierAgency
from usaspending_api.submissions.models import SubmissionAttributes


@pytest.fixture
def budget_function_data(db):
    """
    *******************************************************************************************************************

    STARTING POINT - Budget Function

    DEFAULT FILTER - Fiscal Year

    FILTERS APPLIED:
        Budget Function
        Budget Sub-Function
        Federal Account
        Program Activity
        Object Class
        Recipient
        Award

    Budget Function (Top Level)
        Filters Applied: Fiscal Year
        Next Level(s): Budget Sub-Function, Federal Account, Program Activity, Object Class, Recipient, Award

    Budget Sub-Function
        Filters Applied: Fiscal Year, Budget Function
        Next Level(s): Federal Account, Program Activity, Object Class, Recipient, Award

    Federal Account
        Filters Applied: Fiscal Year, Budget Function, Budget Sub-Function
        Next Level(s): Program Activity, Object Class, Recipient, Award

    Program Activity
        Filters Applied: Fiscal Year, Budget Function, Budget Sub-Function,
                            Federal Account
        Next Level(s): Object Class, Recipient, Award

    Object Class
        Filters Applied: Fiscal Year, Budget Function, Budget Sub-Function,
                            Federal Account, Program Activity
        Next Level(s): Recipient, Award

    Recipient:
        Filters Applied: Fiscal Year, Budget Function, Budget Sub-Function,
                            Federal Account, Program Activity, Object Class
        Next Level(s): Award

    Award:
        Filters Applied: Fiscal Year, Budget Function, Budget Sub-Function,
                            Federal Account, Program Activity, Object Class, Recipient
        Next Level(s): None

    *******************************************************************************************************************

    STARTING POINT - Agency

    DEFAULT FILTER - Fiscal Year

    FILTERS APPLIED:
        Agency
        Federal Account
        Program Activity
        Object Class
        Recipient
        Award

    Agency (Top Level)
        Filters Applied: Fiscal Year
        Next Level(s): Federal Account, Program Activity, Object Class, Recipient, Award

    Federal Account
        Filters Applied: Fiscal Year, Agency
        Next Level(s): Program Activity, Object Class, Recipient, Award

    Program Activity
        Filters Applied: Fiscal Year, Agency, Federal Account
        Next Level(s): Object Class, Recipient, Award

    Object Class
        Filters Applied: Fiscal Year, Agency, Federal Account, Object Class
        Next Level(s): Agency, Recipient, Award

    Recipient:
        Filters Applied: Fiscal Year, Agency, Federal Account, Program Activity, Object Class
        Next Level(s): Award

    Award:
        Filters Applied: Fiscal Year, Agency, Federal Account, Program Activity, Object Class, Recipient
        Next Level(s): None

    *******************************************************************************************************************

    STARTING POINT - Object Class

    DEFAULT FILTER - Fiscal Year

    FILTERS APPLIED:
        Object Class
        Agency
        Federal Account
        Program Activity
        Recipient
        Award

    Object Class (Top Level)
        Filters Applied: Fiscal Year
        Next Level(s): Agency, Federal Account, Program Activity, Recipient, Award

    Agency
        Filters Applied: Fiscal Year, Object Class
        Next Level(s): Federal Account, Program Activity, Recipient, Award

    Federal Account
        Filters Applied: Fiscal Year, Object Class, Agency
        Next Level(s): Program Activity, Recipient, Award

    Program Activity
        Filters Applied: Fiscal Year, Object Class, Agency, Federal Account
        Next Level(s): Recipient, Award

    Recipient:
        Filters Applied: Fiscal Year, Object Class, Agency, Federal Account, Program Activity
        Next Level(s): Award

    Award:
        Filters Applied: Fiscal Year, Object Class, Agency, Federal Account, Program Activity, Recipient
        Next Level(s): None

    """
    fiscal_quarter = mommy.make(SubmissionAttributes, reporting_fiscal_quarter='2')

    agency1 = mommy.make(
        ToptierAgency,
        toptier_agency_id=10,
        name='Agency One',
        cgac_code='001'
    )
    agency2 = mommy.make(
        ToptierAgency,
        toptier_agency_id=20,
        name='Agency Two',
        cgac_code='002'
    )
    agency3 = mommy.make(
        ToptierAgency,
        toptier_agency_id=30,
        name='Agency Three',
        cgac_code='003'
    )

    rec1 = mommy.make(
        LegalEntity,
        legal_entity_id=1,
        recipient_name='Recipient One',
        recipient_unique_id='000000001')

    rec2 = mommy.make(
        LegalEntity,
        legal_entity_id=2,
        recipient_name='Recipient Two',
        recipient_unique_id='000000002')

    rec3 = mommy.make(
        LegalEntity,
        legal_entity_id=3,
        recipient_name='Recipient Three',
        recipient_unique_id='000000003')

    award1 = mommy.make(
        Award,
        id=1,
        recipient=rec1,
        piid='AWARD000000001',
        type_description='Award Type One'
    )
    award2 = mommy.make(
        Award,
        id=2,
        recipient=rec2,
        piid='AWARD000000002',
        type_description='Award Type Two'
    )
    award3 = mommy.make(
        Award,
        id=3,
        recipient=rec3,
        piid='AWARD000000003',
        type_description='Award Type Three'
    )

    oc1 = mommy.make(ObjectClass, major_object_class="40", major_object_class_name="Grants")
    oc2 = mommy.make(ObjectClass, major_object_class="20", major_object_class_name="Contractual Services")

    pa1 = mommy.make(
        RefProgramActivity,
        id=10000,
        program_activity_code='0010',
        program_activity_name='Program Activity One'
    )
    pa2 = mommy.make(
        RefProgramActivity,
        id=20000,
        program_activity_code='0020',
        program_activity_name='Program Activity Two'
    )

    fa1 = mommy.make(
        FederalAccount,
        agency_identifier='1000',
        main_account_code='1111',
        account_title='Federal Account One'
    )
    fa2 = mommy.make(
        FederalAccount,
        agency_identifier='2000',
        main_account_code='2222',
        account_title='Federal Account Two'
    )

    bf1 = mommy.make(
        TreasuryAppropriationAccount,
        federal_account=fa1,
        budget_function_code='100',
        budget_function_title='Budget Function One',
        budget_subfunction_code='101',
        budget_subfunction_title='Budget Sub Function One',
        awarding_toptier_agency=agency1
    )
    bf2 = mommy.make(
        TreasuryAppropriationAccount,
        federal_account=fa2,
        budget_function_code='200',
        budget_function_title='Budget Function Two',
        budget_subfunction_code='202',
        budget_subfunction_title='Budget Sub Function Two',
        awarding_toptier_agency=agency2
    )
    bf3 = mommy.make(
        TreasuryAppropriationAccount,
        federal_account=fa2,
        budget_function_code='300',
        budget_function_title='Budget Function Three',
        budget_subfunction_code='303',
        budget_subfunction_title='Budget Sub Function Three',
        awarding_toptier_agency=agency3
    )

    result1 = mommy.make(
        FinancialAccountsByProgramActivityObjectClass,
        program_activity=pa1,
        submission=fiscal_quarter,
        object_class=oc1,
        treasury_account=bf1,
        obligations_incurred_by_program_object_class_cpe=10000.00
    )

    result2 = mommy.make(
        FinancialAccountsByAwards,
        program_activity=pa2,
        submission=fiscal_quarter,
        object_class=oc2,
        treasury_account=bf2,
        award=award2,
        transaction_obligated_amount=1000.00
    )

    result3 = mommy.make(
        FinancialAccountsByAwards,
        program_activity=pa2,
        submission=fiscal_quarter,
        object_class=oc2,
        treasury_account=bf3,
        award=award3,
        transaction_obligated_amount=1000.00
    )


@pytest.mark.django_db
def test_budget_function_filter_success(client, budget_function_data):

    # Test for Budget Function Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "budget_function",
                "filters": {
                    "fy": "2017"
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Budget Sub Function Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "federal_account",
                "filters": {
                    "fy": "2017",
                    "budget_function": "050"
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Federal Account Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "federal_account",
                "filters": {
                    "fy": "2017",
                    "budget_function": "050",
                    "budget_subfunction": "053"
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Program Activity Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "program_activity",
                "filters": {
                    "fy": "2017",
                    "budget_function": "050",
                    "budget_subfunction": "053",
                    "federal_account": 2715
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Object Class Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "object_class",
                "filters": {
                    "fy": "2017",
                    "budget_function": "050",
                    "budget_subfunction": "053",
                    "federal_account": 2715,
                    "program_activity": 17863
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Recipient Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "recipient",
                "filters": {
                    "fy": "2017",
                    "budget_function": "050",
                    "budget_subfunction": "053",
                    "federal_account": 2715,
                    "program_activity": 17863,
                    "object_class": "20"
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Award Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "award",
                "filters": {
                    "fy": "2017",
                    "budget_function": "050",
                    "budget_subfunction": "053",
                    "federal_account": 2715,
                    "program_activity": 17863,
                    "object_class": "20",
                    "recipient": 13916
                }}))
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_budget_function_failure(client):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post(
        '/api/v2/search/spending_over_time/',
        content_type='application/json',
        data=json.dumps({}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_object_class_filter_success(client, budget_function_data):

    # Test for Object Class Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "object_class",
                "filters": {
                    "fy": "2017"
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Agency Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "agency",
                "filters": {
                    "fy": "2017",
                    "object_class": "20"
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Federal Account Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "federal_account",
                "filters": {
                    "fy": "2017",
                    "object_class": "20",
                    "agency": 78
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Program Activity Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "program_activity",
                "filters": {
                    "fy": "2017",
                    "object_class": "20",
                    "agency": 78,
                    "federal_account": 2358
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Recipient Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "recipient",
                "filters": {
                    "fy": "2017",
                    "object_class": "20",
                    "agency": 78,
                    "federal_account": 2358,
                    "program_activity": 15103
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Award Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "award",
                "filters": {
                    "fy": "2017",
                    "object_class": "20",
                    "agency": 78,
                    "federal_account": 2358,
                    "program_activity": 15103,
                    "recipient": 301773
                }}))
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_object_class_failure(client):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post(
        '/api/v2/search/spending_over_time/',
        content_type='application/json',
        data=json.dumps({}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_agency_filter_success(client, budget_function_data):

    # Test for Object Class Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "agency",
                "filters": {
                    "fy": "2017"
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Agency Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "federal_account",
                "filters": {
                    "fy": "2017",
                    "agency": 35
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Federal Account Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "program_activity",
                "filters": {
                    "fy": "2017",
                    "agency": 35,
                    "federal_account": 1500
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Program Activity Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "object_class",
                "filters": {
                    "fy": "2017",
                    "agency": 35,
                    "federal_account": 1500,
                    "program_activity": 12697
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Recipient Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "recipient",
                "filters": {
                    "fy": "2017",
                    "agency": 35,
                    "federal_account": 1500,
                    "program_activity": 12697,
                    "object_class": "40"
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Award Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "award",
                "filters": {
                    "fy": "2017",
                    "agency": 35,
                    "federal_account": 1500,
                    "program_activity": 12697,
                    "object_class": "40",
                    "recipient": 792917
                }
            }))
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_agency_failure(client):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post(
        '/api/v2/search/spending_over_time/',
        content_type='application/json',
        data=json.dumps({}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST

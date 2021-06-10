import pytest

from model_mommy import mommy
from rest_framework import status
from usaspending_api.common.helpers.fiscal_year_helpers import current_fiscal_year

url = "/api/v2/agency/{toptier_code}/recipients/{filter}"


recipient_agency_list = [
    {
        "id": 1,
        "fiscal_year": 2021,
        "toptier_code": "015",
        "recipient_hash": "b4ba7c9d-a682-6f9a-7bc3-edc035067702",
        "recipient_name": "DYNEGY ENERGY SERVICES (EAST), LLC",
        "recipient_amount": 168000,
    },
    {
        "id": 2,
        "fiscal_year": 2021,
        "toptier_code": "015",
        "recipient_hash": "34fc1b0c-95f3-035d-42a9-d745257b8faf",
        "recipient_name": "EMA FOODS CO., LLC",
        "recipient_amount": 21532,
    },
    {
        "id": 3,
        "fiscal_year": 2021,
        "toptier_code": "015",
        "recipient_hash": "5941fc42-967f-ce5b-2f94-97997ba30637",
        "recipient_name": "FOUR POINTS TECHNOLOGY,L.L.C.",
        "recipient_amount": 34029.61,
    },
    {
        "id": 4,
        "fiscal_year": 2021,
        "toptier_code": "015",
        "recipient_hash": "a289842c-234a-0adf-6930-b7ac4f7282f6",
        "recipient_name": "QUEST DIAGNOSTICS INCORPORATED",
        "recipient_amount": 450000,
    },
    {
        "id": 5,
        "fiscal_year": 2020,
        "toptier_code": "015",
        "recipient_hash": "5932489a-47f0-360c-80f8-ef9cc27e443f",
        "recipient_name": "ALL CLEAN WATER SOLUTIONS, LLC",
        "recipient_amount": 2300.6,
    },
    {
        "id": 6,
        "fiscal_year": 2020,
        "toptier_code": "015",
        "recipient_hash": "9468f690-7dbc-eabb-00ac-5d5566db1b4c",
        "recipient_name": "A. F. WENDLING, INC.",
        "recipient_amount": 33350,
    },
    {
        "id": 7,
        "fiscal_year": 2020,
        "toptier_code": "015",
        "recipient_hash": "02846869-66a8-d17d-0527-9ba71f4e000a",
        "recipient_name": "ADAPT PHARMA INC.",
        "recipient_amount": 1696.8,
    },
    {
        "id": 8,
        "fiscal_year": 2020,
        "toptier_code": "015",
        "recipient_hash": "16e8fb6e-cd35-e399-3add-27bee212372f",
        "recipient_name": "ALFONSO & ASSOCIATES CONSULTING, INC.",
        "recipient_amount": 58937.96,
    },
    {
        "id": 9,
        "fiscal_year": 2020,
        "toptier_code": "015",
        "recipient_hash": "d29c73cf-db68-e237-70eb-d2392fde1298",
        "recipient_name": "ALPHA SIX CORPORATION",
        "recipient_amount": 933821,
    },
    {
        "id": 10,
        "fiscal_year": 2020,
        "toptier_code": "015",
        "recipient_hash": "bf66baaf-2a72-ad0c-2def-bc87400a6911",
        "recipient_name": "AMERICAN CORRECTIONAL HEALTHCARE, INC.",
        "recipient_amount": 9900,
    },
    {
        "id": 11,
        "fiscal_year": 2020,
        "toptier_code": "015",
        "recipient_hash": "c5f87afe-f876-302d-1680-20c49c01abc7",
        "recipient_name": "AMERICAN SANITARY PRODUCTS, INC.",
        "recipient_amount": 6566,
    },
    {
        "id": 12,
        "fiscal_year": 2020,
        "toptier_code": "015",
        "recipient_hash": "7a3b7575-de02-32b6-ea5f-d5e37b27ce3d",
        "recipient_name": "AT&T MOBILITY LLC",
        "recipient_amount": 15365.79,
    },
    {
        "id": 13,
        "fiscal_year": 2020,
        "toptier_code": "015",
        "recipient_hash": "37cc9842-7c9f-bdbb-2540-e6a71e850c8e",
        "recipient_name": "BLAUER MANUFACTURING CO, INC.",
        "recipient_amount": 1965.75,
    },
    {
        "id": 14,
        "fiscal_year": 2020,
        "toptier_code": "015",
        "recipient_hash": "f5a41291-f5fe-f5cd-a21f-61fd556bf181",
        "recipient_name": "BLUE CONSTRUCTION SERVICES LLC",
        "recipient_amount": 479188,
    },
    {
        "id": 15,
        "fiscal_year": 2020,
        "toptier_code": "019",
        "recipient_hash": "4bc16929-fd89-9dac-4c4c-914381f2f65f",
        "recipient_name": "INSPECTION EXPERTS,INC.",
        "recipient_amount": 11721,
    },
    {
        "id": 16,
        "fiscal_year": 2020,
        "toptier_code": "019",
        "recipient_hash": "7e7f87d3-e58b-9434-3830-7f838baecb77",
        "recipient_name": "INSURANCE AUSTRALIA LIMITED",
        "recipient_amount": 11818.01,
    },
    {
        "id": 17,
        "fiscal_year": 2020,
        "toptier_code": "019",
        "recipient_hash": "45d708ee-157f-8ff0-235d-37f6797a104c",
        "recipient_name": "INTERIM HOMES, INC.",
        "recipient_amount": 5113080,
    },
    {
        "id": 18,
        "fiscal_year": 2020,
        "toptier_code": "019",
        "recipient_hash": "651bc003-aa61-2b35-8598-9215c6d2699e",
        "recipient_name": "IRON BOW TECHNOLOGIES, LLC",
        "recipient_amount": 30654,
    },
    {
        "id": 19,
        "fiscal_year": 2020,
        "toptier_code": "019",
        "recipient_hash": "68d7743b-8df8-2970-11bc-6c5d9f523508",
        "recipient_name": "JONES LANGLASALLE AMERICAS, INC.",
        "recipient_amount": 3465,
    },
    {
        "id": 20,
        "fiscal_year": 2020,
        "toptier_code": "019",
        "recipient_hash": "fc7c787c-0a87-88d5-b7ac-49ae545875ad",
        "recipient_name": "JORDAN EXPRESS TOURIST TRANSPORT CO.LTD./JETT",
        "recipient_amount": 1079.1,
    },
    {
        "id": 21,
        "fiscal_year": 2020,
        "toptier_code": "019",
        "recipient_hash": "8b08a942-b97d-b64a-bf68-25f436a29e27",
        "recipient_name": "KAZTEA, OYUL",
        "recipient_amount": 4860,
    },
    {
        "id": 22,
        "fiscal_year": 2020,
        "toptier_code": "019",
        "recipient_hash": "2ae6ba54-eefe-348c-e889-2c6ddc9554a8",
        "recipient_name": "KENJYA-TRUSANT GROUP, LLC, THE",
        "recipient_amount": 133961.23,
    },
]


@pytest.fixture
def recipient_agency_data():
    toptier_agency_1 = mommy.make("references.ToptierAgency", toptier_code="015", name="Agency 1", abbreviation="A1")

    toptier_agency_2 = mommy.make("references.ToptierAgency", toptier_code="019", name="Agency 2", abbreviation="A2")

    mommy.make("references.Agency", toptier_agency=toptier_agency_1, toptier_flag=True, user_selectable=True)
    mommy.make("references.Agency", toptier_agency=toptier_agency_2, toptier_flag=True, user_selectable=True)

    dabs1 = mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_fiscal_year=2020,
        submission_fiscal_month=12,
        submission_fiscal_quarter=4,
        submission_reveal_date="1999-01-01",
    )
    dabs2 = mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_fiscal_year=2021,
        submission_fiscal_month=12,
        submission_fiscal_quarter=4,
        submission_reveal_date="1999-01-01",
    )
    mommy.make("submissions.SubmissionAttributes", toptier_code=toptier_agency_1.toptier_code, submission_window=dabs1)
    mommy.make("submissions.SubmissionAttributes", toptier_code=toptier_agency_1.toptier_code, submission_window=dabs2)
    mommy.make("submissions.SubmissionAttributes", toptier_code=toptier_agency_2.toptier_code, submission_window=dabs1)

    for recipient_lookup in recipient_agency_list:
        mommy.make("recipient.RecipientAgency", **recipient_lookup)


@pytest.mark.django_db
def test_basic_success(client, monkeypatch, recipient_agency_data):
    resp = client.get(url.format(toptier_code="015", filter="?fiscal_year=2021"))
    assert resp.status_code == status.HTTP_200_OK
    expected_results = {
        "toptier_code": "015",
        "fiscal_year": 2021,
        "count": 4,
        "max": 450000.0,
        "min": 21532.0,
        "25th_percentile": 21532.0,
        "50th_percentile": 34029.61,
        "75th_percentile": 168000.0,
        "messages": [],
    }
    assert resp.json() == expected_results

    resp = client.get(url.format(toptier_code="015", filter="?fiscal_year=2020"))
    assert resp.status_code == status.HTTP_200_OK
    expected_results = {
        "toptier_code": "015",
        "fiscal_year": 2020,
        "count": 10,
        "max": 933821.0,
        "min": 1696.8,
        "25th_percentile": 2300.6,
        "50th_percentile": 9900.0,
        "75th_percentile": 58937.96,
        "messages": [],
    }
    assert resp.json() == expected_results

    resp = client.get(url.format(toptier_code="019", filter="?fiscal_year=2020"))
    assert resp.status_code == status.HTTP_200_OK
    expected_results = {
        "toptier_code": "019",
        "fiscal_year": 2020,
        "count": 8,
        "max": 5113080.0,
        "min": 1079.1,
        "25th_percentile": 30654.0,
        "50th_percentile": 3465.0,
        "75th_percentile": 11721.0,
        "messages": [],
    }
    assert resp.json() == expected_results


@pytest.mark.django_db
def test_invalid_fiscal_year(client, recipient_agency_data):
    query_params = f"?fiscal_year={current_fiscal_year() + 1}"
    resp = client.get(url.format(toptier_code="015", filter=query_params))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_invalid_agency(client, recipient_agency_data):
    resp = client.get(url.format(toptier_code="999", filter=""))
    assert resp.status_code == status.HTTP_404_NOT_FOUND

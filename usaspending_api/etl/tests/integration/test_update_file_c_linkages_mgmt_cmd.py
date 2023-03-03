# Stdlib imports

# Core Django imports
from django.core.management import call_command

# Third-party app imports
import pytest
from model_bakery import baker

# Imports from your apps
from usaspending_api.awards.models import Award, FinancialAccountsByAwards
from usaspending_api.search.models import AwardSearch


@pytest.mark.django_db
def test_update_contract_linkages_piid_with_no_parent_piid():
    """
    Test linkage management command for records with only a piid and no parent piid
    """

    models_to_mock = [
        {"model": AwardSearch, "award_id": 999, "piid": "RANDOM_PIID", "parent_award_piid": None},
        {
            "model": FinancialAccountsByAwards,
            "financial_accounts_by_awards_id": 777,
            "piid": "RANDOM_PIID",
            "parent_award_id": None,
        },
    ]

    for entry in models_to_mock:
        baker.make(entry.pop("model"), **entry)

    # use the award_search table because the award_search_temp table is not present in testing
    call_command("update_file_c_linkages", "--recalculate-linkages", "--file-d-table=award_search")

    expected_results = 999

    file_c_award = FinancialAccountsByAwards.objects.filter(financial_accounts_by_awards_id=777).first()

    assert file_c_award is not None
    assert expected_results == file_c_award.award_id


@pytest.mark.django_db
def test_update_contract_linkages_piid_with_parent_piid():
    """
    Test linkage management command for records that have both a piid and parent piid
    """

    models_to_mock = [
        {"model": AwardSearch, "award_id": 999, "piid": "RANDOM_PIID", "parent_award_piid": "RANDOM_PARENT_PIID"},
        {"model": AwardSearch, "award_id": 1999, "piid": "RANDOM_PIID_2", "parent_award_piid": None},
        {
            "model": FinancialAccountsByAwards,
            "financial_accounts_by_awards_id": 777,
            "piid": "RANDOM_PIID",
            "parent_award_id": "RANDOM_PARENT_PIID",
        },
        {
            "model": FinancialAccountsByAwards,
            "financial_accounts_by_awards_id": 1777,
            "piid": "RANDOM_PIID_2",
            "parent_award_id": None,
        },
    ]

    for entry in models_to_mock:
        baker.make(entry.pop("model"), **entry)

    # use the award_search table because the award_search_temp table is not present in testing
    call_command("update_file_c_linkages", "--recalculate-linkages", "--file-d-table=award_search")

    expected_results = {
        "award_ids": [999, 1999],
        "piids": ["RANDOM_PIID", "RANDOM_PIID_2"],
        "parent_piids": ["RANDOM_PARENT_PIID", None],
    }

    award_ids = list(FinancialAccountsByAwards.objects.order_by("award_id").values_list("award_id", flat=True))
    actual_results = {
        "award_ids": award_ids,
        "piids": list(Award.objects.filter(id__in=award_ids).order_by("id").values_list("piid", flat=True)),
        "parent_piids": list(
            Award.objects.filter(id__in=award_ids).order_by("id").values_list("parent_award_piid", flat=True)
        ),
    }

    assert expected_results == actual_results


@pytest.mark.django_db
def test_update_assistance_linkages_fain():
    """
    Test linkage management command for records that only have a fain
    """

    models_to_mock = [
        {"model": AwardSearch, "award_id": 999, "fain": "RANDOM_FAIN"},
        {"model": FinancialAccountsByAwards, "financial_accounts_by_awards_id": 777, "fain": "RANDOM_FAIN"},
    ]

    for entry in models_to_mock:
        baker.make(entry.pop("model"), **entry)

    # use the award_search table because the award_search_temp table is not present in testing
    call_command("update_file_c_linkages", "--recalculate-linkages", "--file-d-table=award_search")

    expected_results = 999

    file_c_award = FinancialAccountsByAwards.objects.filter(financial_accounts_by_awards_id=777).first()

    assert file_c_award is not None
    assert expected_results == file_c_award.award_id


@pytest.mark.django_db
def test_update_assistance_linkages_uri():
    """
    Test linkage management command for records that only have a uri
    """

    models_to_mock = [
        {"model": AwardSearch, "award_id": 999, "uri": "RANDOM_URI"},
        {"model": FinancialAccountsByAwards, "financial_accounts_by_awards_id": 777, "uri": "RANDOM_URI"},
    ]

    for entry in models_to_mock:
        baker.make(entry.pop("model"), **entry)

    # use the award_search table because the award_search_temp table is not present in testing
    call_command("update_file_c_linkages", "--recalculate-linkages", "--file-d-table=award_search")

    expected_results = 999

    file_c_award = FinancialAccountsByAwards.objects.filter(financial_accounts_by_awards_id=777).first()

    assert file_c_award is not None
    assert expected_results == file_c_award.award_id


@pytest.mark.django_db
def test_update_assistance_linkages_fain_and_uri():
    """
    Test linkage management command for records that have both a fain and uri
    """

    models_to_mock = [
        {"model": AwardSearch, "award_id": 999, "fain": "RANDOM_FAIN_999", "uri": "RANDOM_URI_999"},
        {"model": AwardSearch, "award_id": 1999, "fain": "RANDOM_FAIN_1999", "uri": "RANDOM_URI_1999"},
        {
            "model": FinancialAccountsByAwards,
            "financial_accounts_by_awards_id": 777,
            "fain": "RANDOM_FAIN_999",
            "uri": "RANDOM_URI_DNE",
        },
        {
            "model": FinancialAccountsByAwards,
            "financial_accounts_by_awards_id": 1777,
            "fain": "RANDOM_FAIN_DNE",
            "uri": "RANDOM_URI_1999",
        },
    ]

    for entry in models_to_mock:
        baker.make(entry.pop("model"), **entry)

    # use the award_search table because the award_search_temp table is not present in testing
    call_command("update_file_c_linkages", "--recalculate-linkages", "--file-d-table=award_search")

    expected_results = 999

    file_c_award_fain = FinancialAccountsByAwards.objects.filter(financial_accounts_by_awards_id=777).first()

    assert file_c_award_fain is not None
    assert expected_results == file_c_award_fain.award_id

    expected_results = 1999

    file_c_award_uri = FinancialAccountsByAwards.objects.filter(financial_accounts_by_awards_id=1777).first()

    assert file_c_award_uri is not None
    assert expected_results == file_c_award_uri.award_id

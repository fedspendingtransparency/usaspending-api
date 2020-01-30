import pytest

from model_mommy import mommy

from usaspending_api.references import helpers


@pytest.fixture
def load_agency_data(db):
    mommy.make("references.CGAC", agency_name="cgac_100", cgac_code="100")
    mommy.make("references.CGAC", agency_name="cgac_200", cgac_code="200")
    mommy.make("references.CGAC", agency_name="cgac_300", cgac_code="300")
    mommy.make("references.CGAC", agency_name="cgac_500", cgac_code="500")
    mommy.make("references.CGAC", agency_name="cgac_600", cgac_code="600")
    mommy.make("references.CGAC", agency_name="cgac_700", cgac_code="700")
    mommy.make("references.CGAC", agency_name="cgac_090", cgac_code="090")

    mommy.make("references.ToptierAgency", name="agency_400", toptier_code="400")
    mommy.make("references.ToptierAgency", name="agency_401", toptier_code="401")
    mommy.make("references.ToptierAgency", name="agency_402", toptier_code="402")
    mommy.make("references.ToptierAgency", name="agency_403", toptier_code="403")
    mommy.make("references.ToptierAgency", name="agency_404", toptier_code="404")
    mommy.make("references.ToptierAgency", name="agency_4100", toptier_code="4100")
    mommy.make("references.ToptierAgency", name="agency_4200", toptier_code="4200")
    mommy.make("references.ToptierAgency", name="agency_4300", toptier_code="4300")
    mommy.make("references.ToptierAgency", name="agency_4400", toptier_code="4400")
    mommy.make("references.ToptierAgency", name="agency_4500", toptier_code="4500")
    mommy.make("references.ToptierAgency", name="agency_4600", toptier_code="4600")
    mommy.make("references.ToptierAgency", name="agency_4090", toptier_code="4090")


def test_canonicalize_string():
    assert helpers.canonicalize_string(" Däytön\n") == "DÄYTÖN"


def test_obtaining_toptier_agencies(load_agency_data):
    assert helpers.retrive_agency_name_from_code("400") == "agency_400"
    assert helpers.retrive_agency_name_from_code("401") == "agency_401"
    assert helpers.retrive_agency_name_from_code("402") == "agency_402"
    assert helpers.retrive_agency_name_from_code("403") == "agency_403"
    assert helpers.retrive_agency_name_from_code("404") == "agency_404"
    assert helpers.retrive_agency_name_from_code("4100") == "agency_4100"
    assert helpers.retrive_agency_name_from_code("4200") == "agency_4200"
    assert helpers.retrive_agency_name_from_code("4300") == "agency_4300"
    assert helpers.retrive_agency_name_from_code("4400") == "agency_4400"
    assert helpers.retrive_agency_name_from_code("4500") == "agency_4500"
    assert helpers.retrive_agency_name_from_code("4600") == "agency_4600"
    assert helpers.retrive_agency_name_from_code("4090") == "agency_4090"


def test_obtain_agency_name_from_cgac(load_agency_data):
    assert helpers.retrive_agency_name_from_code("100") == "cgac_100"
    assert helpers.retrive_agency_name_from_code("200") == "cgac_200"
    assert helpers.retrive_agency_name_from_code("300") == "cgac_300"
    assert helpers.retrive_agency_name_from_code("500") == "cgac_500"
    assert helpers.retrive_agency_name_from_code("600") == "cgac_600"
    assert helpers.retrive_agency_name_from_code("700") == "cgac_700"
    assert helpers.retrive_agency_name_from_code("090") == "cgac_090"


def test_missing_agency(load_agency_data):
    for code in ["410", "", None, "0", "000", "0100", "90", "09", "4101", "40", "409", "X"]:
        assert helpers.retrive_agency_name_from_code(code) is None

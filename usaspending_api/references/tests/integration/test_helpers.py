import pytest

from model_bakery import baker

from usaspending_api.references import helpers


@pytest.fixture
def load_agency_data(db):
    baker.make("references.CGAC", agency_name="cgac_100", cgac_code="100")
    baker.make("references.CGAC", agency_name="cgac_200", cgac_code="200")
    baker.make("references.CGAC", agency_name="cgac_300", cgac_code="300")
    baker.make("references.CGAC", agency_name="cgac_500", cgac_code="500")
    baker.make("references.CGAC", agency_name="cgac_600", cgac_code="600")
    baker.make("references.CGAC", agency_name="cgac_700", cgac_code="700")
    baker.make("references.CGAC", agency_name="cgac_090", cgac_code="090")

    baker.make("references.FREC", agency_name="frec_400", frec_code="400")
    baker.make("references.FREC", agency_name="frec_401", frec_code="401")
    baker.make("references.FREC", agency_name="frec_402", frec_code="402")
    baker.make("references.FREC", agency_name="frec_403", frec_code="403")
    baker.make("references.FREC", agency_name="frec_404", frec_code="404")
    baker.make("references.FREC", agency_name="frec_4100", frec_code="4100")
    baker.make("references.FREC", agency_name="frec_4200", frec_code="4200")
    baker.make("references.FREC", agency_name="frec_4300", frec_code="4300")
    baker.make("references.FREC", agency_name="frec_4400", frec_code="4400")
    baker.make("references.FREC", agency_name="frec_4500", frec_code="4500")
    baker.make("references.FREC", agency_name="frec_4600", frec_code="4600")
    baker.make("references.FREC", agency_name="frec_4090", frec_code="4090")


@pytest.mark.django_db
def test_obtain_agency_name_from_frec(load_agency_data):
    assert helpers.retrive_agency_name_from_code("400") == "frec_400"
    assert helpers.retrive_agency_name_from_code("401") == "frec_401"
    assert helpers.retrive_agency_name_from_code("402") == "frec_402"
    assert helpers.retrive_agency_name_from_code("403") == "frec_403"
    assert helpers.retrive_agency_name_from_code("404") == "frec_404"
    assert helpers.retrive_agency_name_from_code("4100") == "frec_4100"
    assert helpers.retrive_agency_name_from_code("4200") == "frec_4200"
    assert helpers.retrive_agency_name_from_code("4300") == "frec_4300"
    assert helpers.retrive_agency_name_from_code("4400") == "frec_4400"
    assert helpers.retrive_agency_name_from_code("4500") == "frec_4500"
    assert helpers.retrive_agency_name_from_code("4600") == "frec_4600"
    assert helpers.retrive_agency_name_from_code("4090") == "frec_4090"


@pytest.mark.django_db
def test_obtain_agency_name_from_cgac(load_agency_data):
    assert helpers.retrive_agency_name_from_code("100") == "cgac_100"
    assert helpers.retrive_agency_name_from_code("200") == "cgac_200"
    assert helpers.retrive_agency_name_from_code("300") == "cgac_300"
    assert helpers.retrive_agency_name_from_code("500") == "cgac_500"
    assert helpers.retrive_agency_name_from_code("600") == "cgac_600"
    assert helpers.retrive_agency_name_from_code("700") == "cgac_700"
    assert helpers.retrive_agency_name_from_code("090") == "cgac_090"


@pytest.mark.django_db
def test_missing_agency(load_agency_data):
    for code in ["410", "", None, "0", "000", "0100", "90", "09", "4101", "40", "409", "X"]:
        assert helpers.retrive_agency_name_from_code(code) is None

from model_bakery import baker
import pytest


@pytest.fixture
def no_data(db):
    # lets tests use database
    pass


@pytest.fixture
def basic_rnd(db):
    _psc(db, rnd_tier_two())
    _psc(db, rnd_tier_three_setup())
    _psc(db, rnd_tier_four())


@pytest.fixture
def basic_product(db):
    _psc(db, product_tier_two())
    _psc(db, product_tier_three())


@pytest.fixture
def product_with_count_above_one(db):
    _psc(db, product_tier_two())
    _psc(db, product_tier_three())
    _psc(db, product_other_tier_three("10"))


@pytest.fixture
def product_with_branching_count_above_one(db):
    _psc(db, product_tier_two())
    _psc(db, product_other_tier_two())
    _psc(db, product_tier_three())
    _psc(db, product_other_tier_three("11"))


@pytest.fixture
def basic_service(db):
    _psc(db, service_tier_two())
    _psc(db, service_tier_three())
    _psc(db, service_tier_four())


@pytest.fixture
def rnd_special(db):
    _psc(db, rnd_tier_two_special())
    _psc(db, rnd_tier_four_special())


def toptiers():
    return [
        {"id": "Research and Development", "description": "", "ancestors": [], "count": 1, "children": None},
        {"id": "Research and Development", "description": "", "ancestors": [], "count": 1, "children": None},
    ]


def rnd_tier_two():
    return {
        "id": "AA",
        "description": "tier two R&D",
        "ancestors": ["Research and Development"],
        "count": 2,
        "children": None,
    }


def rnd_tier_three_setup():
    return {
        "id": "AA90",
        "description": "tier three R&D",
        "ancestors": ["Research and Development", "AA"],
        "count": 1,
        "children": None,
    }


def rnd_tier_three():
    return {
        "id": "AA9",
        "description": "tier three R&D",
        "ancestors": ["Research and Development", "AA"],
        "count": 1,
        "children": None,
    }


def rnd_tier_four():
    return {
        "id": "AA91",
        "description": "tier four R&D",
        "ancestors": ["Research and Development", "AA", "AA9"],
        "count": 0,
        "children": None,
    }


def rnd_tier_two_special():
    return {
        "id": "AU",
        "description": "tier two R&D",
        "ancestors": ["Research and Development"],
        "count": 1,
        "children": None,
    }


def rnd_tier_four_special():
    return {
        "id": "AU11",
        "description": "R&D-TRANS OF HAZARD MAT",
        "ancestors": ["Research and Development", "AU"],
        "count": 0,
        "children": None,
    }


def product_tier_two():
    return {"id": "10", "description": "tier two Product", "ancestors": ["Product"], "count": 1, "children": None}


def product_other_tier_two():
    return {"id": "11", "description": "tier two Product 2", "ancestors": ["Product"], "count": 1, "children": None}


def product_tier_three():
    return {
        "id": "1000",
        "description": "tier three Product",
        "ancestors": ["Product", "10"],
        "count": 0,
        "children": None,
    }


def product_other_tier_three(parent):
    return {
        "id": "1010",
        "description": "tier three Product 2",
        "ancestors": ["Product", parent],
        "count": 0,
        "children": None,
    }


def service_tier_two():
    return {"id": "B", "description": "tier two Service", "ancestors": ["Service"], "count": 1, "children": None}


def service_tier_three():
    return {
        "id": "B5",
        "description": "tier three Service",
        "ancestors": ["Service", "B"],
        "count": 1,
        "children": None,
    }


def service_tier_four():
    return {
        "id": "B516",
        "description": "tier four Service",
        "ancestors": ["Service", "B", "B5"],
        "count": 0,
        "children": None,
    }


def _psc(db, dictionary):
    baker.make(
        "references.PSC", code=dictionary["id"], length=len(dictionary["id"]), description=dictionary["description"]
    )

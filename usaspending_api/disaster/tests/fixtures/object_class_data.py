from decimal import Decimal

import pytest
from model_bakery import baker


@pytest.fixture
def basic_faba_with_object_class():
    baker.make("references.DisasterEmergencyFundCode", code="A", public_law="Public law for A")
    baker.make("references.DisasterEmergencyFundCode", code="M", public_law="Public law for M")
    baker.make("references.DisasterEmergencyFundCode", code="N", public_law="Public law for N")
    baker.make("references.DisasterEmergencyFundCode", code="O", public_law="Public law for O")

    baker.make(
        "disaster.CovidFABASpending",
        spending_level="object_class",
        funding_major_object_class_id="001",
        funding_major_object_class_code="001",
        funding_major_object_class_name="001 name",
        funding_object_class_id=1,
        funding_object_class_code="0001",
        funding_object_class_name="0001 name",
        defc="M",
        award_type="A",
        award_count=1,
        obligation_sum=Decimal(1.0),
        outlay_sum=Decimal(0.0),
    )
    baker.make(
        "disaster.CovidFABASpending",
        spending_level="object_class",
        funding_major_object_class_id="002",
        funding_major_object_class_code="002",
        funding_major_object_class_name="002 name",
        funding_object_class_id=2,
        funding_object_class_code="0002",
        funding_object_class_name="0002 name",
        defc="N",
        award_type="B",
        award_count=2,
        obligation_sum=Decimal(1.0),
        outlay_sum=Decimal(0.0),
    )
    baker.make(
        "disaster.CovidFABASpending",
        spending_level="object_class",
        funding_major_object_class_id="003",
        funding_major_object_class_code="003",
        funding_major_object_class_name="003 name",
        funding_object_class_id=3,
        funding_object_class_code="00031",
        funding_object_class_name="00031 name",
        defc="O",
        award_type="B",
        award_count=2,
        obligation_sum=Decimal(1.0),
        outlay_sum=Decimal(10.0),
    )
    baker.make(
        "disaster.CovidFABASpending",
        spending_level="object_class",
        funding_major_object_class_id="003",
        funding_major_object_class_code="003",
        funding_major_object_class_name="003 name",
        funding_object_class_id=4,
        funding_object_class_code="00032",
        funding_object_class_name="00032 name",
        defc="O",
        award_type="C",
        award_count=3,
        obligation_sum=Decimal(1.0),
        outlay_sum=Decimal(20.0),
    )
    baker.make(
        "disaster.CovidFABASpending",
        spending_level="object_class",
        funding_major_object_class_id="003",
        funding_major_object_class_code="003",
        funding_major_object_class_name="003 name",
        funding_object_class_id=5,
        funding_object_class_code="00033",
        funding_object_class_name="00033 name",
        defc="O",
        award_type="D",
        award_count=2,
        obligation_sum=Decimal(1.0),
        outlay_sum=Decimal(30.0),
    )

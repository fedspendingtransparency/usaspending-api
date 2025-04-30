from decimal import Decimal

import pytest
from model_bakery import baker


@pytest.fixture
def basic_object_class_faba_with_loan_value():
    baker.make("references.DisasterEmergencyFundCode", code="A", public_law="Public law for A")
    baker.make("references.DisasterEmergencyFundCode", code="L", public_law="Public law for L")
    baker.make("references.DisasterEmergencyFundCode", code="M", public_law="Public law for M")
    baker.make("references.DisasterEmergencyFundCode", code="N", public_law="Public law for N")

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
        award_type="07",
        award_count=1,
        obligation_sum=Decimal(1.0),
        outlay_sum=Decimal(0.0),
        face_value_of_loan=Decimal(5.0),
    )

    baker.make(
        "disaster.CovidFABASpending",
        spending_level="object_class",
        funding_major_object_class_id="002",
        funding_major_object_class_code="002",
        funding_major_object_class_name="Contractual services and supplies",
        funding_object_class_id=2,
        funding_object_class_code="0002",
        funding_object_class_name="Research and development contracts",
        defc="L",
        award_type="08",
        award_count=5,
        obligation_sum=Decimal(111.0),
        outlay_sum=Decimal(111.0),
        face_value_of_loan=Decimal(555.0),
    )
    baker.make(
        "disaster.CovidFABASpending",
        spending_level="object_class",
        funding_major_object_class_id="003",
        funding_major_object_class_code="003",
        funding_major_object_class_name="Acquisition of assets",
        funding_object_class_id=3,
        funding_object_class_code="0003",
        funding_object_class_name="Land and structures",
        defc="N",
        award_type="08",
        award_count=5,
        obligation_sum=Decimal(111.0),
        outlay_sum=Decimal(111.0),
        face_value_of_loan=Decimal(555.0),
    )

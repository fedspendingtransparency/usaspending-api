from unittest.mock import MagicMock, patch

import pytest

from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.references.models import NAICS, PSC, Cfda, SubtierAgency, ToptierAgency


@pytest.mark.parametrize(
    "model_class,expected_dimensions",
    [
        (Cfda, 512),
        (NAICS, 256),
        (PSC, 256),
        (SubtierAgency, 256),
        (ToptierAgency, 256),
        (TreasuryAppropriationAccount, 256),
    ],
)
def test_embedding_dimensions(model_class, expected_dimensions):
    """Verify each model has correct embedding dimensions"""
    assert model_class.embedding_dimensions == expected_dimensions


@pytest.mark.parametrize(
    "model_class,expected_dimensions",
    [
        (Cfda, 512),
        (NAICS, 256),
        (PSC, 256),
        (SubtierAgency, 256),
        (ToptierAgency, 256),
        (TreasuryAppropriationAccount, 256),
    ],
)
def test_vector_field_dimensions_match(model_class, expected_dimensions):
    """Verify VectorField dimensions match embedding_dimensions"""
    field = model_class._meta.get_field("embedding")
    assert field.dimensions == expected_dimensions


@pytest.mark.parametrize(
    "model_class",
    [Cfda, NAICS, PSC, SubtierAgency, ToptierAgency, TreasuryAppropriationAccount],
)
def test_has_embedding_fields(model_class):
    """Verify each model has required embedding fields"""
    field_names = [f.name for f in model_class._meta.get_fields()]
    assert "embedding" in field_names
    assert "embedding_generated_at" in field_names


@pytest.mark.django_db
@pytest.mark.parametrize(
    "model_class,factory_data",
    [
        (
            Cfda,
            {
                "program_number": "10.001",
                "program_title": "Test Program",
                "objectives": "Test objectives",
            },
        ),
        (
            NAICS,
            {
                "code": "111110",
                "description": "Soybean Farming",
            },
        ),
        (
            PSC,
            {
                "code": "1234",
                "description": "Test PSC",
                "length": 4,
            },
        ),
        (
            ToptierAgency,
            {
                "toptier_code": "012",
                "name": "Test Agency",
                "abbreviation": "TA",
            },
        ),
    ],
)
@patch("usaspending_api.common.mixins.EmbeddingGenerator")
def test_generate_embedding_integration(mock_generator_class, model_class, factory_data):
    """Test full embedding generation flow for each model"""
    mock_generator = MagicMock()
    dimensions = model_class.embedding_dimensions
    mock_generator.generate_embedding.return_value = [0.1] * dimensions
    mock_generator_class.return_value = mock_generator

    instance = model_class.objects.create(**factory_data)

    assert instance.embedding is not None
    assert len(instance.embedding) == dimensions
    assert instance.embedding_generated_at is not None


# CFDA-specific tests
@pytest.mark.django_db
class TestCFDAEmbeddings:

    @pytest.mark.parametrize(
        "program_title,objectives,expected_in_text",
        [
            ("Test Program", "Test objectives", ["Test Program", "Test objectives"]),
            ("Program A", None, ["Program A"]),
            ("", "Only objectives", ["Only objectives"]),
        ],
    )
    def test_get_embedding_text_variations(self, program_title, objectives, expected_in_text):
        """Test CFDA get_embedding_text with various field combinations"""
        instance = Cfda(
            program_number="10.001",
            program_title=program_title,
            objectives=objectives,
        )

        text = instance.get_embedding_text()

        if expected_in_text:
            assert text is not None
            for expected in expected_in_text:
                assert expected in text
        else:
            assert text is None

    def test_get_embedding_text_truncation(self):
        """Test CFDA truncates long fields correctly"""
        long_objectives = "x" * 3000
        instance = Cfda(
            program_number="10.001",
            program_title="Test",
            objectives=long_objectives,
        )

        text = instance.get_embedding_text()

        # Should truncate objectives to 2000 chars + "..."
        assert len(text) < 3000
        assert "..." in text

    @pytest.mark.parametrize(
        "field_name,max_length",
        [
            ("objectives", 2000),
            ("applicant_eligibility", 500),
            ("uses_and_use_restrictions", 500),
            ("examples_of_funded_projects", 500),
        ],
    )
    def test_field_truncation_limits(self, field_name, max_length):
        """Test CFDA truncates each field to correct length"""
        long_text = "x" * (max_length + 100)
        kwargs = {
            "program_number": "10.001",
            "program_title": "Test",
            field_name: long_text,
        }
        instance = Cfda(**kwargs)

        text = instance.get_embedding_text()

        # Text should contain truncated version
        assert f"{'x' * max_length}..." in text


# PSC-specific tests
@pytest.mark.django_db
class TestPSCEmbeddings:

    @pytest.mark.parametrize(
        "description,full_name,expected_format",
        [
            ("Test PSC", "Full Name", "Test PSC | (Full Name)"),
            ("Another PSC", None, "Another PSC"),
            ("PSC", "PSC", "PSC"),  # full_name same as description
        ],
    )
    def test_get_embedding_text_format(self, description, full_name, expected_format):
        """Test PSC get_embedding_text format"""
        instance = PSC(
            code="1234",
            description=description,
            full_name=full_name,
            length=4,
        )

        text = instance.get_embedding_text()

        if full_name and full_name.strip() != description.strip():
            assert description in text
            assert f"({full_name})" in text
        else:
            assert description in text

    @pytest.mark.parametrize(
        "field_name,max_length",
        [
            ("includes", 500),
            ("excludes", 300),
            ("notes", 400),
        ],
    )
    def test_psc_field_truncation(self, field_name, max_length):
        """Test PSC truncates fields correctly"""
        long_text = "x" * (max_length + 100)
        kwargs = {
            "code": "1234",
            "description": "Test",
            "length": 4,
            field_name: long_text,
        }
        instance = PSC(**kwargs)

        text = instance.get_embedding_text()

        assert "..." in text


# ToptierAgency-specific tests
@pytest.mark.django_db
class TestToptierAgencyEmbeddings:

    @pytest.mark.parametrize(
        "name,abbreviation,mission,about,expected_parts",
        [
            ("Agency Name", "AN", "Mission text", "About text", 4),
            ("Agency Name", None, "Mission text", None, 2),
            ("Agency Name", "AN", None, None, 2),
        ],
    )
    def test_get_embedding_text_parts(self, name, abbreviation, mission, about, expected_parts):
        """Test ToptierAgency includes correct number of parts"""
        instance = ToptierAgency(
            toptier_code="012",
            name=name,
            abbreviation=abbreviation,
            mission=mission,
            about_agency_data=about,
        )

        text = instance.get_embedding_text()

        assert text is not None
        parts = text.split(" | ")
        assert len(parts) == expected_parts

    def test_get_embedding_text_about_truncation(self):
        """Test ToptierAgency truncates about_agency_data to 1000 chars"""
        long_about = "x" * 1500
        instance = ToptierAgency(
            toptier_code="012",
            name="Test Agency",
            about_agency_data=long_about,
        )

        text = instance.get_embedding_text()

        assert "..." in text
        # Should have truncated about section
        assert text.count("x") <= 1003  # 1000 + "..."


# TreasuryAppropriationAccount-specific tests
@pytest.mark.django_db
class TestTreasuryAppropriationAccountEmbeddings:

    @pytest.mark.parametrize(
        "tas_label,account_title,agency_name,expected_in_text",
        [
            (
                "012-X-0001-000",
                "Test Account",
                "Test Agency",
                ["TAS 012-X-0001-000", "Test Account", "Agency: Test Agency"],
            ),
            ("012-X-0001-000", "Test Account", None, ["TAS 012-X-0001-000", "Test Account"]),
        ],
    )
    def test_get_embedding_text_format(self, tas_label, account_title, agency_name, expected_in_text):
        """Test TreasuryAppropriationAccount get_embedding_text format"""
        instance = TreasuryAppropriationAccount(
            tas_rendering_label=tas_label,
            account_title=account_title,
            reporting_agency_name=agency_name,
            agency_id="012",
            main_account_code="0001",
            sub_account_code="000",
        )

        text = instance.get_embedding_text()

        assert text is not None
        for expected in expected_in_text:
            assert expected in text

    def test_get_embedding_text_excludes_non_reporting(self):
        """Test TreasuryAppropriationAccount excludes 'Non - Reporting' entity"""
        instance = TreasuryAppropriationAccount(
            tas_rendering_label="012-X-0001-000",
            account_title="Test Account",
            fr_entity_description="Non - Reporting",
            agency_id="012",
            main_account_code="0001",
            sub_account_code="000",
        )

        text = instance.get_embedding_text()

        assert "Non - Reporting" not in text


# Edge cases across all models
@pytest.mark.django_db
@pytest.mark.parametrize(
    "model_class,minimal_data",
    [
        (Cfda, {"program_number": "10.001", "program_title": ""}),
        (PSC, {"code": "1234", "description": "", "length": 4}),
        (ToptierAgency, {"toptier_code": "012", "name": ""}),
    ],
)
def test_get_embedding_text_empty_returns_none(model_class, minimal_data):
    """Test models return None when all text fields are empty"""
    instance = model_class(**minimal_data)

    text = instance.get_embedding_text()

    assert text is None


@pytest.mark.django_db
@pytest.mark.parametrize(
    "model_class,data_with_whitespace",
    [
        (PSC, {"code": "1234", "description": "   \n\t  ", "length": 4}),
        (ToptierAgency, {"toptier_code": "012", "name": "   "}),
    ],
)
def test_get_embedding_text_whitespace_only(model_class, data_with_whitespace):
    """Test models handle whitespace-only fields correctly"""
    instance = model_class(**data_with_whitespace)

    text = instance.get_embedding_text()

    # Should either be None or not contain the whitespace
    if text:
        assert text.strip() != ""

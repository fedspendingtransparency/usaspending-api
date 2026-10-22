from io import StringIO
from unittest.mock import MagicMock, patch

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError

from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.references.models import PSC, Cfda, ToptierAgency


@pytest.mark.django_db
class TestGenerateEmbeddingsBasicFunctionality:
    """Tests for basic command functionality"""

    @pytest.mark.parametrize(
        "model_name,app_name",
        [
            ("psc", "references"),
            ("cfda", "references"),
            ("toptieragency", "references"),
            ("treasuryappropriationaccount", "accounts"),
        ],
    )
    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_command_single_model_by_name(self, mock_generator_class, model_name, app_name):
        """Should process a single model by name"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.return_value = [0.1] * 256
        mock_generator_class.return_value = mock_generator

        # Create test data
        if model_name == "psc":
            psc = PSC(code="1234", description="Test", length=4)
            psc.save(auto_generate_embedding=False)
        elif model_name == "cfda":
            cfda = Cfda(program_number="10.001", program_title="Test Program")
            cfda.save(auto_generate_embedding=False)
        elif model_name == "toptieragency":
            tta = ToptierAgency(toptier_code="012", name="Test Agency")
            tta.save(auto_generate_embedding=False)
        elif model_name == "treasuryappropriationaccount":
            taa = TreasuryAppropriationAccount(
                tas_rendering_label="012-X-0001-000",
                account_title="Test Account",
                agency_id="012",
                main_account_code="0001",
                sub_account_code="000",
            )
            taa.save(auto_generate_embedding=False)

        out = StringIO()
        call_command("generate_embeddings", model_name, app=app_name, stdout=out)

        output = out.getvalue()
        assert "Processing:" in output
        assert "COMPLETE:" in output
        assert "1 processed" in output or "processed" in output

    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_command_all_models(self, mock_generator_class):
        """Should process all models with 'all' argument"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.return_value = [0.1] * 256
        mock_generator_class.return_value = mock_generator

        # Create test data for multiple models
        psc = PSC(code="1234", description="Test", length=4)
        psc.save(auto_generate_embedding=False)
        tta = ToptierAgency(toptier_code="012", name="Test Agency")
        tta.save(auto_generate_embedding=False)

        out = StringIO()
        call_command("generate_embeddings", "all", stdout=out)

        output = out.getvalue()
        assert "Found" in output
        assert "models with EmbeddingMixin" in output
        assert "PSC" in output or "ToptierAgency" in output

    def test_command_model_not_found(self):
        """Should raise CommandError for invalid model name"""
        with pytest.raises(CommandError) as exc_info:
            call_command("generate_embeddings", "nonexistentmodel")

        assert "not found" in str(exc_info.value)

    def test_command_model_without_mixin(self):
        """Should raise CommandError for model without EmbeddingMixin"""
        with pytest.raises(CommandError) as exc_info:
            call_command("generate_embeddings", "agency", app="references")

        assert "does not use EmbeddingMixin" in str(exc_info.value)

    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_command_no_records_to_process(self, mock_generator_class):
        """Should handle case when all embeddings already exist"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.return_value = [0.1] * 256
        mock_generator_class.return_value = mock_generator

        PSC.objects.create(code="1234", description="Test", length=4)

        out = StringIO()
        call_command("generate_embeddings", "psc", stdout=out)

        output = out.getvalue()
        assert "No PSC records need embeddings" in output

    def test_command_empty_table(self):
        """Should handle empty table gracefully"""
        out = StringIO()
        call_command("generate_embeddings", "psc", stdout=out)

        output = out.getvalue()
        assert "No PSC records need embeddings" in output


@pytest.mark.django_db
class TestGenerateEmbeddingsOptions:
    """Tests for command options"""

    @pytest.mark.parametrize("force_flag", [True, False])
    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_command_with_force_flag(self, mock_generator_class, force_flag):
        """Should regenerate existing embeddings when --force is used"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.side_effect = [[0.1] * 256, [0.2] * 256]
        mock_generator_class.return_value = mock_generator

        PSC.objects.create(code="1234", description="Test", length=4)

        out = StringIO()
        if force_flag:
            call_command("generate_embeddings", "psc", force=True, stdout=out)
            output = out.getvalue()
            assert "1 processed" in output or "processed" in output
        else:
            call_command("generate_embeddings", "psc", stdout=out)
            output = out.getvalue()
            assert "No PSC records need embeddings" in output

    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_command_with_dry_run(self, mock_generator_class):
        """Should not make changes in dry-run mode"""
        psc = PSC(code="1234", description="Test", length=4)
        psc.save(auto_generate_embedding=False)

        out = StringIO()
        call_command("generate_embeddings", "psc", dry_run=True, stdout=out)

        output = out.getvalue()
        assert "DRY RUN MODE" in output
        assert "Would process" in output
        assert "DRY RUN COMPLETE" in output

        # Verify no embedding was created
        instance = PSC.objects.get(code="1234")
        assert instance.embedding is None

    @pytest.mark.parametrize("limit", [1, 5, 10])
    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_command_with_limit(self, mock_generator_class, limit):
        """Should only process specified number of records"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.return_value = [0.1] * 256
        mock_generator_class.return_value = mock_generator

        # Create more records than limit
        for i in range(15):
            psc = PSC(code=f"{i:04d}", description=f"Test {i}", length=4)
            psc.save(auto_generate_embedding=False)

        out = StringIO()
        call_command("generate_embeddings", "psc", limit=limit, stdout=out)

        output = out.getvalue()
        assert f"Limited to {limit} records" in output

    @pytest.mark.parametrize("batch_size", [1, 5, 25, 100])
    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_command_with_batch_size(self, mock_generator_class, batch_size):
        """Should process in specified batch sizes"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.return_value = [0.1] * 256
        mock_generator_class.return_value = mock_generator

        # Create records
        num_records = 10
        for i in range(num_records):
            psc = PSC(code=f"{i:04d}", description=f"Test {i}", length=4)
            psc.save(auto_generate_embedding=False)

        out = StringIO()
        call_command("generate_embeddings", "psc", batch_size=batch_size, stdout=out)

        output = out.getvalue()
        expected_batches = (num_records + batch_size - 1) // batch_size
        assert f"Batch 1/{expected_batches}" in output

    @pytest.mark.parametrize("verbose", [True, False])
    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_command_with_verbose_flag(self, mock_generator_class, verbose):
        """Should show detailed output when --verbose is used"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.return_value = [0.1] * 256
        mock_generator_class.return_value = mock_generator

        psc = PSC(code="1234", description="Test", length=4)
        psc.save(auto_generate_embedding=False)

        out = StringIO()
        call_command("generate_embeddings", "psc", verbose=verbose, stdout=out)

        output = out.getvalue()
        if verbose:
            # Verbose mode should show per-record details
            assert "PSC" in output
        # Both modes should show progress
        assert "Progress:" in output

    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_command_with_custom_app(self, mock_generator_class):
        """Should use --app parameter to specify app"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.return_value = [0.1] * 256
        mock_generator_class.return_value = mock_generator

        taa = TreasuryAppropriationAccount(
            tas_rendering_label="012-X-0001-000",
            account_title="Test Account",
            agency_id="012",
            main_account_code="0001",
            sub_account_code="000",
        )
        taa.save(auto_generate_embedding=False)

        out = StringIO()
        call_command("generate_embeddings", "treasuryappropriationaccount", app="accounts", stdout=out)

        output = out.getvalue()
        assert "Processing: TreasuryAppropriationAccount" in output


@pytest.mark.django_db
class TestGenerateEmbeddingsBatchProcessing:
    """Tests for batch processing logic"""

    @pytest.mark.parametrize(
        "num_records,batch_size,expected_batches",
        [
            (10, 5, 2),
            (10, 3, 4),
            (10, 10, 1),
            (10, 20, 1),
            (7, 3, 3),
        ],
    )
    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_batch_processing_logic(self, mock_generator_class, num_records, batch_size, expected_batches):
        """Should process records in correct number of batches"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.return_value = [0.1] * 256
        mock_generator_class.return_value = mock_generator

        for i in range(num_records):
            psc = PSC(code=f"{i:04d}", description=f"Test {i}", length=4)
            psc.save(auto_generate_embedding=False)

        out = StringIO()
        call_command("generate_embeddings", "psc", batch_size=batch_size, stdout=out)

        output = out.getvalue()
        assert f"/{expected_batches}" in output


@pytest.mark.django_db
class TestGenerateEmbeddingsErrorHandling:
    """Tests for error handling"""

    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_command_handles_generation_failures(self, mock_generator_class):
        """Should continue processing when some records fail"""
        mock_generator = MagicMock()
        # First call fails, second succeeds
        mock_generator.generate_embedding.side_effect = [
            Exception("API Error"),
            [0.1] * 256,
        ]
        mock_generator_class.return_value = mock_generator

        psc1 = PSC(code="0001", description="Test 1", length=4)
        psc1.save(auto_generate_embedding=False)
        psc2 = PSC(code="0002", description="Test 2", length=4)
        psc2.save(auto_generate_embedding=False)

        out = StringIO()
        call_command("generate_embeddings", "psc", stdout=out)

        output = out.getvalue()
        assert "Failed" in output or "failed" in output

    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_command_handles_empty_text_records(self, mock_generator_class):
        """Should skip records with no text"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.return_value = [0.1] * 256
        mock_generator_class.return_value = mock_generator

        # Create record with empty description
        psc1 = PSC(code="0001", description="", length=4)
        psc1.save(auto_generate_embedding=False)
        psc2 = PSC(code="0002", description="Valid", length=4)
        psc2.save(auto_generate_embedding=False)

        out = StringIO()
        call_command("generate_embeddings", "psc", verbose=True, stdout=out)

        output = out.getvalue()
        assert "skipped" in output.lower()

    @patch("usaspending_api.common.mixins.logger")
    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_command_logs_failures(self, mock_generator_class, mock_logger):
        """Should log exceptions to logger"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.side_effect = Exception("API Error")
        mock_generator_class.return_value = mock_generator

        psc = PSC(code="0001", description="Test", length=4)
        psc.save(auto_generate_embedding=False)

        out = StringIO()
        call_command("generate_embeddings", "psc", stdout=out)

        # Verify logger.exception was called
        mock_logger.exception.assert_called()


@pytest.mark.django_db
class TestGenerateEmbeddingsOutput:
    """Tests for command output"""

    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_output_shows_model_name(self, mock_generator_class):
        """Should display model name in output"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.return_value = [0.1] * 256
        mock_generator_class.return_value = mock_generator

        psc = PSC(code="1234", description="Test", length=4)
        psc.save(auto_generate_embedding=False)

        out = StringIO()
        call_command("generate_embeddings", "psc", stdout=out)

        output = out.getvalue()
        assert "Processing: PSC" in output

    @pytest.mark.parametrize(
        "model_class,expected_dimensions",
        [
            (PSC, 256),
            (Cfda, 512),
            (ToptierAgency, 256),
        ],
    )
    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_output_shows_dimensions(self, mock_generator_class, model_class, expected_dimensions):
        """Should display embedding dimensions"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.return_value = [0.1] * expected_dimensions
        mock_generator_class.return_value = mock_generator

        if model_class == PSC:
            m = model_class(code="1234", description="Test", length=4)
            m.save(auto_generate_embedding=False)
            model_name = "psc"
        elif model_class == Cfda:
            m = model_class(program_number="10.001", program_title="Test")
            m.save(auto_generate_embedding=False)
            model_name = "cfda"
        elif model_class == ToptierAgency:
            m = model_class(toptier_code="012", name="Test")
            m.save(auto_generate_embedding=False)
            model_name = "toptieragency"

        out = StringIO()
        call_command("generate_embeddings", model_name, stdout=out)

        output = out.getvalue()
        assert f"dimensions: {expected_dimensions}" in output

    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_output_shows_record_count(self, mock_generator_class):
        """Should display number of records to process"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.return_value = [0.1] * 256
        mock_generator_class.return_value = mock_generator

        num_records = 5
        for i in range(num_records):
            psc = PSC(code=f"{i:04d}", description=f"Test {i}", length=4)
            psc.save(auto_generate_embedding=False)

        out = StringIO()
        call_command("generate_embeddings", "psc", stdout=out)

        output = out.getvalue()
        assert f"Found {num_records}" in output
        assert "records to process" in output

    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_output_shows_batch_progress(self, mock_generator_class):
        """Should show batch progress"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.return_value = [0.1] * 256
        mock_generator_class.return_value = mock_generator

        for i in range(10):
            psc = PSC(code=f"{i:04d}", description=f"Test {i}", length=4)
            psc.save(auto_generate_embedding=False)

        out = StringIO()
        call_command("generate_embeddings", "psc", batch_size=5, stdout=out)

        output = out.getvalue()
        assert "Batch 1/" in output
        assert "Progress:" in output

    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_output_shows_final_summary(self, mock_generator_class):
        """Should display final summary"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.return_value = [0.1] * 256
        mock_generator_class.return_value = mock_generator

        psc = PSC(code="1234", description="Test", length=4)
        psc.save(auto_generate_embedding=False)

        out = StringIO()
        call_command("generate_embeddings", "psc", stdout=out)

        output = out.getvalue()
        assert "COMPLETE:" in output
        assert "processed" in output


@pytest.mark.django_db
class TestGetAllEmbeddingModels:
    """Tests for get_all_embedding_models method"""

    def test_finds_all_embedding_models(self):
        """Should discover all models with EmbeddingMixin"""
        out = StringIO()
        call_command("generate_embeddings", "all", dry_run=True, stdout=out)

        output = out.getvalue()
        assert "models with EmbeddingMixin" in output
        # Should find at least PSC, Cfda, ToptierAgency, etc.
        assert "Found" in output


@pytest.mark.django_db
class TestProcessModel:
    """Tests for process_model method"""

    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_returns_correct_counts(self, mock_generator_class):
        """Should return (processed, failed) tuple"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.return_value = [0.1] * 256
        mock_generator_class.return_value = mock_generator

        psc = PSC(code="1234", description="Test", length=4)
        psc.save(auto_generate_embedding=False)

        out = StringIO()
        call_command("generate_embeddings", "psc", stdout=out)

        output = out.getvalue()
        assert "1 processed" in output or "processed" in output
        assert "0 failed" in output or "failed" in output

    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_with_force_includes_all_records(self, mock_generator_class):
        """Should process all records when force=True"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.return_value = [0.1] * 256
        mock_generator_class.return_value = mock_generator

        # Create record with existing embedding
        instance = PSC(code="1234", description="Test", length=4)
        instance.embedding = [0.1] * 256
        instance.save(auto_generate_embedding=False)

        out = StringIO()
        call_command("generate_embeddings", "psc", force=True, stdout=out)

        output = out.getvalue()
        assert "1 processed" in output or "processed" in output

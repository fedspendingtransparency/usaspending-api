import pytest
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock, call
from django.db import connection, models

from pgvector.django import VectorField

from usaspending_api.common.mixins import EmbeddingMixin


class TestModel(EmbeddingMixin, models.Model):
    """Concrete test model for testing EmbeddingMixin"""

    name = models.CharField(max_length=100)
    description = models.TextField(blank=True, null=True)

    class Meta:
        app_label = "tests"

    def get_embedding_text(self) -> str | None:
        """Return name and description for embedding"""
        parts = []
        if self.name:
            parts.append(self.name)
        if self.description:
            parts.append(self.description)
        return " | ".join(parts) if parts else None


class TestModelWithCustomDimensions(EmbeddingMixin, models.Model):
    """Test model with custom embedding dimensions"""

    embedding_dimensions = 512
    name = models.CharField(max_length=100)
    embedding = VectorField(dimensions=512, null=True, blank=True)

    class Meta:
        app_label = "tests"

    def get_embedding_text(self) -> str | None:
        return self.name if self.name else None


class TestModelNoImplementation(EmbeddingMixin, models.Model):
    """Test model that doesn't implement get_embedding_text"""

    name = models.CharField(max_length=100)

    class Meta:
        app_label = "tests"


@pytest.fixture(scope="session")
def create_test_tables(django_db_setup, django_db_blocker):
    """Create tables for test models"""
    with django_db_blocker.unblock():
        with connection.schema_editor() as schema_editor:
            schema_editor.create_model(TestModel)
            schema_editor.create_model(TestModelWithCustomDimensions)
            schema_editor.create_model(TestModelNoImplementation)

    yield

    with django_db_blocker.unblock():
        with connection.schema_editor() as schema_editor:
            schema_editor.delete_model(TestModel)
            schema_editor.delete_model(TestModelWithCustomDimensions)
            schema_editor.delete_model(TestModelNoImplementation)


@pytest.mark.django_db
class TestEmbeddingMixinGetEmbeddingText:
    """Tests for get_embedding_text method"""

    def test_get_embedding_text_not_implemented(self):
        """Should raise NotImplementedError if not overridden"""
        instance = TestModelNoImplementation(name="Test")
        with pytest.raises(NotImplementedError) as info:
            instance.get_embedding_text()
            assert "must implement get_embedding_text" in str(info.exception)


@pytest.mark.django_db
class TestEmbeddingMixinGetEmbeddingGenerator:
    """Tests for get_embedding_generator method"""

    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_get_embedding_generator_returns_correct_dimensions_default(self, mock_generator_class):
        """Should create generator with default 256 dimensions"""
        instance = TestModel(name="Test")

        generator = instance.get_embedding_generator()

        mock_generator_class.assert_called_once_with(dimensions=256)

    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_get_embedding_generator_returns_correct_dimensions_custom(self, mock_generator_class):
        """Should create generator with custom dimensions"""
        instance = TestModelWithCustomDimensions(name="Test")

        generator = instance.get_embedding_generator()

        mock_generator_class.assert_called_once_with(dimensions=512)


@pytest.mark.django_db
class TestEmbeddingMixinGenerateEmbedding:
    """Tests for generate_embedding method"""

    @patch("usaspending_api.common.mixins.timezone.now")
    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_generate_embedding_success(self, mock_generator_class, mock_now):
        """Should successfully generate embedding and set timestamp"""

        fixed_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_now.return_value = fixed_time

        mock_generator = MagicMock()
        mock_generator.generate_embedding.return_value = [0.1] * 256
        mock_generator_class.return_value = mock_generator

        instance = TestModel(name="Test", description="Description")

        result = instance.generate_embedding()

        assert result
        assert instance.embedding == [0.1] * 256
        assert instance.embedding_generated_at == fixed_time
        mock_generator.generate_embedding.assert_called_once_with("Test | Description")

    @patch("usaspending_api.common.mixins.logger")
    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_generate_embedding_already_exists_no_force(self, mock_generator_class, mock_logger, create_test_tables):
        """Should return False and not regenerate when embedding exists and force=False"""
        instance = TestModel(name="Test")
        instance.embedding = [0.1] * 256
        instance.save(auto_generate_embedding=False)

        result = instance.generate_embedding(force=False, verbose=False)

        assert not result
        mock_generator_class.assert_not_called()
        mock_logger.debug.assert_called_once()

    @patch("usaspending_api.common.mixins.logger")
    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_generate_embedding_already_exists_no_force_verbose(
        self, mock_generator_class, mock_logger, create_test_tables
    ):
        """Should log debug message when embedding exists and verbose=True"""
        instance = TestModel(name="Test")
        instance.embedding = [0.1] * 256
        instance.save(auto_generate_embedding=False)

        result = instance.generate_embedding(force=False, verbose=True)

        assert not result
        mock_logger.debug.assert_called_once()
        assert "Embedding already exists" in mock_logger.debug.call_args[0][0]
        assert "TestModel" in mock_logger.debug.call_args[0][0]

    @patch("usaspending_api.common.mixins.timezone.now")
    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_generate_embedding_already_exists_with_force(self, mock_generator_class, mock_now, create_test_tables):
        """Should regenerate when embedding exists and force=True"""
        fixed_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_now.return_value = fixed_time

        mock_generator = MagicMock()
        mock_generator.generate_embedding.return_value = [0.2] * 256
        mock_generator_class.return_value = mock_generator

        instance = TestModel(name="Test")
        instance.embedding = [0.1] * 256
        old_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        instance.embedding_generated_at = old_time
        instance.save(auto_generate_embedding=False)

        result = instance.generate_embedding(force=True)

        assert result
        assert instance.embedding == [0.2] * 256
        assert instance.embedding_generated_at == fixed_time
        assert instance.embedding_generated_at != old_time

    @patch("usaspending_api.common.mixins.logger")
    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_generate_embedding_empty_text(self, mock_generator_class, mock_logger):
        """Should return False when get_embedding_text returns empty string"""
        instance = TestModel.objects.create(name="")

        result = instance.generate_embedding()

        assert not result
        mock_generator_class.assert_not_called()
        mock_logger.warning.call_args_list = [
            call("Embedding text is empty for: TestModel None"),
            call(f"Embedding text is empty for: TestModel {instance.pk}"),
        ]

    @patch("usaspending_api.common.mixins.logger")
    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_generate_embedding_whitespace_only_text(self, mock_generator_class, mock_logger):
        """Should return False when text is only whitespace"""
        instance = TestModel.objects.create(name="   \n\t  ")

        result = instance.generate_embedding()

        assert not result
        mock_logger.warning.call_args_list = [
            call("Embedding text is empty for: TestModel None"),
            call(f"Embedding text is empty for: TestModel {instance.pk}"),
        ]

    @patch("usaspending_api.common.mixins.timezone.now")
    @patch("usaspending_api.common.mixins.logger")
    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_generate_embedding_sets_timestamp(self, mock_generator_class, mock_logger, mock_now):
        """Should set embedding_generated_at to current time"""
        fixed_time = datetime(2024, 6, 15, 10, 30, 45, tzinfo=timezone.utc)
        mock_now.return_value = fixed_time

        mock_generator = MagicMock()
        mock_generator.generate_embedding.return_value = [0.1] * 256
        mock_generator_class.return_value = mock_generator

        instance = TestModel.objects.create(name="Test")

        assert instance.embedding_generated_at == fixed_time

    @patch("usaspending_api.common.mixins.logger")
    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_generate_embedding_exception_handling(self, mock_generator_class, mock_logger):
        """Should catch exceptions and return False"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.side_effect = Exception("API Error")
        mock_generator_class.return_value = mock_generator

        instance = TestModel.objects.create(name="Test")

        result = instance.generate_embedding()

        assert not result
        assert instance.embedding is None

        assert mock_logger.error.call_args == call(
            f"Failed to generate embedding for TestModel {instance.pk}: API Error"
        )

    @patch("usaspending_api.common.mixins.logger")
    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_generate_embedding_logs_success_verbose(self, mock_generator_class, mock_logger):
        """Should log info message on success when verbose=True"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.return_value = [0.1] * 256
        mock_generator_class.return_value = mock_generator

        instance = TestModel.objects.create(name="Test")

        result = instance.generate_embedding(force=True, verbose=True)

        assert result

        mock_logger.info.assert_called_once()
        assert "Generated embedding for" in mock_logger.info.call_args[0][0]
        assert "TestModel" in mock_logger.info.call_args[0][0]

    @patch("usaspending_api.common.mixins.logger")
    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_generate_embedding_no_log_success_not_verbose(self, mock_generator_class, mock_logger):
        """Should not log info message on success when verbose=False"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.return_value = [0.1] * 256
        mock_generator_class.return_value = mock_generator

        instance = TestModel.objects.create(name="Test")

        result = instance.generate_embedding(force=True, verbose=False)

        assert result
        mock_logger.info.assert_not_called()

    @patch("usaspending_api.common.mixins.logger")
    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_generate_embedding_logs_error_on_exception(self, mock_generator_class, mock_logger):
        """Should log error message when exception occurs"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.side_effect = ValueError("Invalid input")
        mock_generator_class.return_value = mock_generator

        instance = TestModel.objects.create(name="Test")

        result = instance.generate_embedding()

        assert not result
        assert mock_logger.error.call_args == call(
            f"Failed to generate embedding for TestModel {instance.pk}: Invalid input"
        )


@pytest.mark.django_db
class TestEmbeddingMixinHasEmbeddingProperty:
    """Tests for has_embedding property"""

    def test_has_embedding_property_true(self):
        """Should return True when embedding exists"""
        instance = TestModel.objects.create(name="Test")
        instance.embedding = [0.1] * 256
        instance.save(auto_generate_embedding=False)

        assert instance.has_embedding

    def test_has_embedding_property_false(self):
        """Should return False when embedding is None"""
        instance = TestModel.objects.create(name="Test")
        instance.embedding = None
        instance.save(auto_generate_embedding=False)

        assert not instance.has_embedding

    def test_has_embedding_property_false_on_new_instance(self):
        """Should return False for newly created instance before save"""
        instance = TestModel(name="Test")

        assert not instance.has_embedding


@pytest.mark.django_db
class TestEmbeddingMixinSave:
    """Tests for save method"""

    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_save_auto_generates_embedding_default(self, mock_generator_class):
        """Should auto-generate embedding by default"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.return_value = [0.1] * 256
        mock_generator_class.return_value = mock_generator

        instance = TestModel(name="Test")
        instance.save()

        assert instance.embedding is not None
        assert instance.embedding == [0.1] * 256

    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_save_skip_auto_generate(self, mock_generator_class):
        """Should not generate embedding when auto_generate_embedding=False"""
        instance = TestModel(name="Test")
        instance.save(auto_generate_embedding=False)

        assert instance.embedding is None
        mock_generator_class.assert_not_called()

    @patch("usaspending_api.common.mixins.logger")
    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_save_auto_generate_exception_handling(self, mock_generator_class, mock_logger):
        """Should continue save even if embedding generation fails"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.side_effect = Exception("API Error")
        mock_generator_class.return_value = mock_generator

        instance = TestModel(name="Test")
        instance.save()  # Should not raise exception

        # Instance should be saved despite embedding failure
        assert instance.pk is not None
        assert instance.embedding is None
        mock_logger.error.assert_called()
        assert "Failed to generate embedding for" in mock_logger.error.call_args[0][0]

    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_save_does_not_regenerate_existing(self, mock_generator_class):
        """Should not regenerate embedding on subsequent saves"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.return_value = [0.1] * 256
        mock_generator_class.return_value = mock_generator

        instance = TestModel(name="Test")
        instance.save()

        # Reset mock
        mock_generator_class.reset_mock()
        mock_generator.generate_embedding.reset_mock()

        # Save again
        instance.name = "Updated"
        instance.save()

        # Should not call generator again
        mock_generator_class.assert_not_called()
        mock_generator.generate_embedding.assert_not_called()

    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_save_with_force_generate_embedding(self, mock_generator_class):
        """Should regenerate embedding when force_generate_embedding=True"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.side_effect = [[0.1] * 256, [0.2] * 256]
        mock_generator_class.return_value = mock_generator

        instance = TestModel(name="Test")
        instance.save()

        original_embedding = instance.embedding

        # Save with force
        instance.name = "Updated"
        instance.save(force_generate_embedding=True)

        # Should have new embedding
        assert instance.embedding != original_embedding
        assert mock_generator.generate_embedding.call_count == 2

    @patch("usaspending_api.common.mixins.logger")
    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_save_with_verbose_flag(self, mock_generator_class, mock_logger):
        """Should pass verbose flag to generate_embedding"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.return_value = [0.1] * 256
        mock_generator_class.return_value = mock_generator

        instance = TestModel(name="Test")
        instance.save(verbose=True)

        # Should log info message
        mock_logger.info.assert_called_once()
        assert "Generated embedding for" in mock_logger.info.call_args[0][0]

    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_save_with_update_fields(self, mock_generator_class):
        """Should work with Django's update_fields parameter"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.return_value = [0.1] * 256
        mock_generator_class.return_value = mock_generator

        instance = TestModel(name="Test", description="Original")
        instance.save(auto_generate_embedding=False)

        # Update only description
        instance.description = "Updated"
        instance.save(update_fields=["description"], auto_generate_embedding=False)

        # Should not generate embedding
        mock_generator_class.assert_not_called()

    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_save_multiple_times_same_instance(self, mock_generator_class):
        """Should be idempotent - multiple saves don't regenerate"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.return_value = [0.1] * 256
        mock_generator_class.return_value = mock_generator

        instance = TestModel(name="Test")
        instance.save()
        instance.save()
        instance.save()

        # Should only generate once
        assert mock_generator.generate_embedding.call_count == 1


@pytest.mark.django_db
class TestEmbeddingMixinEdgeCases:
    """Edge case tests for EmbeddingMixin"""

    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_generate_embedding_with_null_pk(self, mock_generator_class):
        """Should handle unsaved instance (pk is None)"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.return_value = [0.1] * 256
        mock_generator_class.return_value = mock_generator

        instance = TestModel(name="Test")

        result = instance.generate_embedding()

        assert result
        assert instance.embedding is not None

    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_generate_embedding_unicode_text(self, mock_generator_class):
        """Should handle unicode characters in text"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.return_value = [0.1] * 256
        mock_generator_class.return_value = mock_generator

        instance = TestModel(name="Test 日本語 Ñoño")

        result = instance.generate_embedding()

        assert result
        mock_generator.generate_embedding.assert_called_once()
        call_args = mock_generator.generate_embedding.call_args[0][0]
        assert "日本語" in call_args
        assert "Ñoño" in call_args

    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_generate_embedding_emoji_text(self, mock_generator_class):
        """Should handle emoji in text"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.return_value = [0.1] * 256
        mock_generator_class.return_value = mock_generator

        instance = TestModel(name="Test 🚀 🎉")

        result = instance.generate_embedding()

        assert result
        call_args = mock_generator.generate_embedding.call_args[0][0]
        assert all(emoji in call_args for emoji in ["🚀", "🎉"])

    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_generate_embedding_very_long_text(self, mock_generator_class):
        """Should handle very long text"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.return_value = [0.1] * 256
        mock_generator_class.return_value = mock_generator

        long_text = "A" * 50000
        instance = TestModel(name=long_text)

        result = instance.generate_embedding()

        assert result
        mock_generator.generate_embedding.assert_called_once()

    @patch("usaspending_api.common.mixins.EmbeddingGenerator")
    def test_generate_embedding_newlines_and_tabs(self, mock_generator_class):
        """Should handle text with newlines and tabs"""
        mock_generator = MagicMock()
        mock_generator.generate_embedding.return_value = [0.1] * 256
        mock_generator_class.return_value = mock_generator

        instance = TestModel(name="Line 1\nLine 2\tTabbed")

        result = instance.generate_embedding()

        assert result
        call_args = mock_generator.generate_embedding.call_args[0][0]
        assert all(char in call_args for char in ["\n", "\t"])

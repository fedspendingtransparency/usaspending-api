import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest
from django.test import TestCase

from usaspending_api.download.helpers.cleanup_helpers import cleanup_previous_download_attempt  # Updated import
from usaspending_api.download.models import DownloadJob, JobStatus
from usaspending_api.download.models.download_job_lookup import DownloadJobLookup


@pytest.mark.django_db
class TestDownloadCleanup(TestCase):
    """Test suite for download cleanup functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.job_status = JobStatus.objects.get_or_create(
            name="ready",
            defaults={"description": "Ready for processing"}
        )[0]

        self.download_job = DownloadJob.objects.create(
            job_status=self.job_status,
            file_name="test_cleanup_download.zip",
            json_request='{"filters": {}, "download_types": ["awards"], "request_type": "award"}'
        )

    def test_cleanup_removes_orphaned_lookup_entries(self):
        """Test that cleanup removes all orphaned DownloadJobLookup entries"""

        # Create orphaned lookup entries (simulating a failed previous attempt)
        orphaned_count = 500
        lookup_entries = [
            DownloadJobLookup(
                created_at=datetime.now(timezone.utc),
                download_job_id=self.download_job.download_job_id,
                lookup_id=i,
                lookup_id_type="award_id"
            )
            for i in range(orphaned_count)
        ]
        DownloadJobLookup.objects.bulk_create(lookup_entries)

        # Verify lookups were created
        lookup_count_before = DownloadJobLookup.objects.filter(
            download_job_id=self.download_job.download_job_id
        ).count()
        assert lookup_count_before == orphaned_count, "Failed to create test lookup entries"

        # Run cleanup
        cleanup_previous_download_attempt(self.download_job)

        # Verify all lookups were removed
        lookup_count_after = DownloadJobLookup.objects.filter(
            download_job_id=self.download_job.download_job_id
        ).count()
        assert lookup_count_after == 0, f"Expected 0 lookups after cleanup, found {lookup_count_after}"

    def test_cleanup_only_removes_entries_for_specific_job(self):
        """Test that cleanup only removes entries for the target download job"""

        # Create another download job
        other_job = DownloadJob.objects.create(
            job_status=self.job_status,
            file_name="other_download.zip",
            json_request='{"filters": {}}'
        )

        # Create lookup entries for both jobs
        for i in range(100):
            DownloadJobLookup.objects.create(
                created_at=datetime.now(timezone.utc),
                download_job_id=self.download_job.download_job_id,
                lookup_id=i,
                lookup_id_type="award_id"
            )
            DownloadJobLookup.objects.create(
                created_at=datetime.now(timezone.utc),
                download_job_id=other_job.download_job_id,
                lookup_id=i + 1000,
                lookup_id_type="award_id"
            )

        # Run cleanup on first job only
        cleanup_previous_download_attempt(self.download_job)

        # Verify first job's lookups are removed
        first_job_count = DownloadJobLookup.objects.filter(
            download_job_id=self.download_job.download_job_id
        ).count()
        assert first_job_count == 0, "First job's lookups should be removed"

        # Verify other job's lookups remain
        other_job_count = DownloadJobLookup.objects.filter(
            download_job_id=other_job.download_job_id
        ).count()
        assert other_job_count == 100, "Other job's lookups should remain untouched"

    def test_cleanup_handles_missing_files_gracefully(self):
        """Test that cleanup doesn't fail when files don't exist"""

        # No files created, cleanup should run without errors
        try:
            cleanup_previous_download_attempt(self.download_job)
        except Exception as e:
            pytest.fail(f"Cleanup should handle missing files gracefully, but raised: {e}")

    @patch('usaspending_api.download.helpers.cleanup_helpers.settings')  # Updated patch path
    def test_cleanup_removes_incomplete_zip_file(self, mock_settings):
        """Test that cleanup removes incomplete zip files"""

        with tempfile.TemporaryDirectory() as temp_dir:
            mock_settings.CSV_LOCAL_PATH = temp_dir + "/"

            # Create an incomplete zip file
            zip_file_path = os.path.join(temp_dir, self.download_job.file_name)
            Path(zip_file_path).touch()

            assert os.path.exists(zip_file_path), "Test zip file should exist"

            # Run cleanup
            cleanup_previous_download_attempt(self.download_job)

            # Verify zip file was removed
            assert not os.path.exists(zip_file_path), "Incomplete zip file should be removed"

    @patch('usaspending_api.download.helpers.cleanup_helpers.settings')  # Updated patch path
    def test_cleanup_removes_incomplete_working_directory(self, mock_settings):
        """Test that cleanup removes incomplete working directories"""

        with tempfile.TemporaryDirectory() as temp_dir:
            mock_settings.CSV_LOCAL_PATH = temp_dir + "/"

            # Create an incomplete working directory with some files
            zip_file_path = os.path.join(temp_dir, self.download_job.file_name)
            working_dir = os.path.splitext(zip_file_path)[0]
            os.makedirs(working_dir, exist_ok=True)

            # Add some files to the working directory
            test_file = os.path.join(working_dir, "test_data.csv")
            Path(test_file).touch()

            assert os.path.exists(working_dir), "Test working directory should exist"
            assert os.path.exists(test_file), "Test file should exist in working directory"

            # Run cleanup
            cleanup_previous_download_attempt(self.download_job)

            # Verify working directory and contents were removed
            assert not os.path.exists(working_dir), "Working directory should be removed"
            assert not os.path.exists(test_file), "Files in working directory should be removed"

    def test_cleanup_with_large_number_of_lookups(self):
        """Test cleanup performance with large number of lookup entries"""

        # Create a large number of orphaned entries (simulating 240k duplicates mentioned in the issue)
        large_count = 10_000  # Use smaller number for tests, but validates the approach
        lookup_entries = [
            DownloadJobLookup(
                created_at=datetime.now(timezone.utc),
                download_job_id=self.download_job.download_job_id,
                lookup_id=i,
                lookup_id_type="award_id"
            )
            for i in range(large_count)
        ]
        DownloadJobLookup.objects.bulk_create(lookup_entries, batch_size=1000)

        # Run cleanup
        import time
        start_time = time.time()
        cleanup_previous_download_attempt(self.download_job)
        elapsed_time = time.time() - start_time

        # Verify cleanup completed
        lookup_count_after = DownloadJobLookup.objects.filter(
            download_job_id=self.download_job.download_job_id
        ).count()
        assert lookup_count_after == 0, "All lookups should be removed"

        # Verify reasonable performance (should complete in under 5 seconds)
        assert elapsed_time < 5.0, f"Cleanup took too long: {elapsed_time:.2f}s"
        print(f"Cleaned up {large_count} entries in {elapsed_time:.2f}s")

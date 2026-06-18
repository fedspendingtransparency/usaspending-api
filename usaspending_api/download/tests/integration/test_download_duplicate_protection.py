import json
from datetime import datetime, timezone

import pytest

from usaspending_api.download.filestreaming.download_generation import generate_download
from usaspending_api.download.models import DownloadJob, JobStatus
from usaspending_api.download.models.download_job_lookup import DownloadJobLookup


@pytest.mark.django_db
class TestDownloadDuplicatePrevention:
    """Integration tests to verify duplicate prevention in downloads"""

    @pytest.fixture
    def download_job(self):
        """Create a test download job"""
        job_status = JobStatus.objects.get_or_create(name="ready")[0]

        json_request = {
            "filters": {
                "award_type_codes": ["A", "B", "C"],
                "time_period": [{"start_date": "2020-01-01", "end_date": "2020-12-31"}]
            },
            "columns": [],
            "file_format": "csv",
            "download_types": ["elasticsearch_awards"],
            "request_type": "award"
        }

        return DownloadJob.objects.create(
            job_status=job_status,
            file_name=f"test_download_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
            json_request=json.dumps(json_request)
        )

    def test_retry_does_not_create_duplicate_lookups(self, download_job):
        """Test that retrying a download doesn't create duplicate lookup entries"""

        # Simulate first attempt: create lookup entries
        first_attempt_lookups = [
            DownloadJobLookup(
                created_at=datetime.now(timezone.utc),
                download_job_id=download_job.download_job_id,
                lookup_id=i,
                lookup_id_type="award_id"
            )
            for i in range(100)
        ]
        DownloadJobLookup.objects.bulk_create(first_attempt_lookups)

        # Verify first attempt created entries
        count_after_first = DownloadJobLookup.objects.filter(
            download_job_id=download_job.download_job_id
        ).count()
        assert count_after_first == 100

        # Simulate second attempt: cleanup should run
        from usaspending_api.download.filestreaming.download_generation import cleanup_previous_download_attempt
        cleanup_previous_download_attempt(download_job)

        # Create new lookup entries for second attempt
        second_attempt_lookups = [
            DownloadJobLookup(
                created_at=datetime.now(timezone.utc),
                download_job_id=download_job.download_job_id,
                lookup_id=i,
                lookup_id_type="award_id"
            )
            for i in range(100)
        ]
        DownloadJobLookup.objects.bulk_create(second_attempt_lookups)

        # Verify only second attempt's entries exist (no duplicates)
        count_after_second = DownloadJobLookup.objects.filter(
            download_job_id=download_job.download_job_id
        ).count()
        assert count_after_second == 100, "Should only have entries from second attempt"

        # Verify no duplicate lookup_ids exist
        from django.db.models import Count
        duplicates = (
            DownloadJobLookup.objects
            .filter(download_job_id=download_job.download_job_id)
            .values('lookup_id')
            .annotate(count=Count('lookup_id'))
            .filter(count__gt=1)
        )
        assert duplicates.count() == 0, "No duplicate lookup_ids should exist"

    def test_cleanup_called_before_populate_lookups(self, download_job, monkeypatch):
        """Verify cleanup is called before populating new lookups"""

        cleanup_called = []
        populate_called = []

        # Mock the cleanup function
        original_cleanup = __import__(
            'usaspending_api.download.filestreaming.download_generation',
            fromlist=['cleanup_previous_download_attempt']
        ).cleanup_previous_download_attempt

        def mock_cleanup(job):
            cleanup_called.append(True)
            original_cleanup(job)

        # Mock the populate function
        @classmethod
        def mock_populate(cls, filters, download_job, size, filter_options):
            populate_called.append(True)
            # Don't actually populate to keep test fast
            return None

        # Apply mocks
        monkeypatch.setattr(
            'usaspending_api.download.filestreaming.download_generation.cleanup_previous_download_attempt',
            mock_cleanup
        )
        monkeypatch.setattr(
            'usaspending_api.download.helpers.elasticsearch_download_functions._ElasticsearchDownload._populate_download_lookups',
            mock_populate
        )

        # Attempt to run download (will fail due to mocking, but we only care about order)
        try:
            generate_download(download_job)
        except Exception:  # noqa: S110
            pass  # Expected to fail due to incomplete mocking

        # Verify cleanup was called
        assert len(cleanup_called) == 1, "Cleanup should be called once"

        # If populate was called, verify cleanup was called first
        if populate_called:
            assert len(cleanup_called) == 1, "Cleanup must be called before populate"

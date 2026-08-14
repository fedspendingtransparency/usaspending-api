import json
from datetime import datetime, timezone

import pytest

from usaspending_api.download.filestreaming.download_generation import cleanup_previous_download_attempt
from usaspending_api.download.models import DownloadJob, JobStatus
from usaspending_api.download.models.download_job_lookup import DownloadJobLookup


@pytest.mark.django_db
class TestDownloadDuplicatePrevention:
    """Integration tests to verify duplicate prevention in downloads"""

    @pytest.fixture
    def download_job(self):
        """Create a test download job"""
        # Ensure the job_status exists in the database
        job_status, _ = JobStatus.objects.get_or_create(
            name="ready",
            defaults={"description": "Ready for processing"}
        )

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

    def test_cleanup_with_duplicate_lookups(self, download_job):
        """Test cleanup removes duplicate lookup entries (the actual bug scenario)"""

        # Create duplicate lookups - simulating the bug where same lookup_id appears multiple times
        for _ in range(3):  # Create 3 duplicates of each ID
            for i in range(100):
                DownloadJobLookup.objects.create(
                    created_at=datetime.now(timezone.utc),
                    download_job_id=download_job.download_job_id,
                    lookup_id=i,  # Same IDs repeated
                    lookup_id_type="award_id"
                )

        # Should have 300 total entries (100 unique IDs x 3 duplicates)
        count_before = DownloadJobLookup.objects.filter(
            download_job_id=download_job.download_job_id
        ).count()
        assert count_before == 300

        # Cleanup should remove ALL of them
        cleanup_previous_download_attempt(download_job)

        count_after = DownloadJobLookup.objects.filter(
            download_job_id=download_job.download_job_id
        ).count()
        assert count_after == 0, "All duplicate lookups should be removed"

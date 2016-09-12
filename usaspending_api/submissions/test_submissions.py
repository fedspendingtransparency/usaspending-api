from django.test import TestCase, Client
import pytest

from model_mommy import mommy


class SubmissionsTest(TestCase):
    def setUp(self):
        self.award = mommy.make('submissions.SubmissionProcess', _quantity=2)

    @pytest.mark.django_db
    def test_submissions_list(self):
        """
        Ensure the submissions endpoint lists the right number of entities
        """
        resp = self.client.get('/api/v1/submissions/')
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(resp.data), 2)

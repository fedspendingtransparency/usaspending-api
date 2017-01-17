from django.test import TestCase, Client
import pytest
import json

from model_mommy import mommy
from usaspending_api.awards.models import Award


class AwardSummaryTests(TestCase):

    @pytest.mark.django_db
    def test_null_awards(self):
        nonempty = mommy.make('awards.Award', total_obligation="2000", _quantity=2)
        empty = mommy.make('awards.Award', type="U", total_obligation=None, date_signed=None, recipient=None)

        self.assertEqual(Award.objects.count(), 3)
        self.assertEqual(Award.nonempty.count(), 2)

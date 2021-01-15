import json
import pytest
import random

from model_mommy import mommy
from rest_framework import status
from unittest.mock import Mock
from itertools import chain, combinations

@pytest.fixture
def monthly_download_delta_data(db):
    mommy.make("references.Agency")


def test_all_agencies(client, monthly_download_delta_data):
    assert False

def test_specific_agency(client, monthly_download_delta_data):
    assert False

def test_fail_state(client, monthly_download_delta_data):
    assert False
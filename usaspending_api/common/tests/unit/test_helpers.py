# Stdlib imports
from datetime import datetime

# Core Django imports

# Third-party app imports
from django_mock_queries.query import MockModel
from django_mock_queries.query import MockSet
from unittest.mock import patch
from unittest import TestCase

# Imports from your apps
from usaspending_api.common.helpers import check_valid_toptier_agency
from usaspending_api.common.helpers import generate_fiscal_period
from usaspending_api.common.helpers import generate_fiscal_year


# example of a mocked unit test
class TestCheckValidToptierAgency(TestCase):
    agencies = MockSet()
    agency_objects = patch('usaspending_api.references.models.Agency.objects', agencies)

    @agency_objects
    def test_valid_toptier_agency(self):
        self.agencies.add(
            MockModel(mock_name='toptier agency', id=12345, toptier_flag=True)
        )
        self.assertTrue(check_valid_toptier_agency(12345))

    @agency_objects
    def test_invalid_toptier_agency(self):
        self.agencies.add(
            MockModel(mock_name='subtier agency', id=54321, toptier_flag=False)
        )
        self.assertFalse(check_valid_toptier_agency(12345))


class TestGenerateFiscalPeriod(TestCase):

    def test_beginning_of_fiscal_year(self):
        date = datetime.strptime('10/01/2018', '%m/%d/%Y')
        expected = 1
        actual = generate_fiscal_period(date)
        self.assertEqual(actual, expected)

    def test_end_of_fiscal_year(self):
        date = datetime.strptime('09/30/2019', '%m/%d/%Y')
        expected = 12
        actual = generate_fiscal_period(date)
        self.assertEqual(actual, expected)

    def test_middle_of_fiscal_year(self):
        date = datetime.strptime('01/01/2019', '%m/%d/%Y')
        expected = 4
        actual = generate_fiscal_period(date)
        self.assertEqual(actual, expected)

    def test_incorrect_data_type_string(self):
        with self.assertRaises(TypeError):
            generate_fiscal_period('2019')

    def test_incorrect_data_type_int(self):
        with self.assertRaises(TypeError):
            generate_fiscal_period(2019)

    def test_malformed_date_month_year(self):
        date = datetime.strptime('10/2018', '%m/%Y').date
        with self.assertRaises(Exception):
            generate_fiscal_period(date)


# example of a simple unit test
class TestGenerateFiscalYear(TestCase):

    def test_beginning_of_fiscal_year(self):
        date = datetime.strptime('10/01/2018', '%m/%d/%Y')
        expected = 2019
        actual = generate_fiscal_year(date)
        self.assertEqual(actual, expected)

    def test_end_of_fiscal_year(self):
        date = datetime.strptime('09/30/2019', '%m/%d/%Y')
        expected = 2019
        actual = generate_fiscal_year(date)
        self.assertEqual(actual, expected)

    def test_middle_of_fiscal_year(self):
        date = datetime.strptime('01/01/2019', '%m/%d/%Y')
        expected = 2019
        actual = generate_fiscal_year(date)
        self.assertEqual(actual, expected)

    def test_incorrect_data_type_string(self):
        with self.assertRaises(TypeError):
            generate_fiscal_year('2019')

    def test_incorrect_data_type_int(self):
        with self.assertRaises(TypeError):
            generate_fiscal_year(2019)

    def test_malformed_date_month_year(self):
        date = datetime.strptime('10/2018', '%m/%Y').date
        with self.assertRaises(Exception):
            generate_fiscal_year(date)

import pytest


CURRENT_FISCAL_YEAR = 2020


class Helpers:
    @staticmethod
    def get_mocked_current_fiscal_year():
        return CURRENT_FISCAL_YEAR

    @staticmethod
    def mock_current_fiscal_year(monkeypatch, path=None):
        def _mocked_fiscal_year():
            return CURRENT_FISCAL_YEAR

        if path is None:
            path = "usaspending_api.agency.v2.views.agency_base.current_fiscal_year"
        monkeypatch.setattr(path, _mocked_fiscal_year)


@pytest.fixture
def helpers():
    return Helpers


__all__ = ["helpers"]

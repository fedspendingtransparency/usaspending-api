import pytest

from model_mommy import mommy

@pytest.fixure
def bureau_data():
    mommy.make()

import pytest

from telelem_wire import WireFixture


@pytest.fixture
def fx():
    with WireFixture(max_hold=10.0) as f:
        yield f

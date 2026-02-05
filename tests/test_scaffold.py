from factor_engine import get_version


def test_version_string():
    assert isinstance(get_version(), str)

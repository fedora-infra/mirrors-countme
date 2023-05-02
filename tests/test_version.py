from importlib import metadata

from mirrors_countme import version


def test_version_info():
    assert len(version.__version_info__)
    assert all(isinstance(digit, int) for digit in version.__version_info__)
    assert ".".join(str(digit) for digit in version.__version_info__) == metadata.version(
        "mirrors-countme"
    )

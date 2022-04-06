"""This setup.py module accommodates dynamic core metadata via a setuptools build-system backend

See:
    - More background on this approach documented in ./pyproject.toml [build-system] and [project] tables
    - Details of setuptools.setup parameters:
        - https://packaging.python.org/en/latest/guides/distributing-packages-using-setuptools/#setup-args
"""
import pathlib

from setuptools import setup

# Project paths
_PROJECT_NAME = "usaspending-api"
_SRC_ROOT_DIR = pathlib.Path(__file__).parent.resolve() / _PROJECT_NAME.replace("-", "_")
_PROJECT_ROOT_DIR = _SRC_ROOT_DIR.parent.resolve()

# Requirements
# Dependent packages to install will be pulled from these well-known files (if they exist) when installing this package
_INSTALL_REQUIRES = open(_PROJECT_ROOT_DIR / "requirements" / "requirements-app.txt").read().strip().split("\n")
_DEV_REQUIRES = (
    open(_PROJECT_ROOT_DIR / "requirements" / "requirements-dev.txt").read().strip().split("\n")
    if (_PROJECT_ROOT_DIR / "requirements" / "requirements-dev.txt").exists()
    else []
)
_TEST_REQUIRES = (
    open(_PROJECT_ROOT_DIR / "requirements" / "requirements-test.txt").read().strip().split("\n")
    if (_PROJECT_ROOT_DIR / "requirements" / "requirements-test.txt").exists()
    else []
)
_EXTRAS = {k: v for k, v in {"dev": _DEV_REQUIRES, "test": _TEST_REQUIRES}.items() if v}

if __name__ == "__main__":
    # The given parameters in this call to setuptools.setup backfill any project core metadata
    # that could not be declared statically in pyproject.toml, and so were declared as being provided
    # dynamically (by the setuptools build-system backend here) in the "dynamic" field of pyproject.toml
    setup(
        install_requires=_INSTALL_REQUIRES,
        extras_require=_EXTRAS,
    )

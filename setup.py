"""setuptools based setup.py module to accommodate setuptools build system

See:
    Emerging pattern to follow:
        https://github.com/pypa/sampleproject
    Transition towards pyproject.toml centric build/setup config:
        https://www.python.org/dev/peps/pep-0621/
            - Implies that much of the "packaging core metadata" in setup(...) may move to pyproject.toml
        https://snarky.ca/what-the-heck-is-pyproject-toml/
    Following a "src/" Python Package Layout:
        https://github.com/pypa/packaging.python.org/issues/320
        https://blog.ionelmc.ro/2014/05/25/python-packaging/#the-structure
        https://bskinn.github.io/My-How-Why-Pyproject-Src/
    Detailed overview of setuptools packaging:
        https://packaging.python.org/guides/distributing-packages-using-setuptools/
"""
import itertools
import pathlib

from setuptools import setup, find_namespace_packages, find_packages

_PROJECT_NAME = "usaspending-api"
_SRC_ROOT_DIR = pathlib.Path(__file__).resolve() / _PROJECT_NAME.replace("-", "_")
_PROJECT_ROOT_DIR = _SRC_ROOT_DIR.parent.resolve()

# dict of root (top-level) packages mapped to the the directory (relative to project root) they are found under
# These are packages that are not under the same directory hirearchy, but are each parts of the overall core source code

# DOES NOT WORK: Tries to find automation/app as a directory
# _CORE_PACKAGES = {
#     "config": "automation",
#     "lakehouse_etl": "src",
# }
# _found_packages = list(
#     set(itertools.chain(*[find_packages(where=parent_dir) for parent_dir in _CORE_PACKAGES.values()]))
# )

# THIS WORKS... but is verbose and unmaintainable to have to specify each package
# _CORE_PACKAGES = {
#     "config": "automation",
#     "config.app": "automation",
#     "lakehouse_etl": "src",
#     "lakehouse_etl.app_config": "src",
#     "lakehouse_etl.prototypes": "src",
# }
# _found_packages = list(
#     set(itertools.chain(*[find_packages(where=parent_dir) for parent_dir in _CORE_PACKAGES.values()]))
# )

# THIS WORKS... but is verbose and unmaintainable to have to specify each package
# _CORE_PACKAGES = {
#     "config": "automation",
#     "config.app": "automation",
#     "lakehouse_etl": "src",
#     "lakehouse_etl.app_config": "src",
#     "lakehouse_etl.prototypes": "src",
# }
# _found_packages = list(
#     set(itertools.chain(*[find_packages(where=parent_dir) for parent_dir in _CORE_PACKAGES.values()]))
# )

# _CORE_PACKAGES = {
#     "lakehouse_etl": "apps",
#     "lakehouse_etl.app_envs": "automation/config/apps",
#     "lakehouse_etl.app_config": "apps",
#     "lakehouse_etl.prototypes": "apps",
# }
# _found_packages = list(
#     set(itertools.chain(*[find_packages(where=parent_dir) for parent_dir in set(_CORE_PACKAGES.values())]))
# )
# Alternative way to locate namespace packages
#_found_packages = find_namespace_packages()

# Requirements
_install_requires = open(_PROJECT_ROOT_DIR / "requirements" / "requirements.txt").read().strip().split("\n")
_dev_requires = (
    open(_PROJECT_ROOT_DIR / "requirements" / "requirements-dev.txt").read().strip().split("\n")
    if (_PROJECT_ROOT_DIR / "requirements" / "requirements-dev.txt").exists()
    else []
)
_test_requires = (
    open(_PROJECT_ROOT_DIR / "requirements" / "requirements-test.txt").read().strip().split("\n")
    if (_PROJECT_ROOT_DIR / "requirements" / "requirements-test.txt").exists()
    else []
)
_extras = {"dev": _dev_requires + _test_requires}

setup(
    name=_PROJECT_NAME,
    version="0.0.0",
    description=(
        "This API is utilized by USAspending.gov to obtain all federal spending data which is open source "
        "and provided to the public as part of the DATA Act."
    ),
    long_description=(_PROJECT_ROOT_DIR / "README.md").read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
    python_requires="==3.7.*",
    license=(_PROJECT_ROOT_DIR / "LICENSE").read_text(encoding="utf-8"),
    #package_dir=_CORE_PACKAGES,
    packages=find_packages,
    install_requires=_install_requires,
    extras_require=_extras,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3 :: Only",
    ],
)

if __name__ == "__main__":
    print("Running setup.__main__ from setup.py to invoke function setuptools.setup(...)")
    # NOTE: The below setup() will be called when this is invoked from a pyproject.toml build backend.
    #       But until all the packaging core metadata is moved over to the pyproject.toml file, it will remain
    #       commented out
#     setup()

# TODO: Leads
#  1.
#  2. https://github.com/pypa/pip/issues/7265

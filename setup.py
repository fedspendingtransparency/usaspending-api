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

Testing:
    A local editabale install with dependent packages can be tested with a command like:

        python3 -m venv .venv/$(basename "$PWD")
        source .venv/$(basename "$PWD")/bin/activate
        pip install --editable git+file://<LOCAL_PATH>@<BRANCH>#egg=usaspending-api --src .

    - <LOCAL_PATH> replaced with e.g.: /Users/devmcdev/Development/myapps/thisrepo
    - <BRANCH> replaced with whatever branch or tag name in the repo you want to pull code from

"""
import pathlib

from setuptools import setup, find_packages

_PROJECT_NAME = "usaspending-api"
_SRC_ROOT_DIR = pathlib.Path(__file__).parent.resolve() / _PROJECT_NAME.replace("-", "_")
_PROJECT_ROOT_DIR = _SRC_ROOT_DIR.parent.resolve()

# Requirements
# Packages will be installed from these well-known files (if they exist) when installing this package
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
_EXTRAS = {"dev": _DEV_REQUIRES + _TEST_REQUIRES}

if __name__ == "__main__":
    print("????====???? No setup(...) call to run in this setup.py")

    # print("Running setup.__main__ from setup.py to invoke function setuptools.setup(...)")

    # NOTE: The below call to the setuptools.setup function with an empty arguments list is a crutch
    # for an editable install, which still requires the build-system backend (setuptools here) to interrogate
    # as setup.py file to make work, rather than the build-system backend config being 100% statically declared in
    # pyproject.toml
    setup()

    # Invoke the below call to setuptools.setup with ALL the build-system core metadata given by arguments in the call
    # setup(
    #     name=_PROJECT_NAME,
    #     version="0.0.0",
    #     description=(
    #         "This API is utilized by USAspending.gov to obtain all federal spending data which is open source "
    #         "and provided to the public as part of the DATA Act."
    #     ),
    #     long_description=(_PROJECT_ROOT_DIR / "README.md").read_text(encoding="utf-8"),
    #     long_description_content_type="text/markdown",
    #     python_requires="==3.7.*",
    #     license=(_PROJECT_ROOT_DIR / "LICENSE").read_text(encoding="utf-8"),
    #     packages=find_packages(),
    #     install_requires=_INSTALL_REQUIRES,
    #     extras_require=_EXTRAS,
    #     classifiers=[
    #         "Development Status :: 5 - Production/Stable",
    #         "Programming Language :: Python",
    #         "Programming Language :: Python :: 3",
    #         "Programming Language :: Python :: 3.7",
    #         "Programming Language :: Python :: 3 :: Only",
    #     ],
    # )

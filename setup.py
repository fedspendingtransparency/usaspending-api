"""This setup.py module accommodates dynamic core metadata via a setuptools build-system backend
See:
    - More background on this approach documented in ./pyproject.toml [build-system] and [project] tables
    - Details of setuptools.setup parameters:
        - https://packaging.python.org/en/latest/guides/distributing-packages-using-setuptools/#setup-args
"""
import pathlib
import subprocess
from setuptools import find_packages, setup
from setuptools.command.install import install

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


class CustomInstallCommand(install):
    """Custom handler for the install command to include 'bootstrap -a install'."""

    def run(self):
        # Run the default installation process
        print("Running default installation process...")
        super().run()

        # Run the custom 'bootstrap -a install' command
        print("Running 'opentelemetry-bootstrap -a install'...")
        try:
            subprocess.check_call(["opentelemetry-bootstrap", "-a", "install"])
        except subprocess.CalledProcessError as e:
            print(f"Error: Command 'bootstrap -a install' failed with exit code {e.returncode}")
            raise
        except FileNotFoundError:
            print("Error: 'bootstrap' command not found. Ensure it is installed and in your PATH.")
            raise


if __name__ == "__main__":
    setup(
        name=_PROJECT_NAME,
        version="0.0.0",
        description=(
            "This API is utilized by USAspending.gov to obtain all federal spending data which is open source "
            "and provided to the public as part of the DATA Act."
        ),
        long_description=(_PROJECT_ROOT_DIR / "README.md").read_text(encoding="utf-8"),
        long_description_content_type="text/markdown",
        python_requires="==3.8.*",
        license=(_PROJECT_ROOT_DIR / "LICENSE").read_text(encoding="utf-8"),
        packages=find_packages(),
        include_package_data=True,  # see MANIFEST.in for what is included
        install_requires=_INSTALL_REQUIRES,
        extras_require=_EXTRAS,
        classifiers=[
            "Development Status :: 5 - Production/Stable",
            "Programming Language :: Python",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3 :: Only",
        ],
        cmdclass={
            "install": CustomInstallCommand,
        },
    )

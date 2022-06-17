#!/usr/bin/env python3
import os
import sys

from usaspending_api.settings import APP_DIR

if __name__ == "__main__":
    os.environ.setdefault("DDM_CONTAINER_NAME", "app")
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "usaspending_api.settings")
    try:
        from django.core.management import execute_from_command_line
    except ImportError:
        # The above import may fail for some other reason. Ensure that the
        # issue is really that Django is missing to avoid masking other
        # exceptions on Python
        try:
            import django  # noqa
        except ImportError:
            raise ImportError(
                "Couldn't import Django. Are you sure it's installed and "
                "available on your PYTHONPATH environment variable? Did you "
                "forget to activate a virtual environment?"
            )
        raise

    # Setup logs dir
    logs_dir = APP_DIR / "logs"
    console_log_file_path = logs_dir / "console.log"

    logs_dir.mkdir(parents=True, exist_ok=True)
    console_log_file_path.touch(exist_ok=True)

    execute_from_command_line(sys.argv)

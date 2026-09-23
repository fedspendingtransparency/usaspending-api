"""Compatibility shim for the stdlib `imp` module, removed in Python 3.12.

Only implements what dredd-hooks (unmaintained since 2018, archived on GitHub
Nov 2024) needs: `imp.load_source`. Loaded via PYTHONPATH=patches wherever
`dredd` is invoked (see docker-compose.yml and code-style-checks.yaml).
"""

import importlib.util
from types import ModuleType


def load_source(name: str, path: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

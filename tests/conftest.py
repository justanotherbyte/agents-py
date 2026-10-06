"""Test setup: stand-ins for the modules that only exist on Workers.

``agents._ffi`` imports ``js`` / ``pyodide``, and the Workers SDK (``workers``)
imports ``js`` too; neither exists outside the Workers runtime. CPython tests
register pure-Python stand-ins before any SDK module imports them
(``.design/code_semantics.md`` §2).
"""

import sys

import _fake_ffi
import _fake_workers

sys.modules["workers"] = _fake_workers
sys.modules["agents._ffi"] = _fake_ffi
